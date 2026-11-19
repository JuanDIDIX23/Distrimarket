"""api-gateway: punto único de entrada del sistema.

Responsabilidades:
  * Enrutamiento hacia los microservicios (/api/auth, /api/products, /api/orders, /api/notifications).
  * Balanceo de carga round-robin entre réplicas (inventory x2) con failover.
  * Circuit breaker por instancia, timeouts y reintentos con backoff (solo operaciones idempotentes).
  * Validación temprana de JWT (defensa en profundidad: los servicios también validan).
  * Rate limiting distribuido en Redis (ventana fija) — más estricto en /auth/login (fuerza bruta).
  * Cabeceras de seguridad, límite de tamaño de cuerpo y CORS restringido.
  * Sirve el cliente web estático y un panel de estado (/api/status).
"""
import asyncio
import itertools
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response

from common import config
from common.observability import get_logger, instrument, request_id_ctx
from common.resilience import CircuitBreaker
from common.security import decode_token


def urls(name: str, default: str) -> list[str]:
    return [u.strip() for u in os.getenv(name, default).split(",") if u.strip()]


UPSTREAMS = {
    "auth": urls("AUTH_URLS", "http://localhost:8001"),
    "inventory": urls("INVENTORY_URLS", "http://localhost:8002,http://localhost:8012"),
    "orders": urls("ORDERS_URLS", "http://localhost:8003"),
    "notifications": urls("NOTIFICATIONS_URLS", "http://localhost:8004"),
}
ROUTES = [  # (prefijo público, servicio, requiere JWT)
    ("/api/auth/me", "auth", True),
    ("/api/auth", "auth", False),
    ("/api/products", "inventory", None),  # GET público, escritura con JWT
    ("/api/orders", "orders", True),
    ("/api/notifications", "notifications", True),
]
RATE_LIMIT = int(os.getenv("RATE_LIMIT_PER_MIN", "600"))
LOGIN_RATE_LIMIT = int(os.getenv("LOGIN_RATE_LIMIT_PER_MIN", "10"))
MAX_BODY = 64 * 1024
TIMEOUT = float(os.getenv("UPSTREAM_TIMEOUT", "3.0"))
FRONTEND = Path(os.getenv("FRONTEND_DIR", Path(__file__).resolve().parents[2] / "frontend"))
ALLOWED_ORIGINS = urls("CORS_ORIGINS", "http://localhost:8000")
log = get_logger("gateway")

breakers = {u: CircuitBreaker(name=u, failure_threshold=3, reset_timeout=5.0)
            for lst in UPSTREAMS.values() for u in lst}
rr = {svc: itertools.cycle(range(len(lst))) for svc, lst in UPSTREAMS.items()}


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT, connect=0.5),
                                       limits=httpx.Limits(max_connections=500))
    app.state.redis = aioredis.from_url(config.REDIS_URL, socket_timeout=0.5,
                                        socket_connect_timeout=0.5)
    yield
    await app.state.http.aclose()
    await app.state.redis.aclose()


app = FastAPI(title="DistriMarket api-gateway", lifespan=lifespan, docs_url=None, redoc_url=None)
instrument(app)
app.add_middleware(CORSMiddleware, allow_origins=ALLOWED_ORIGINS,
                   allow_methods=["GET", "POST", "PATCH"],
                   allow_headers=["Authorization", "Content-Type", "Idempotency-Key"])


@app.middleware("http")
async def security_headers(request: Request, call_next):
    if int(request.headers.get("content-length") or 0) > MAX_BODY:
        return JSONResponse({"detail": "Cuerpo demasiado grande"}, status_code=413)
    resp = await call_next(request)
    resp.headers.update({
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "Referrer-Policy": "no-referrer",
        "Content-Security-Policy": "default-src 'self'; style-src 'self' 'unsafe-inline'; "
                                   "script-src 'self'; img-src 'self' data:",
    })
    return resp


def _rl_key(kind: str, request: Request) -> str:
    client_ip = request.client.host if request.client else "unknown"
    return f"rl:{kind}:{client_ip}:{int(time.time() // 60)}"


async def rate_limited(request: Request) -> bool:
    """Ventana fija de 60 s compartida por todas las instancias del gateway (vía Redis)."""
    try:
        pipe = app.state.redis.pipeline()
        pipe.incr(_rl_key("all", request))
        pipe.expire(_rl_key("all", request), 61)
        count, _ = await pipe.execute()
        return count > RATE_LIMIT
    except Exception as exc:
        # Fail-open: si Redis cae se prioriza la disponibilidad y se registra el evento.
        log.warning(f"rate limiter degradado (Redis no disponible): {exc}")
        return False


async def login_blocked(request: Request) -> bool:
    """Bloquea la IP si acumuló LOGIN_RATE_LIMIT intentos FALLIDOS en el minuto actual."""
    try:
        count = await app.state.redis.get(_rl_key("loginfail", request))
        return int(count or 0) >= LOGIN_RATE_LIMIT
    except Exception:
        return False


async def register_login_failure(request: Request) -> None:
    try:
        pipe = app.state.redis.pipeline()
        pipe.incr(_rl_key("loginfail", request))
        pipe.expire(_rl_key("loginfail", request), 61)
        await pipe.execute()
    except Exception:
        pass


async def forward(service: str, request: Request, path: str) -> Response:
    instances = UPSTREAMS[service]
    start = next(rr[service])
    order = [instances[(start + i) % len(instances)] for i in range(len(instances))]
    body = await request.body()
    idempotent = request.method in ("GET", "HEAD") or "idempotency-key" in request.headers
    headers = {k: v for k, v in request.headers.items()
               if k.lower() in ("authorization", "content-type", "idempotency-key")}
    headers["X-Request-ID"] = request_id_ctx.get()
    headers["X-Forwarded-For"] = request.client.host if request.client else ""
    attempts = 3 if idempotent else 1
    last_error = "sin instancias disponibles"
    open_count = 0
    for attempt in range(attempts):
        for url in order:
            cb = breakers[url]
            if not cb.allow():
                open_count += 1
                continue
            try:
                resp = await app.state.http.request(
                    request.method, url + path, params=request.query_params,
                    content=body, headers=headers)
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                cb.on_failure()
                last_error = f"{type(exc).__name__} en {url}"
                if not idempotent:
                    # Una POST sin Idempotency-Key solo se reintenta si la conexión ni se estableció.
                    if isinstance(exc, httpx.ConnectError):
                        continue
                    break
                continue
            if resp.status_code >= 500 and resp.status_code != 503:
                cb.on_failure()
                last_error = f"HTTP {resp.status_code} en {url}"
                if idempotent:
                    continue
            else:
                cb.on_success()
            out = Response(content=resp.content, status_code=resp.status_code,
                           media_type=resp.headers.get("content-type"))
            out.headers["X-Upstream"] = resp.headers.get("x-served-by", url)
            return out
        if attempt < attempts - 1:
            await asyncio.sleep(0.1 * 2 ** attempt)
    if open_count and open_count >= len(order) * attempts:
        last_error = "circuito abierto"
    log.error(f"{service} no disponible: {last_error}")
    return JSONResponse({"detail": f"Servicio '{service}' no disponible temporalmente",
                         "reason": last_error}, status_code=503,
                        headers={"Retry-After": "5"})


@app.api_route("/api/{rest:path}", methods=["GET", "POST", "PATCH"])
async def gateway(rest: str, request: Request):
    full = "/api/" + rest
    match = next(((p, s, auth) for p, s, auth in ROUTES if full == p or full.startswith(p + "/")), None)
    if not match:
        raise HTTPException(404, "Ruta no encontrada")
    prefix, service, needs_auth = match
    is_login = full == "/api/auth/login"
    if await rate_limited(request) or (is_login and await login_blocked(request)):
        return JSONResponse({"detail": "Demasiadas solicitudes"}, status_code=429,
                            headers={"Retry-After": "60"})
    if needs_auth or (needs_auth is None and request.method != "GET"):
        authz = request.headers.get("authorization", "")
        if not authz.lower().startswith("bearer "):
            raise HTTPException(401, "Falta token Bearer")
        decode_token(authz.split(" ", 1)[1])  # lanza 401 si es inválido/expirado
    upstream_path = full[len("/api"):]  # /api/orders/1 -> /orders/1
    resp = await forward(service, request, upstream_path)
    if is_login and resp.status_code == 401:
        await register_login_failure(request)
    return resp


@app.get("/api-status")
async def status_panel():
    """Estado agregado: salud de cada instancia y estado de cada circuit breaker."""
    async def probe(url: str):
        t = time.perf_counter()
        try:
            r = await app.state.http.get(url + "/ready", timeout=1.0)
            return {"url": url, "up": r.status_code == 200,
                    "ms": round((time.perf_counter() - t) * 1000, 1)}
        except Exception as exc:
            return {"url": url, "up": False, "error": type(exc).__name__}
    results = {svc: await asyncio.gather(*(probe(u) for u in lst)) for svc, lst in UPSTREAMS.items()}
    try:
        redis_ok = bool(await app.state.redis.ping())
    except Exception:
        redis_ok = False
    return {"services": results, "redis": redis_ok,
            "circuit_breakers": [cb.snapshot() for cb in breakers.values()]}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/")
async def index():
    return FileResponse(FRONTEND / "index.html")


@app.get("/{asset}")
async def static_asset(asset: str):
    if asset not in ("app.js", "styles.css"):
        raise HTTPException(404)
    return FileResponse(FRONTEND / asset)
