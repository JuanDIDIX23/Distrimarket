"""orders-service: creación y consulta de pedidos.

Flujo de creación (Saga orquestada con compensación):
  1. Idempotencia: si el cliente reenvía la misma Idempotency-Key se devuelve el pedido original.
  2. Reserva síncrona de stock en inventory-service (REST) con balanceo entre réplicas,
     timeouts, reintentos con backoff y circuit breaker por réplica.
  3. En UNA transacción local se guarda el pedido + un evento en la tabla outbox
     (Transactional Outbox): nunca se pierde un evento aunque Redis esté caído.
  4. Si el paso 3 falla, se ejecuta la compensación: liberar la reserva.
  5. Un proceso en segundo plano (relay) publica los eventos del outbox en Redis Streams.
"""
import asyncio
import hashlib
import itertools
import json
import os
import uuid
from contextlib import asynccontextmanager

import asyncpg
import httpx
import redis.asyncio as aioredis
from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field

from common import config
from common.db import create_pool
from common.observability import get_logger, instrument, request_id_ctx
from common.resilience import CircuitBreaker, CircuitOpenError
from common.security import current_user, require_internal

DSN = os.getenv("DATABASE_URL", "postgresql://dm:dm@localhost:5432/orders_db")
INVENTORY_URLS = [u.strip() for u in os.getenv(
    "INVENTORY_URLS", "http://localhost:8002,http://localhost:8012").split(",") if u.strip()]
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "2.0"))
log = get_logger("orders")

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    order_ref VARCHAR(64) UNIQUE NOT NULL,
    user_id INTEGER NOT NULL,
    user_email VARCHAR(254) NOT NULL,
    idempotency_key VARCHAR(64),
    status VARCHAR(12) NOT NULL CHECK (status IN ('CONFIRMED','CANCELLED')),
    total NUMERIC(14,2) NOT NULL,
    items JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id);
CREATE TABLE IF NOT EXISTS outbox (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    event_type VARCHAR(40) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox(id) WHERE published_at IS NULL;
"""


class InventoryUnavailable(Exception):
    pass


class InventoryClient:
    """Cliente con balanceo round-robin, failover entre réplicas y circuit breaker por réplica."""

    def __init__(self, urls: list[str]):
        self.urls = urls
        self.breakers = {u: CircuitBreaker(name=u, failure_threshold=3, reset_timeout=5.0) for u in urls}
        self._rr = itertools.cycle(range(len(urls)))
        self.http = httpx.AsyncClient(timeout=httpx.Timeout(HTTP_TIMEOUT, connect=0.5))

    async def call(self, method: str, path: str, **kw) -> httpx.Response:
        start = next(self._rr)
        order = [self.urls[(start + i) % len(self.urls)] for i in range(len(self.urls))]
        headers = {"X-Internal-Token": config.INTERNAL_TOKEN, "X-Request-ID": request_id_ctx.get()}
        errors = []
        # Dos pasadas por las réplicas con backoff: tolera fallos transitorios.
        for pass_no in range(2):
            for url in order:
                cb = self.breakers[url]
                if not cb.allow():
                    errors.append(f"{url}: circuito abierto")
                    continue
                try:
                    resp = await self.http.request(method, url + path, headers=headers, **kw)
                except (httpx.TransportError, httpx.TimeoutException) as exc:
                    cb.on_failure()
                    errors.append(f"{url}: {type(exc).__name__}")
                    continue
                if resp.status_code >= 500:
                    cb.on_failure()
                    errors.append(f"{url}: HTTP {resp.status_code}")
                    continue
                cb.on_success()
                return resp
            if all(e.endswith("circuito abierto") for e in errors):
                break  # Fail-fast: todos los circuitos abiertos, no tiene sentido esperar.
            if pass_no == 0:
                await asyncio.sleep(0.15)
        log.error("inventory no disponible", extra={"extra_data": {"errors": errors}})
        if all(e.endswith("circuito abierto") for e in errors):
            raise CircuitOpenError("; ".join(errors))
        raise InventoryUnavailable("; ".join(errors))


inventory = InventoryClient(INVENTORY_URLS)


async def outbox_relay(app: FastAPI) -> None:
    """Publica eventos pendientes del outbox en Redis Streams (entrega al-menos-una-vez)."""
    r = aioredis.from_url(config.REDIS_URL, socket_timeout=2, socket_connect_timeout=2)
    app.state.relay_stats = {"published": 0, "errors": 0, "last_error": None}
    while True:
        try:
            async with app.state.pool.acquire() as conn:
                async with conn.transaction():
                    rows = await conn.fetch(
                        """SELECT id, event_id, event_type, payload FROM outbox
                           WHERE published_at IS NULL ORDER BY id LIMIT 100
                           FOR UPDATE SKIP LOCKED""")
                    for row in rows:
                        await r.xadd(config.EVENT_STREAM, {
                            "event_id": str(row["event_id"]), "type": row["event_type"],
                            "payload": row["payload"]}, maxlen=100_000, approximate=True)
                        await conn.execute("UPDATE outbox SET published_at=now() WHERE id=$1", row["id"])
                        app.state.relay_stats["published"] += 1
            await asyncio.sleep(0.05 if rows else 0.5)
        except asyncio.CancelledError:
            break
        except Exception as exc:  # Redis/BD caídos: se reintenta sin perder eventos.
            app.state.relay_stats["errors"] += 1
            app.state.relay_stats["last_error"] = f"{type(exc).__name__}: {exc}"[:200]
            log.warning(f"relay outbox: {exc}")
            await asyncio.sleep(2)
    await r.aclose()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await create_pool(DSN, SCHEMA)
    task = asyncio.create_task(outbox_relay(app))
    yield
    task.cancel()
    await inventory.http.aclose()
    await app.state.pool.close()


app = FastAPI(title="DistriMarket orders-service", lifespan=lifespan)
instrument(app)


class OrderItemIn(BaseModel):
    product_id: int = Field(gt=0)
    quantity: int = Field(gt=0, le=1000)


class OrderIn(BaseModel):
    items: list[OrderItemIn] = Field(min_length=1, max_length=50)


def order_out(r) -> dict:
    return {"id": r["id"], "order_ref": r["order_ref"], "status": r["status"],
            "total": float(r["total"]), "items": json.loads(r["items"]),
            "created_at": r["created_at"].isoformat()}


async def add_event(conn, event_type: str, payload: dict) -> None:
    await conn.execute(
        "INSERT INTO outbox (event_id, event_type, payload) VALUES ($1,$2,$3)",
        uuid.uuid4(), event_type, json.dumps(payload))


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    async with app.state.pool.acquire() as conn:
        await conn.fetchval("SELECT 1")
    return {"status": "ready"}


@app.post("/orders", status_code=201)
async def create_order(body: OrderIn, user: dict = Depends(current_user),
                       idempotency_key: str | None = Header(default=None, max_length=64)):
    uid = int(user["sub"])
    if idempotency_key:
        async with app.state.pool.acquire() as conn:
            prev = await conn.fetchrow(
                "SELECT * FROM orders WHERE user_id=$1 AND idempotency_key=$2", uid, idempotency_key)
        if prev:
            return {**order_out(prev), "idempotent_replay": True}

    # Mismo order_ref para la misma Idempotency-Key => la reserva también es idempotente.
    order_ref = (hashlib.sha256(f"{uid}:{idempotency_key}".encode()).hexdigest()[:32]
                 if idempotency_key else uuid.uuid4().hex)
    merged: dict[int, int] = {}
    for it in body.items:
        merged[it.product_id] = merged.get(it.product_id, 0) + it.quantity
    payload = {"order_ref": order_ref,
               "items": [{"product_id": k, "quantity": v} for k, v in merged.items()]}

    try:
        resp = await inventory.call("POST", "/internal/reservations", json=payload)
    except CircuitOpenError:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE,
                            "Inventario temporalmente no disponible (circuito abierto). Intente más tarde.")
    except InventoryUnavailable:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE,
                            "Inventario no disponible. Intente más tarde.")
    if resp.status_code != 200:
        raise HTTPException(resp.status_code, resp.json().get("detail", "Error de inventario"))

    reservation = resp.json()
    items = reservation["items"]
    total = round(sum(i["unit_price"] * i["quantity"] for i in items), 2)
    try:
        async with app.state.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """INSERT INTO orders (order_ref,user_id,user_email,idempotency_key,status,total,items)
                       VALUES ($1,$2,$3,$4,'CONFIRMED',$5,$6) RETURNING *""",
                    order_ref, uid, user["email"], idempotency_key, total, json.dumps(items))
                await add_event(conn, "order.confirmed", {
                    "order_id": row["id"], "order_ref": order_ref, "user_id": uid,
                    "user_email": user["email"], "total": total, "items": items})
    except asyncpg.UniqueViolationError:
        # Carrera entre peticiones concurrentes con la MISMA Idempotency-Key: otra petición ya
        # registró el pedido usando la misma reserva (order_ref idéntico). NO se compensa,
        # porque liberar la reserva dejaría al pedido ganador sin stock descontado.
        async with app.state.pool.acquire() as conn:
            prev = await conn.fetchrow("SELECT * FROM orders WHERE order_ref=$1", order_ref)
        return {**order_out(prev), "idempotent_replay": True}
    except Exception as exc:
        # Compensación de la Saga: devolver el stock reservado.
        log.error(f"fallo al persistir pedido, compensando: {exc}")
        try:
            await inventory.call("POST", f"/internal/reservations/{order_ref}/release")
        except Exception as comp_exc:  # queda registrado para conciliación manual
            log.error(f"compensación fallida para {order_ref}: {comp_exc}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "No se pudo registrar el pedido")
    log.info("pedido confirmado", extra={"extra_data": {"order_id": row["id"], "total": total}})
    return {**order_out(row), "idempotent_replay": False}


@app.get("/orders")
async def my_orders(user: dict = Depends(current_user)):
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM orders WHERE user_id=$1 ORDER BY id DESC LIMIT 100", int(user["sub"]))
    return [order_out(r) for r in rows]


@app.get("/orders/{order_id}")
async def get_order(order_id: int, user: dict = Depends(current_user)):
    async with app.state.pool.acquire() as conn:
        r = await conn.fetchrow("SELECT * FROM orders WHERE id=$1", order_id)
    # Autorización a nivel de objeto: un cliente solo ve sus pedidos (evita IDOR).
    if not r or (r["user_id"] != int(user["sub"]) and user["role"] != "admin"):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Pedido no encontrado")
    return order_out(r)


@app.post("/orders/{order_id}/cancel")
async def cancel_order(order_id: int, user: dict = Depends(current_user)):
    async with app.state.pool.acquire() as conn:
        r = await conn.fetchrow("SELECT * FROM orders WHERE id=$1", order_id)
    if not r or (r["user_id"] != int(user["sub"]) and user["role"] != "admin"):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Pedido no encontrado")
    if r["status"] == "CANCELLED":
        return order_out(r)
    try:
        resp = await inventory.call("POST", f"/internal/reservations/{r['order_ref']}/release")
    except (CircuitOpenError, InventoryUnavailable):
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Inventario no disponible")
    if resp.status_code != 200:
        raise HTTPException(resp.status_code, "No se pudo liberar la reserva")
    async with app.state.pool.acquire() as conn:
        async with conn.transaction():
            r = await conn.fetchrow(
                "UPDATE orders SET status='CANCELLED' WHERE id=$1 RETURNING *", order_id)
            await add_event(conn, "order.cancelled", {
                "order_id": order_id, "order_ref": r["order_ref"], "user_id": r["user_id"],
                "user_email": r["user_email"], "total": float(r["total"])})
    return order_out(r)


@app.get("/internal/status", dependencies=[Depends(require_internal)])
async def internal_status():
    async with app.state.pool.acquire() as conn:
        pending = await conn.fetchval("SELECT count(*) FROM outbox WHERE published_at IS NULL")
    return {"inventory_breakers": [cb.snapshot() for cb in inventory.breakers.values()],
            "outbox_pending": pending, "relay": getattr(app.state, "relay_stats", {})}
