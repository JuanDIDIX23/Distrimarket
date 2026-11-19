"""Prueba de rendimiento (carga en lazo cerrado): N usuarios virtuales concurrentes envían
peticiones sin pausa durante D segundos. Mide throughput (req/s), latencias p50/p95/p99 y errores.

Uso: python scripts/load_test.py [--base http://localhost:8000] [--out docs/evidencias/rendimiento.json]
"""
import argparse
import asyncio
import json
import pathlib
import statistics
import time
import uuid

import httpx


def pct(values, p):
    if not values:
        return None
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(p / 100 * len(s) + 0.5)) - 1))
    return round(s[k], 1)


async def run_scenario(base, name, concurrency, duration, make_request):
    latencies, codes = [], {}
    stop_at = time.perf_counter() + duration
    async with httpx.AsyncClient(base_url=base, timeout=15,
                                 limits=httpx.Limits(max_connections=concurrency + 10)) as c:
        async def vu(i):
            while time.perf_counter() < stop_at:
                t = time.perf_counter()
                try:
                    r = await make_request(c, i)
                    code = r.status_code
                except Exception as exc:
                    code = type(exc).__name__
                latencies.append((time.perf_counter() - t) * 1000)
                codes[code] = codes.get(code, 0) + 1
        t0 = time.perf_counter()
        await asyncio.gather(*(vu(i) for i in range(concurrency)))
        elapsed = time.perf_counter() - t0
    total = sum(codes.values())
    ok = sum(v for k, v in codes.items() if isinstance(k, int) and k < 400)
    res = {"escenario": name, "concurrencia": concurrency, "duracion_s": round(elapsed, 2),
           "peticiones": total, "throughput_rps": round(total / elapsed, 1),
           "exito_pct": round(100 * ok / total, 2) if total else 0,
           "lat_media_ms": round(statistics.mean(latencies), 1),
           "p50_ms": pct(latencies, 50), "p95_ms": pct(latencies, 95), "p99_ms": pct(latencies, 99),
           "max_ms": round(max(latencies), 1), "codigos": {str(k): v for k, v in codes.items()}}
    print(json.dumps(res, ensure_ascii=False))
    return res


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8000")
    ap.add_argument("--duration", type=float, default=15)
    ap.add_argument("--out", default="docs/evidencias/rendimiento.json")
    ap.add_argument("--label", default="2 réplicas de inventory")
    ap.add_argument("--only", default="")
    a = ap.parse_args()

    async with httpx.AsyncClient(base_url=a.base, timeout=10) as c:
        admin = (await c.post("/api/auth/login", json={"email": "admin@distrimarket.co",
                                                        "password": "Admin#2026seguro"})).json()["access_token"]
        ah = {"Authorization": f"Bearer {admin}"}
        prod = (await c.post("/api/products", headers=ah, json={
            "sku": "LOAD-" + uuid.uuid4().hex[:6].upper(), "name": "Producto carga",
            "price": 1000, "stock": 1_000_000})).json()
        tokens = []
        for i in range(20):
            email = f"carga-{uuid.uuid4().hex[:8]}@correo.co"
            await c.post("/api/auth/register", json={"email": email, "full_name": "Usuario carga",
                                                       "password": "Carga2026x"})
            r = await c.post("/api/auth/login", json={"email": email, "password": "Carga2026x"})
            tokens.append(r.json()["access_token"])

    async def get_products(c, i):
        return await c.get("/api/products")

    async def post_order(c, i):
        return await c.post("/api/orders", headers={"Authorization": f"Bearer {tokens[i % 20]}"},
                            json={"items": [{"product_id": prod["id"], "quantity": 1}]})

    results = []
    plan = [("GET /api/products (catálogo)", get_products, [10, 50, 100]),
            ("POST /api/orders (crear pedido)", post_order, [10, 25, 50])]
    for name, fn, levels in plan:
        if a.only and a.only not in name:
            continue
        for conc in levels:
            results.append(await run_scenario(a.base, name, conc, a.duration, fn))
            await asyncio.sleep(2)
    out = pathlib.Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"configuracion": a.label, "fecha": time.strftime("%Y-%m-%d %H:%M"),
                               "resultados": results}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
