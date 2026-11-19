"""Pruebas de concurrencia: múltiples solicitudes simultáneas sobre el mismo recurso."""
import asyncio
import json
import pathlib
import time
import uuid

import httpx

from conftest import BASE_URL, make_product, new_user

EVID = pathlib.Path(__file__).resolve().parents[1] / "docs" / "evidencias"


async def _post_orders(tokens, product_id, n, key_fn=None):
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=20,
                                 limits=httpx.Limits(max_connections=200)) as c:
        async def one(i):
            h = {"Authorization": f"Bearer {tokens[i % len(tokens)]}"}
            if key_fn:
                h["Idempotency-Key"] = key_fn(i)
            r = await c.post("/api/orders", headers=h,
                             json={"items": [{"product_id": product_id, "quantity": 1}]})
            return r.status_code, r.json(), r.headers.get("x-upstream")
        return await asyncio.gather(*(one(i) for i in range(n)))


def test_sin_sobreventa_con_100_compras_simultaneas(client, admin):
    """100 clientes compran a la vez un producto con 25 unidades: exactamente 25 éxitos."""
    stock = 25
    p = make_product(client, admin, stock=stock)
    users = [new_user(client, "conc") for _ in range(10)]
    t0 = time.perf_counter()
    results = asyncio.run(_post_orders([u["token"] for u in users], p["id"], 100))
    elapsed = time.perf_counter() - t0
    codes = [c for c, _, _ in results]
    ok, conflict = codes.count(201), codes.count(409)
    final_stock = client.get(f"/api/products/{p['id']}").json()["stock"]
    evidence = {"prueba": "100 pedidos simultáneos, stock inicial 25", "exitosos_201": ok,
                "rechazados_409": conflict, "otros": len(codes) - ok - conflict,
                "stock_final": final_stock, "duracion_s": round(elapsed, 3),
                "sobreventa": ok > stock or final_stock < 0}
    EVID.mkdir(parents=True, exist_ok=True)
    (EVID / "concurrencia_sobreventa.json").write_text(json.dumps(evidence, indent=2, ensure_ascii=False))
    assert ok == stock and conflict == 100 - stock and final_stock == 0


def test_misma_idempotency_key_concurrente_crea_un_solo_pedido(client, admin):
    """Un doble/triple clic simultáneo (20 peticiones con la misma clave) => 1 pedido, stock -1."""
    p = make_product(client, admin, stock=10)
    u = new_user(client, "dobleclic")
    key = uuid.uuid4().hex
    results = asyncio.run(_post_orders([u["token"]], p["id"], 20, key_fn=lambda i: key))
    ids = {body.get("id") for code, body, _ in results if code in (200, 201)}
    final_stock = client.get(f"/api/products/{p['id']}").json()["stock"]
    evidence = {"prueba": "20 peticiones simultáneas con la misma Idempotency-Key",
                "codigos": sorted(set(c for c, _, _ in results)), "pedidos_distintos": len(ids),
                "stock_inicial": 10, "stock_final": final_stock}
    (EVID / "concurrencia_idempotencia.json").write_text(json.dumps(evidence, indent=2, ensure_ascii=False))
    assert len(ids) == 1 and final_stock == 9
