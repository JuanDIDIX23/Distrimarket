"""Pruebas de integración: comunicación real entre componentes distribuidos."""
import os
import time

import httpx
import redis

from conftest import BASE_URL, make_product

INVENTORY_DIRECT = os.getenv("INVENTORY_DIRECT", "http://localhost:8002")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


def test_gateway_balancea_entre_replicas_de_inventario(client):
    served = [client.get("/api/products").headers["x-upstream"] for _ in range(20)]
    assert set(served) == {"inventory-1", "inventory-2"}
    assert abs(served.count("inventory-1") - served.count("inventory-2")) <= 2


def test_gateway_propaga_request_id(client):
    r = client.get("/api/products", headers={"X-Request-ID": "prueba-trazabilidad-01"})
    assert r.headers["x-request-id"] == "prueba-trazabilidad-01"


def test_flujo_completo_pedido_evento_notificacion(client, admin, user):
    """orders -> inventory (REST) -> outbox -> Redis Streams -> notifications (evento)."""
    p = make_product(client, admin, stock=5, price=12345)
    oid = client.post("/api/orders", headers=user["headers"],
                      json={"items": [{"product_id": p["id"], "quantity": 1}]}).json()["id"]
    deadline = time.time() + 10
    while time.time() < deadline:
        subjects = [n["subject"] for n in client.get("/api/notifications", headers=user["headers"]).json()]
        if f"Pedido #{oid} confirmado" in subjects:
            break
        time.sleep(0.2)
    else:
        raise AssertionError("La notificación no llegó en 10 s")


def test_evento_publicado_en_stream(client, admin, user):
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    before = r.xlen("distrimarket.events")
    p = make_product(client, admin, stock=2)
    client.post("/api/orders", headers=user["headers"], json={"items": [{"product_id": p["id"], "quantity": 1}]})
    time.sleep(1.5)
    assert r.xlen("distrimarket.events") == before + 1
    last = r.xrevrange("distrimarket.events", count=1)[0][1]
    assert last["type"] == "order.confirmed" and "event_id" in last


def test_endpoint_interno_rechaza_llamadas_sin_token_de_servicio():
    r = httpx.post(f"{INVENTORY_DIRECT}/internal/reservations",
                   json={"order_ref": "ataque-directo-1", "items": [{"product_id": 1, "quantity": 1}]})
    assert r.status_code == 403


def test_endpoints_internos_no_expuestos_por_gateway(client, user):
    r = client.post("/api/products/../internal/reservations", headers=user["headers"], json={})
    assert r.status_code in (401, 403, 404, 405)
    assert client.get("/api/internal/status").status_code == 404


def test_panel_de_estado_agrega_salud(client):
    data = client.get("/api-status").json()
    assert data["redis"] is True
    assert all(i["up"] for lst in data["services"].values() for i in lst)
    assert len(data["services"]["inventory"]) == 2


def test_rate_limiting_login_por_ip():
    """Protección contra fuerza bruta: >10 intentos/min desde una IP => 429.
    Se usa una IP de loopback distinta para no afectar a las demás pruebas."""
    import random
    transport = httpx.HTTPTransport(local_address=f"127.{random.randint(1, 250)}.{random.randint(1, 250)}.7")
    with httpx.Client(base_url=BASE_URL, transport=transport, timeout=5) as c:
        codes = [c.post("/api/auth/login", json={"email": "x@correo.co", "password": "Mala12345"}).status_code
                 for _ in range(14)]
    assert codes[:10] == [401] * 10 and 429 in codes[10:]
