"""Pruebas funcionales: cada operación produce el resultado esperado (RF-01 … RF-10)."""
import time
import uuid

import jwt

from conftest import make_product


# ----------------------------- Autenticación (RF-01, RF-02) -----------------------------
def test_registro_exitoso(client):
    email = f"nuevo-{uuid.uuid4().hex[:6]}@correo.co"
    r = client.post("/api/auth/register", json={"email": email, "full_name": "Ana Pérez", "password": "Segura2026"})
    assert r.status_code == 201
    body = r.json()
    assert body["email"] == email and body["role"] == "cliente"
    assert "password" not in str(body) and "hash" not in str(body)


def test_registro_correo_duplicado(client, user):
    r = client.post("/api/auth/register", json={"email": user["email"], "full_name": "Dup", "password": "Segura2026"})
    assert r.status_code == 409


def test_registro_contrasena_debil_rechazada(client):
    r = client.post("/api/auth/register", json={"email": "debil@correo.co", "full_name": "Débil", "password": "12345678"})
    assert r.status_code == 422


def test_registro_entrada_maliciosa_rechazada(client):
    r = client.post("/api/auth/register", json={"email": "xss@correo.co", "full_name": "<script>alert(1)</script>",
                                                 "password": "Segura2026"})
    assert r.status_code == 422


def test_login_credenciales_invalidas(client, user):
    r = client.post("/api/auth/login", json={"email": user["email"], "password": "Incorrecta99"})
    assert r.status_code == 401
    r2 = client.post("/api/auth/login", json={"email": "noexiste@correo.co", "password": "Incorrecta99"})
    assert r2.status_code == 401 and r.json() == r2.json()  # sin enumeración de usuarios


def test_login_devuelve_jwt_valido(client, user):
    claims = jwt.decode(user["token"], options={"verify_signature": False})
    assert claims["sub"] == str(user["id"]) and claims["role"] == "cliente" and claims["exp"] > time.time()


# ----------------------------- Catálogo (RF-03, RF-04) -----------------------------
def test_listar_productos_publico(client):
    r = client.get("/api/products")
    assert r.status_code == 200
    assert len(r.json()) >= 6 and {"id", "sku", "name", "price", "stock"} <= r.json()[0].keys()


def test_producto_inexistente(client):
    assert client.get("/api/products/999999").status_code == 404


def test_admin_crea_producto(client, admin):
    p = make_product(client, admin, stock=7, price=4500)
    assert p["stock"] == 7 and p["price"] == 4500


def test_cliente_no_puede_crear_producto(client, user):
    r = client.post("/api/products", headers=user["headers"],
                    json={"sku": "NOPE-1", "name": "No permitido", "price": 1, "stock": 1})
    assert r.status_code == 403


def test_crear_producto_sin_token(client):
    r = client.post("/api/products", json={"sku": "NOPE-2", "name": "Sin token", "price": 1, "stock": 1})
    assert r.status_code == 401


# ----------------------------- Pedidos (RF-05 … RF-08) -----------------------------
def test_crear_pedido_descuenta_stock_y_calcula_total(client, admin, user):
    p = make_product(client, admin, stock=10, price=2500)
    r = client.post("/api/orders", headers=user["headers"],
                    json={"items": [{"product_id": p["id"], "quantity": 3}]})
    assert r.status_code == 201, r.text
    order = r.json()
    assert order["status"] == "CONFIRMED" and order["total"] == 7500
    assert client.get(f"/api/products/{p['id']}").json()["stock"] == 7


def test_pedido_items_repetidos_se_agrupan(client, admin, user):
    p = make_product(client, admin, stock=10, price=1000)
    r = client.post("/api/orders", headers=user["headers"], json={"items": [
        {"product_id": p["id"], "quantity": 2}, {"product_id": p["id"], "quantity": 3}]})
    assert r.status_code == 201 and r.json()["total"] == 5000
    assert client.get(f"/api/products/{p['id']}").json()["stock"] == 5


def test_pedido_stock_insuficiente_es_atomico(client, admin, user):
    a = make_product(client, admin, stock=5)
    b = make_product(client, admin, stock=1)
    r = client.post("/api/orders", headers=user["headers"], json={"items": [
        {"product_id": a["id"], "quantity": 2}, {"product_id": b["id"], "quantity": 5}]})
    assert r.status_code == 409
    # Todo-o-nada: el producto A no debe haber perdido stock.
    assert client.get(f"/api/products/{a['id']}").json()["stock"] == 5


def test_pedido_producto_inexistente(client, user):
    r = client.post("/api/orders", headers=user["headers"], json={"items": [{"product_id": 999999, "quantity": 1}]})
    assert r.status_code == 404


def test_pedido_cantidad_invalida(client, user):
    r = client.post("/api/orders", headers=user["headers"], json={"items": [{"product_id": 1, "quantity": -3}]})
    assert r.status_code == 422


def test_listar_mis_pedidos(client, admin, user):
    p = make_product(client, admin, stock=3)
    oid = client.post("/api/orders", headers=user["headers"],
                      json={"items": [{"product_id": p["id"], "quantity": 1}]}).json()["id"]
    ids = [o["id"] for o in client.get("/api/orders", headers=user["headers"]).json()]
    assert oid in ids


def test_no_puede_ver_pedido_ajeno(client, admin, user, other_user):
    p = make_product(client, admin, stock=3)
    oid = client.post("/api/orders", headers=user["headers"],
                      json={"items": [{"product_id": p["id"], "quantity": 1}]}).json()["id"]
    assert client.get(f"/api/orders/{oid}", headers=other_user["headers"]).status_code == 404
    assert client.get(f"/api/orders/{oid}", headers=user["headers"]).status_code == 200


def test_cancelar_pedido_libera_stock(client, admin, user):
    p = make_product(client, admin, stock=4)
    oid = client.post("/api/orders", headers=user["headers"],
                      json={"items": [{"product_id": p["id"], "quantity": 4}]}).json()["id"]
    assert client.get(f"/api/products/{p['id']}").json()["stock"] == 0
    r = client.post(f"/api/orders/{oid}/cancel", headers=user["headers"])
    assert r.status_code == 200 and r.json()["status"] == "CANCELLED"
    assert client.get(f"/api/products/{p['id']}").json()["stock"] == 4
    # Cancelar dos veces no devuelve stock dos veces (idempotente).
    client.post(f"/api/orders/{oid}/cancel", headers=user["headers"])
    assert client.get(f"/api/products/{p['id']}").json()["stock"] == 4


def test_idempotency_key_evita_pedido_duplicado(client, admin, user):
    p = make_product(client, admin, stock=10)
    key = uuid.uuid4().hex
    h = {**user["headers"], "Idempotency-Key": key}
    body = {"items": [{"product_id": p["id"], "quantity": 2}]}
    r1, r2 = client.post("/api/orders", headers=h, json=body), client.post("/api/orders", headers=h, json=body)
    assert r1.json()["id"] == r2.json()["id"] and r2.json()["idempotent_replay"] is True
    assert client.get(f"/api/products/{p['id']}").json()["stock"] == 8


# ----------------------------- Seguridad de tokens -----------------------------
def test_pedidos_sin_token(client):
    assert client.get("/api/orders").status_code == 401


def test_token_alterado_rechazado(client, user):
    forged = jwt.encode({"sub": str(user["id"]), "role": "admin", "exp": time.time() + 600,
                         "iss": "distrimarket-auth"}, "clave-falsa", algorithm="HS256")
    assert client.get("/api/orders", headers={"Authorization": f"Bearer {forged}"}).status_code == 401


def test_token_alg_none_rechazado(client, user):
    forged = jwt.encode({"sub": str(user["id"]), "role": "admin", "exp": time.time() + 600,
                         "iss": "distrimarket-auth"}, None, algorithm="none")
    assert client.get("/api/orders", headers={"Authorization": f"Bearer {forged}"}).status_code == 401


def test_token_expirado_rechazado(client, user):
    import os
    secret = os.getenv("JWT_SECRET", "dev-secret-distrimarket-2026-cambiar")
    expired = jwt.encode({"sub": str(user["id"]), "role": "cliente", "exp": time.time() - 10,
                          "iss": "distrimarket-auth"}, secret, algorithm="HS256")
    r = client.get("/api/orders", headers={"Authorization": f"Bearer {expired}"})
    assert r.status_code == 401 and "expirado" in r.json()["detail"]


def test_cabeceras_de_seguridad(client):
    h = client.get("/api/products").headers
    assert h["x-content-type-options"] == "nosniff" and h["x-frame-options"] == "DENY"
    assert "default-src 'self'" in h["content-security-policy"]


def test_cuerpo_demasiado_grande(client, user):
    r = client.post("/api/orders", headers={**user["headers"], "Content-Type": "application/json"},
                    content=b'{"items":[' + b'{"product_id":1,"quantity":1},' * 3000 + b'{"product_id":1,"quantity":1}]}')
    assert r.status_code == 413
