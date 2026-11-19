"""Fixtures compartidas. Las pruebas se ejecutan contra el sistema REAL desplegado
(API Gateway en BASE_URL), no contra mocks."""
import os
import uuid

import httpx
import pytest

BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "admin@distrimarket.co")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "Admin#2026seguro")


@pytest.fixture(scope="session")
def client():
    with httpx.Client(base_url=BASE_URL, timeout=10) as c:
        yield c


def new_user(client, prefix="user") -> dict:
    email = f"{prefix}-{uuid.uuid4().hex[:8]}@correo.co"
    pw = "Prueba2026x"
    r = client.post("/api/auth/register", json={"email": email, "full_name": "Usuario Prueba", "password": pw})
    assert r.status_code == 201, r.text
    r = client.post("/api/auth/login", json={"email": email, "password": pw})
    assert r.status_code == 200, r.text
    data = r.json()
    return {"email": email, "id": data["user"]["id"], "token": data["access_token"],
            "headers": {"Authorization": f"Bearer {data['access_token']}"}}


@pytest.fixture(scope="session")
def user(client):
    return new_user(client, "cliente")


@pytest.fixture(scope="session")
def other_user(client):
    return new_user(client, "otro")


@pytest.fixture(scope="session")
def admin(client):
    r = client.post("/api/auth/login", json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def make_product(client, admin, stock=50, price=10000) -> dict:
    sku = "T-" + uuid.uuid4().hex[:8].upper()
    r = client.post("/api/products", headers=admin,
                    json={"sku": sku, "name": f"Producto prueba {sku}", "price": price, "stock": stock})
    assert r.status_code == 201, r.text
    return r.json()
