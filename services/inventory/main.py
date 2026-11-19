"""inventory-service: catálogo de productos y control de stock.

Se despliega con VARIAS RÉPLICAS sin estado (el estado vive en inventory_db).
La reserva de stock es atómica e idempotente:
  * Atómica: UPDATE ... WHERE stock >= qty  -> imposible sobrevender bajo concurrencia.
  * Idempotente: tabla reservations con clave única (order_ref) -> un reintento de
    la misma orden no descuenta stock dos veces.
"""
import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from common import config
from common.db import create_pool
from common.observability import get_logger, instrument
from common.security import require_internal, require_role

DSN = os.getenv("DATABASE_URL", "postgresql://dm:dm@localhost:5432/inventory_db")
log = get_logger("inventory")

SCHEMA = """
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    sku VARCHAR(40) UNIQUE NOT NULL,
    name VARCHAR(120) NOT NULL,
    price NUMERIC(12,2) NOT NULL CHECK (price > 0),
    stock INTEGER NOT NULL CHECK (stock >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS reservations (
    order_ref VARCHAR(64) PRIMARY KEY,
    status VARCHAR(12) NOT NULL CHECK (status IN ('RESERVED','RELEASED')),
    items JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

SEED = [
    ("CAF-500", "Café de origen Huila 500 g", 28000, 120),
    ("PAN-ART", "Pan artesanal de masa madre", 9500, 80),
    ("AREQ-250", "Arequipe tradicional 250 g", 7800, 60),
    ("QUE-CAM", "Queso campesino 500 g", 14500, 50),
    ("CHO-70", "Chocolate de mesa 70% cacao", 11200, 90),
    ("PANELA", "Panela orgánica 1 kg", 6500, 150),
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await create_pool(DSN, SCHEMA)
    async with app.state.pool.acquire() as conn:
        if await conn.fetchval("SELECT count(*) FROM products") == 0:
            await conn.executemany(
                "INSERT INTO products (sku,name,price,stock) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING",
                SEED)
    yield
    await app.state.pool.close()


app = FastAPI(title="DistriMarket inventory-service", lifespan=lifespan)
instrument(app)


class ProductIn(BaseModel):
    sku: str = Field(pattern=r"^[A-Z0-9\-]{3,40}$")
    name: str = Field(min_length=3, max_length=120)
    price: float = Field(gt=0, le=100_000_000)
    stock: int = Field(ge=0, le=1_000_000)


class StockPatch(BaseModel):
    stock: int = Field(ge=0, le=1_000_000)


class ReserveItem(BaseModel):
    product_id: int = Field(gt=0)
    quantity: int = Field(gt=0, le=1000)


class ReserveIn(BaseModel):
    order_ref: str = Field(min_length=8, max_length=64)
    items: list[ReserveItem] = Field(min_length=1, max_length=50)


def row_to_product(r) -> dict:
    return {"id": r["id"], "sku": r["sku"], "name": r["name"],
            "price": float(r["price"]), "stock": r["stock"]}


@app.get("/health")
async def health():
    return {"status": "ok", "instance": config.INSTANCE_ID}


@app.get("/ready")
async def ready():
    async with app.state.pool.acquire() as conn:
        await conn.fetchval("SELECT 1")
    return {"status": "ready", "instance": config.INSTANCE_ID}


@app.get("/products")
async def list_products():
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM products ORDER BY id")
    return [row_to_product(r) for r in rows]


@app.get("/products/{product_id}")
async def get_product(product_id: int):
    async with app.state.pool.acquire() as conn:
        r = await conn.fetchrow("SELECT * FROM products WHERE id=$1", product_id)
    if not r:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Producto no encontrado")
    return row_to_product(r)


@app.post("/products", status_code=201, dependencies=[Depends(require_role("admin"))])
async def create_product(body: ProductIn):
    async with app.state.pool.acquire() as conn:
        r = await conn.fetchrow(
            """INSERT INTO products (sku,name,price,stock) VALUES ($1,$2,$3,$4)
               ON CONFLICT (sku) DO NOTHING RETURNING *""",
            body.sku, body.name, body.price, body.stock)
    if not r:
        raise HTTPException(status.HTTP_409_CONFLICT, "SKU duplicado")
    return row_to_product(r)


@app.patch("/products/{product_id}/stock", dependencies=[Depends(require_role("admin"))])
async def set_stock(product_id: int, body: StockPatch):
    async with app.state.pool.acquire() as conn:
        r = await conn.fetchrow(
            "UPDATE products SET stock=$1, updated_at=now() WHERE id=$2 RETURNING *",
            body.stock, product_id)
    if not r:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Producto no encontrado")
    return row_to_product(r)


# ---------------------------- Endpoints internos ----------------------------
@app.post("/internal/reservations", dependencies=[Depends(require_internal)])
async def reserve(body: ReserveIn):
    """Reserva todo-o-nada dentro de una transacción ACID."""
    import json
    async with app.state.pool.acquire() as conn:
        async with conn.transaction():
            existing = await conn.fetchrow(
                "SELECT status, items FROM reservations WHERE order_ref=$1 FOR UPDATE", body.order_ref)
            if existing:  # Reintento de la misma orden: respuesta idempotente.
                return {"order_ref": body.order_ref, "status": existing["status"],
                        "items": json.loads(existing["items"]), "idempotent_replay": True}
            # Orden determinista de bloqueo (por product_id) para evitar interbloqueos.
            priced = []
            for it in sorted(body.items, key=lambda i: i.product_id):
                r = await conn.fetchrow(
                    """UPDATE products SET stock = stock - $1, updated_at=now()
                       WHERE id=$2 AND stock >= $1 RETURNING id, name, price, stock""",
                    it.quantity, it.product_id)
                if r is None:
                    exists = await conn.fetchval("SELECT 1 FROM products WHERE id=$1", it.product_id)
                    # Al lanzar la excepción, la transacción hace ROLLBACK de lo ya descontado.
                    raise HTTPException(
                        status.HTTP_409_CONFLICT if exists else status.HTTP_404_NOT_FOUND,
                        f"Stock insuficiente para producto {it.product_id}" if exists
                        else f"Producto {it.product_id} no existe")
                priced.append({"product_id": r["id"], "name": r["name"],
                               "unit_price": float(r["price"]), "quantity": it.quantity})
            await conn.execute(
                "INSERT INTO reservations (order_ref, status, items) VALUES ($1,'RESERVED',$2)",
                body.order_ref, json.dumps(priced))
    log.info("stock reservado", extra={"extra_data": {"order_ref": body.order_ref}})
    return {"order_ref": body.order_ref, "status": "RESERVED", "items": priced,
            "idempotent_replay": False}


@app.post("/internal/reservations/{order_ref}/release", dependencies=[Depends(require_internal)])
async def release(order_ref: str):
    """Acción compensatoria (patrón Saga): devuelve el stock si la orden no pudo completarse."""
    import json
    async with app.state.pool.acquire() as conn:
        async with conn.transaction():
            r = await conn.fetchrow(
                "SELECT status, items FROM reservations WHERE order_ref=$1 FOR UPDATE", order_ref)
            if not r:
                raise HTTPException(status.HTTP_404_NOT_FOUND, "Reserva no encontrada")
            if r["status"] == "RELEASED":
                return {"order_ref": order_ref, "status": "RELEASED", "idempotent_replay": True}
            for it in json.loads(r["items"]):
                await conn.execute("UPDATE products SET stock = stock + $1 WHERE id=$2",
                                   it["quantity"], it["product_id"])
            await conn.execute("UPDATE reservations SET status='RELEASED' WHERE order_ref=$1", order_ref)
    log.info("reserva liberada", extra={"extra_data": {"order_ref": order_ref}})
    return {"order_ref": order_ref, "status": "RELEASED", "idempotent_replay": False}
