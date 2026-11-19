"""notifications-service: consumidor asíncrono de eventos (Redis Streams, consumer groups).

Garantías:
  * Al-menos-una-vez: el mensaje solo se confirma (XACK) después de persistirlo.
  * Idempotencia: event_id es UNIQUE, los duplicados se ignoran.
  * Recuperación: XAUTOCLAIM reasigna mensajes que un consumidor caído dejó pendientes.
  * Dead Letter Queue: tras MAX_DELIVERIES intentos fallidos el evento pasa a <stream>.dlq.
  * Desacople temporal: si este servicio cae, los eventos se acumulan en el stream y se
    procesan al volver, sin afectar la creación de pedidos.
"""
import asyncio
import json
import os
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import Depends, FastAPI

from common import config
from common.db import create_pool
from common.observability import get_logger, instrument
from common.security import current_user, require_internal

DSN = os.getenv("DATABASE_URL", "postgresql://dm:dm@localhost:5432/notifications_db")
GROUP = "notifications"
CONSUMER = config.INSTANCE_ID
MAX_DELIVERIES = 5
log = get_logger("notifications")

SCHEMA = """
CREATE TABLE IF NOT EXISTS notifications (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID UNIQUE NOT NULL,
    user_id INTEGER NOT NULL,
    channel VARCHAR(10) NOT NULL DEFAULT 'email',
    recipient VARCHAR(254) NOT NULL,
    subject VARCHAR(200) NOT NULL,
    body TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_notif_user ON notifications(user_id);
"""

TEMPLATES = {
    "order.confirmed": ("Pedido #{order_id} confirmado",
                        "Hola, tu pedido #{order_id} por ${total_cop} COP fue confirmado."),
    "order.cancelled": ("Pedido #{order_id} cancelado",
                        "Tu pedido #{order_id} fue cancelado y el stock fue liberado."),
}


async def handle(pool, fields: dict) -> bool:
    """Procesa un evento. Devuelve True si se insertó, False si era duplicado."""
    etype = fields["type"]
    payload = json.loads(fields["payload"])
    payload["total_cop"] = f"{payload.get('total', 0):,.0f}".replace(",", ".")
    subject_t, body_t = TEMPLATES.get(etype, ("Evento {order_id}", "Evento recibido"))
    async with pool.acquire() as conn:
        status = await conn.execute(
            """INSERT INTO notifications (event_id,user_id,recipient,subject,body)
               VALUES ($1,$2,$3,$4,$5) ON CONFLICT (event_id) DO NOTHING""",
            fields["event_id"], payload["user_id"], payload["user_email"],
            subject_t.format(**payload), body_t.format(**payload))
    inserted = status.endswith("1")
    if inserted:  # Simulación del envío (SMTP/SMS fuera del alcance del proyecto)
        log.info("notificación enviada", extra={"extra_data": {
            "event_id": fields["event_id"], "type": etype, "to": payload["user_email"]}})
    return inserted


async def consumer_loop(app: FastAPI) -> None:
    stats = app.state.stats = {"processed": 0, "duplicates": 0, "failed": 0,
                               "dead_lettered": 0, "reclaimed": 0, "redis_errors": 0}
    r = aioredis.from_url(config.REDIS_URL, decode_responses=True,
                          socket_timeout=5, socket_connect_timeout=2)
    group_ready = False
    claim_cursor = "0-0"
    while True:
        try:
            if not group_ready:
                try:
                    await r.xgroup_create(config.EVENT_STREAM, GROUP, id="0", mkstream=True)
                except aioredis.ResponseError as exc:
                    if "BUSYGROUP" not in str(exc):
                        raise
                group_ready = True
            # 1) Reclamar mensajes abandonados por consumidores caídos (> 15 s sin ACK).
            claim_cursor, claimed, _ = await r.xautoclaim(
                config.EVENT_STREAM, GROUP, CONSUMER, min_idle_time=15_000,
                start_id=claim_cursor, count=50)
            stats["reclaimed"] += len(claimed)
            batches = [(config.EVENT_STREAM, claimed)] if claimed else []
            # 2) Leer mensajes nuevos (bloqueo de hasta 2 s).
            batches += await r.xreadgroup(GROUP, CONSUMER, {config.EVENT_STREAM: ">"},
                                          count=50, block=2000) or []
            for _stream, messages in batches:
                for msg_id, fields in messages:
                    if not fields:
                        await r.xack(config.EVENT_STREAM, GROUP, msg_id)
                        continue
                    try:
                        if await handle(app.state.pool, fields):
                            stats["processed"] += 1
                        else:
                            stats["duplicates"] += 1
                        await r.xack(config.EVENT_STREAM, GROUP, msg_id)
                    except Exception as exc:
                        stats["failed"] += 1
                        info = await r.xpending_range(config.EVENT_STREAM, GROUP, msg_id, msg_id, 1)
                        deliveries = info[0]["times_delivered"] if info else 1
                        log.error(f"error procesando {msg_id} (entrega {deliveries}): {exc}")
                        if deliveries >= MAX_DELIVERIES:
                            await r.xadd(config.EVENT_STREAM + ".dlq", {**fields, "error": str(exc)[:200]})
                            await r.xack(config.EVENT_STREAM, GROUP, msg_id)
                            stats["dead_lettered"] += 1
        except asyncio.CancelledError:
            break
        except (aioredis.ConnectionError, aioredis.TimeoutError, OSError) as exc:
            stats["redis_errors"] += 1
            group_ready = False
            log.warning(f"Redis no disponible, reintentando: {exc}")
            await asyncio.sleep(2)
        except Exception as exc:
            log.error(f"error inesperado en consumidor: {exc}")
            await asyncio.sleep(1)
    await r.aclose()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await create_pool(DSN, SCHEMA)
    task = asyncio.create_task(consumer_loop(app))
    yield
    task.cancel()
    await app.state.pool.close()


app = FastAPI(title="DistriMarket notifications-service", lifespan=lifespan)
instrument(app)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    async with app.state.pool.acquire() as conn:
        await conn.fetchval("SELECT 1")
    return {"status": "ready"}


@app.get("/notifications")
async def my_notifications(user: dict = Depends(current_user)):
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM notifications WHERE user_id=$1 ORDER BY id DESC LIMIT 50", int(user["sub"]))
    return [{"id": r["id"], "event_id": str(r["event_id"]), "channel": r["channel"],
             "subject": r["subject"], "body": r["body"],
             "created_at": r["created_at"].isoformat()} for r in rows]


@app.get("/internal/stats", dependencies=[Depends(require_internal)])
async def stats():
    return getattr(app.state, "stats", {})
