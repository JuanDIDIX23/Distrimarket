"""Pool de conexiones asyncpg con reintentos al arrancar (la BD puede tardar en estar lista)."""
import asyncio

import asyncpg

from .observability import get_logger

log = get_logger("db")


async def create_pool(dsn: str, schema_sql: str, attempts: int = 30) -> asyncpg.Pool:
    for attempt in range(1, attempts + 1):
        try:
            pool = await asyncpg.create_pool(dsn, min_size=2, max_size=20, command_timeout=5)
            async with pool.acquire() as conn:
                # Bloqueo consultivo: evita carreras al crear el esquema con varias réplicas.
                async with conn.transaction():
                    await conn.execute("SELECT pg_advisory_xact_lock(424242)")
                    await conn.execute(schema_sql)
            log.info("Conectado a la base de datos")
            return pool
        except (OSError, asyncpg.PostgresError) as exc:
            log.warning(f"BD no disponible (intento {attempt}/{attempts}): {exc}")
            await asyncio.sleep(min(2.0, 0.2 * attempt))
    raise RuntimeError("No fue posible conectar a la base de datos")
