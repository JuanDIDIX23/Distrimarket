"""Configuración compartida leída desde variables de entorno (12-factor)."""
import os


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Variable de entorno requerida no definida: {name}")
    return value


JWT_SECRET = env("JWT_SECRET", "cambie-esta-clave-en-produccion-0123456789")
JWT_ALGORITHM = "HS256"
JWT_TTL_MINUTES = int(env("JWT_TTL_MINUTES", "60"))
INTERNAL_TOKEN = env("INTERNAL_TOKEN", "token-interno-servicio-a-servicio")
REDIS_URL = env("REDIS_URL", "redis://localhost:6379/0")
EVENT_STREAM = env("EVENT_STREAM", "distrimarket.events")
SERVICE_NAME = env("SERVICE_NAME", "unknown")
INSTANCE_ID = env("INSTANCE_ID", SERVICE_NAME)
