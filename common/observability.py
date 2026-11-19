"""Logging estructurado en JSON y propagación de ID de correlación (X-Request-ID)."""
import contextvars
import json
import logging
import sys
import time
import uuid

from fastapi import FastAPI, Request

from . import config

request_id_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="-")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        data = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)),
            "level": record.levelname,
            "service": config.SERVICE_NAME,
            "instance": config.INSTANCE_ID,
            "request_id": request_id_ctx.get(),
            "msg": record.getMessage(),
        }
        if hasattr(record, "extra_data"):
            data.update(record.extra_data)
        return json.dumps(data, ensure_ascii=False)


def get_logger(name: str = "app") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def instrument(app: FastAPI) -> None:
    """Middleware: asigna/propaga X-Request-ID, mide latencia y la expone en cabeceras."""
    log = get_logger("http")

    @app.middleware("http")
    async def _mw(request: Request, call_next):
        rid = request.headers.get("x-request-id") or uuid.uuid4().hex[:16]
        token = request_id_ctx.set(rid)
        start = time.perf_counter()
        try:
            response = await call_next(request)
        finally:
            request_id_ctx.reset(token)
        elapsed = (time.perf_counter() - start) * 1000
        response.headers["X-Request-ID"] = rid
        response.headers["X-Served-By"] = config.INSTANCE_ID
        response.headers["X-Response-Time-ms"] = f"{elapsed:.1f}"
        if request.url.path not in ("/health", "/ready"):
            request_id_ctx.set(rid)
            log.info("request", extra={"extra_data": {
                "method": request.method, "path": request.url.path,
                "status": response.status_code, "ms": round(elapsed, 1)}})
        return response
