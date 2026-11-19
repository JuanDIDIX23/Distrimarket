"""Utilidades de seguridad: emisión/validación de JWT y autenticación servicio-a-servicio."""
import hmac
from datetime import datetime, timedelta, timezone

import jwt
from fastapi import Depends, Header, HTTPException, status

from . import config


def create_token(user_id: int, email: str, role: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "email": email,
        "role": role,
        "iat": now,
        "exp": now + timedelta(minutes=config.JWT_TTL_MINUTES),
        "iss": "distrimarket-auth",
    }
    return jwt.encode(payload, config.JWT_SECRET, algorithm=config.JWT_ALGORITHM)


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(
            token,
            config.JWT_SECRET,
            algorithms=[config.JWT_ALGORITHM],
            issuer="distrimarket-auth",
            options={"require": ["exp", "sub", "role"]},
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Token expirado")
    except jwt.InvalidTokenError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Token inválido")


def current_user(authorization: str | None = Header(default=None)) -> dict:
    """Dependencia FastAPI: exige 'Authorization: Bearer <jwt>'."""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Falta token Bearer")
    return decode_token(authorization.split(" ", 1)[1])


def require_role(role: str):
    def checker(user: dict = Depends(current_user)) -> dict:
        if user.get("role") != role:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Permisos insuficientes")
        return user
    return checker


def require_internal(x_internal_token: str | None = Header(default=None)) -> None:
    """Protege endpoints internos: solo otros servicios conocen el token compartido."""
    if not x_internal_token or not hmac.compare_digest(x_internal_token, config.INTERNAL_TOKEN):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Endpoint interno")
