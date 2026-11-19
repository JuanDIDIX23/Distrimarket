"""auth-service: registro, inicio de sesión y emisión de JWT.

Las contraseñas se almacenan con bcrypt (sal aleatoria, factor de costo 12).
Base de datos propia: auth_db (patrón database-per-service).
"""
import asyncio
import os
from contextlib import asynccontextmanager

import asyncpg
import bcrypt
from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, EmailStr, Field, field_validator

from common.db import create_pool
from common.observability import get_logger, instrument
from common.security import create_token, current_user

DSN = os.getenv("DATABASE_URL", "postgresql://dm:dm@localhost:5432/auth_db")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "admin@distrimarket.co")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "Admin#2026seguro")
BCRYPT_ROUNDS = int(os.getenv("BCRYPT_ROUNDS", "12"))
log = get_logger("auth")

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(254) UNIQUE NOT NULL,
    full_name VARCHAR(120) NOT NULL,
    password_hash VARCHAR(100) NOT NULL,
    role VARCHAR(20) NOT NULL DEFAULT 'cliente' CHECK (role IN ('cliente','admin')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def hash_password(raw: str) -> str:
    return bcrypt.hashpw(raw.encode(), bcrypt.gensalt(rounds=BCRYPT_ROUNDS)).decode()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await create_pool(DSN, SCHEMA)
    async with app.state.pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO users (email, full_name, password_hash, role)
               VALUES ($1, 'Administrador', $2, 'admin') ON CONFLICT (email) DO NOTHING""",
            ADMIN_EMAIL, hash_password(ADMIN_PASSWORD))
    yield
    await app.state.pool.close()


app = FastAPI(title="DistriMarket auth-service", lifespan=lifespan)
instrument(app)


class RegisterIn(BaseModel):
    email: EmailStr
    full_name: str = Field(min_length=3, max_length=120)
    password: str = Field(min_length=8, max_length=72)

    @field_validator("password")
    @classmethod
    def strong(cls, v: str) -> str:
        if not (any(c.isdigit() for c in v) and any(c.isalpha() for c in v)):
            raise ValueError("La contraseña debe contener letras y números")
        return v

    @field_validator("full_name")
    @classmethod
    def clean_name(cls, v: str) -> str:
        if any(c in v for c in "<>{}$;"):
            raise ValueError("El nombre contiene caracteres no permitidos")
        return v.strip()


class LoginIn(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1, max_length=72)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    async with app.state.pool.acquire() as conn:
        await conn.fetchval("SELECT 1")
    return {"status": "ready"}


@app.post("/auth/register", status_code=status.HTTP_201_CREATED)
async def register(body: RegisterIn):
    # bcrypt es intensivo en CPU: se ejecuta en un hilo para no bloquear el event loop.
    pw_hash = await asyncio.to_thread(hash_password, body.password)
    try:
        async with app.state.pool.acquire() as conn:
            row = await conn.fetchrow(
                """INSERT INTO users (email, full_name, password_hash) VALUES ($1,$2,$3)
                   RETURNING id, email, full_name, role""",
                body.email.lower(), body.full_name, pw_hash)
    except asyncpg.UniqueViolationError:
        raise HTTPException(status.HTTP_409_CONFLICT, "El correo ya está registrado")
    log.info("usuario registrado", extra={"extra_data": {"user_id": row["id"]}})
    return dict(row)


@app.post("/auth/login")
async def login(body: LoginIn):
    async with app.state.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE email=$1", body.email.lower())
    # Mismo mensaje para usuario inexistente o clave errada (evita enumeración de usuarios).
    ok = row is not None and await asyncio.to_thread(
        bcrypt.checkpw, body.password.encode(), row["password_hash"].encode())
    if not ok:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Credenciales inválidas")
    token = create_token(row["id"], row["email"], row["role"])
    return {"access_token": token, "token_type": "bearer",
            "user": {"id": row["id"], "email": row["email"],
                     "full_name": row["full_name"], "role": row["role"]}}


@app.get("/auth/me")
async def me(user: dict = Depends(current_user)):
    return user
