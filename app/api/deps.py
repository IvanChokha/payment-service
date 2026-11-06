"""Зависимости FastAPI: сессия БД, проверка X-API-Key, заголовок Idempotency-Key."""

import secrets
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Security, status
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import get_session_factory

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False, description="Статический API-ключ")


async def verify_api_key(api_key: Annotated[str | None, Security(api_key_header)]) -> None:
    expected = get_settings().api_key
    # compare_digest — сравнение за постоянное время; bytes, чтобы не падать на не-ASCII.
    if api_key is None or not secrets.compare_digest(api_key.encode(), expected.encode()):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing API key")


async def get_session() -> AsyncIterator[AsyncSession]:
    async with get_session_factory()() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]

IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
        description="Уникальный ключ запроса: повтор с тем же ключом не создаёт дубль платежа",
    ),
]
