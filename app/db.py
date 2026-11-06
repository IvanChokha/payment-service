"""Подключение к PostgreSQL (SQLAlchemy 2.0, async). Engine создаётся лениво."""

from functools import lru_cache

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import get_settings


@lru_cache
def get_engine() -> AsyncEngine:
    return create_async_engine(get_settings().database_url, pool_pre_ping=True)


@lru_cache
def get_session_factory() -> async_sessionmaker[AsyncSession]:
    # expire_on_commit=False: объекты остаются читаемыми после commit без ленивой подгрузки,
    # которая в async-режиме невозможна.
    return async_sessionmaker(get_engine(), expire_on_commit=False)
