"""HTTP API: uvicorn app.api.main:app

В процессе API работает outbox relay (фоновая задача): он публикует события из таблицы
outbox в RabbitMQ. API не зависит от доступности брокера — при его недоступности события
накапливаются в БД и уходят позже.
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI
from faststream.rabbit import RabbitBroker

from app.api.payments import router as payments_router
from app.config import get_settings
from app.db import get_engine, get_session_factory
from app.logging_setup import setup_logging
from app.outbox.publisher import RabbitEventPublisher
from app.outbox.relay import OutboxRelay

settings = get_settings()
setup_logging(settings.log_level)
logger = logging.getLogger("payments.api")


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    publisher = RabbitEventPublisher(RabbitBroker(settings.rabbitmq_url))
    relay = OutboxRelay(
        get_session_factory(),
        publisher,
        batch_size=settings.outbox_batch_size,
        poll_interval=settings.outbox_poll_interval,
        publish_timeout=settings.outbox_publish_timeout,
    )
    relay_task = asyncio.create_task(relay.run(), name="outbox-relay")
    try:
        yield
    finally:
        relay_task.cancel()
        with suppress(asyncio.CancelledError):
            await relay_task
        await publisher.close()
        await get_engine().dispose()


app = FastAPI(
    title="Payment Processing Service",
    version="1.0.0",
    description=(
        "Асинхронная обработка платежей: приём запроса → outbox → RabbitMQ → "
        "эмуляция шлюза → webhook."
    ),
    lifespan=lifespan,
)
app.include_router(payments_router, prefix="/api/v1")


@app.get("/health", tags=["service"], summary="Проверка живости (без X-API-Key)")
async def health() -> dict[str, str]:
    return {"status": "ok"}
