"""Обработка события payments.new: платёж → БД → webhook, с повторами."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Protocol
from uuid import UUID

from app.exceptions import PaymentNotFound

if TYPE_CHECKING:
    from app.models import Payment

logger = logging.getLogger("payments.handler")


class Processor(Protocol):
    async def process(self, payment_id: UUID) -> "Payment": ...


class Notifier(Protocol):
    async def notify(self, payment: "Payment") -> None: ...


class PaymentEventHandler:
    """Один «проход» = провести платёж (идемпотентно) + отправить webhook.

    При любой ошибке проход повторяется с экспоненциальной паузой base_delay * 2**(n-1)
    (по умолчанию 1 с, 2 с), всего max_attempts попыток. Повторный проход дёшев: платёж уже в
    финальном статусе, шлюз не вызывается — повторяется только то, что упало (обычно webhook).

    Исключение после последней попытки пробрасывается наверх: consumer отклонит сообщение,
    и RabbitMQ переложит его в Dead Letter Queue. PaymentNotFound не повторяется — ждать нечего.
    """

    def __init__(
        self,
        processor: Processor,
        notifier: Notifier,
        *,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_concurrency: int = 20,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._processor = processor
        self._notifier = notifier
        self._max_attempts = max_attempts
        self._base_delay = base_delay
        self._sleep = sleep
        # Ограничение параллелизма: всплеск сообщений не должен исчерпать пул соединений
        # с БД и пул HTTP-клиента.
        self._semaphore = asyncio.Semaphore(max_concurrency)

    async def handle(self, payment_id: UUID) -> None:
        async with self._semaphore:
            await self._run_with_retries(payment_id)

    async def _run_with_retries(self, payment_id: UUID) -> None:
        for attempt in range(1, self._max_attempts + 1):
            try:
                payment = await self._processor.process(payment_id)
                await self._notifier.notify(payment)
                return
            except PaymentNotFound:
                raise
            except Exception as exc:
                if attempt == self._max_attempts:
                    raise
                delay = self._base_delay * 2 ** (attempt - 1)
                logger.warning(
                    "payment %s: attempt %d/%d failed: %r; retrying in %.1fs",
                    payment_id,
                    attempt,
                    self._max_attempts,
                    exc,
                    delay,
                )
                await self._sleep(delay)
