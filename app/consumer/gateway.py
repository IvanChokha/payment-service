"""Эмуляция внешнего платёжного шлюза."""

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from app.enums import PaymentStatus

if TYPE_CHECKING:
    from app.models import Payment


class PaymentGateway:
    """Имитирует обращение к шлюзу: задержка 2–5 с, 90 % успешных платежей.

    В реальной интеграции payment.id нужно передавать шлюзу как ключ идемпотентности, чтобы
    повторная доставка события не привела к двойному списанию.
    """

    def __init__(
        self,
        *,
        min_delay: float = 2.0,
        max_delay: float = 5.0,
        success_rate: float = 0.9,
        rng: random.Random | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate
        self._rng = rng or random.Random()
        self._sleep = sleep

    async def charge(self, payment: "Payment") -> PaymentStatus:
        await self._sleep(self._rng.uniform(self._min_delay, self._max_delay))
        succeeded = self._rng.random() < self._success_rate
        return PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED
