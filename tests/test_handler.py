"""Retry-логика обработчика: экспоненциальная пауза, лимит попыток, непереповторяемые ошибки."""

import asyncio
from uuid import uuid4

import pytest

from app.consumer.handler import PaymentEventHandler
from app.exceptions import PaymentNotFound, WebhookDeliveryError

PAYMENT_ID = uuid4()


class Scripted:
    """Базовый заменитель: на каждый вызов бросает следующую заготовленную ошибку (None — успех)."""

    def __init__(self, errors=()):
        self._errors = list(errors)
        self.calls = 0

    def _step(self):
        self.calls += 1
        if self._errors:
            error = self._errors.pop(0)
            if error is not None:
                raise error


class FakeProcessor(Scripted):
    async def process(self, payment_id):
        self._step()
        return object()


class FakeNotifier(Scripted):
    async def notify(self, payment):
        self._step()


class Sleeps:
    """Записывает запрошенные паузы вместо реального ожидания."""

    def __init__(self):
        self.delays = []

    async def __call__(self, delay):
        self.delays.append(delay)


def make_handler(processor, notifier, sleeps, **kwargs):
    return PaymentEventHandler(processor, notifier, sleep=sleeps, **kwargs)


async def test_success_on_first_attempt_does_not_sleep():
    processor, notifier, sleeps = FakeProcessor(), FakeNotifier(), Sleeps()

    await make_handler(processor, notifier, sleeps).handle(PAYMENT_ID)

    assert (processor.calls, notifier.calls) == (1, 1)
    assert sleeps.delays == []


async def test_webhook_failures_are_retried_with_exponential_backoff():
    processor = FakeProcessor()
    notifier = FakeNotifier([WebhookDeliveryError("500"), WebhookDeliveryError("500"), None])
    sleeps = Sleeps()

    await make_handler(processor, notifier, sleeps).handle(PAYMENT_ID)

    assert notifier.calls == 3
    assert sleeps.delays == [1.0, 2.0]  # base * 2**(n-1)


async def test_gives_up_after_max_attempts_and_reraises_last_error():
    notifier = FakeNotifier([WebhookDeliveryError(f"boom {n}") for n in (1, 2, 3, 4)])
    sleeps = Sleeps()

    with pytest.raises(WebhookDeliveryError, match="boom 3"):
        await make_handler(FakeProcessor(), notifier, sleeps, max_attempts=3).handle(PAYMENT_ID)

    assert notifier.calls == 3
    assert sleeps.delays == [1.0, 2.0]  # после последней неудачи не ждём


async def test_processing_errors_are_retried_too():
    processor = FakeProcessor([RuntimeError("db is down"), None])
    notifier, sleeps = FakeNotifier(), Sleeps()

    await make_handler(processor, notifier, sleeps).handle(PAYMENT_ID)

    assert (processor.calls, notifier.calls) == (2, 1)
    assert sleeps.delays == [1.0]


async def test_payment_not_found_is_not_retried():
    processor = FakeProcessor([PaymentNotFound("missing")])
    notifier, sleeps = FakeNotifier(), Sleeps()

    with pytest.raises(PaymentNotFound):
        await make_handler(processor, notifier, sleeps).handle(PAYMENT_ID)

    assert (processor.calls, notifier.calls) == (1, 0)
    assert sleeps.delays == []


async def test_base_delay_and_attempts_are_configurable():
    notifier = FakeNotifier([WebhookDeliveryError("x")] * 4 + [None])
    sleeps = Sleeps()

    await make_handler(
        FakeProcessor(), notifier, sleeps, max_attempts=5, base_delay=0.5
    ).handle(PAYMENT_ID)

    assert sleeps.delays == [0.5, 1.0, 2.0, 4.0]


async def test_concurrency_is_bounded():
    active = peak = 0

    class SlowProcessor:
        async def process(self, payment_id):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.01)
            active -= 1
            return object()

    handler = make_handler(SlowProcessor(), FakeNotifier(), Sleeps(), max_concurrency=2)

    await asyncio.gather(*(handler.handle(uuid4()) for _ in range(6)))

    assert peak == 2
