"""Эмуляция шлюза: задержка из заданного диапазона и доля успехов."""

import random

from app.consumer.gateway import PaymentGateway
from app.enums import PaymentStatus


class ScriptedRandom:
    """Замена random.Random с заранее заданными значениями."""

    def __init__(self, uniform_value, random_values):
        self.uniform_value = uniform_value
        self.uniform_args = []
        self._values = list(random_values)

    def uniform(self, low, high):
        self.uniform_args.append((low, high))
        return self.uniform_value

    def random(self):
        return self._values.pop(0)


async def test_delay_is_drawn_from_configured_range():
    slept = []

    async def sleep(delay):
        slept.append(delay)

    rng = ScriptedRandom(3.3, [0.0])
    gateway = PaymentGateway(min_delay=2, max_delay=5, rng=rng, sleep=sleep)

    await gateway.charge(object())

    assert rng.uniform_args == [(2, 5)]
    assert slept == [3.3]


async def test_outcome_follows_success_rate_threshold():
    async def no_sleep(delay):
        pass

    rng = ScriptedRandom(0, [0.0, 0.89, 0.9, 0.99])
    gateway = PaymentGateway(success_rate=0.9, rng=rng, sleep=no_sleep)

    outcomes = [await gateway.charge(object()) for _ in range(4)]

    assert outcomes == [
        PaymentStatus.SUCCEEDED,
        PaymentStatus.SUCCEEDED,
        PaymentStatus.FAILED,
        PaymentStatus.FAILED,
    ]


async def test_about_ninety_percent_succeed():
    async def no_sleep(delay):
        pass

    gateway = PaymentGateway(
        min_delay=0, max_delay=0, success_rate=0.9, rng=random.Random(12345), sleep=no_sleep
    )

    outcomes = [await gateway.charge(object()) for _ in range(5000)]
    share = outcomes.count(PaymentStatus.SUCCEEDED) / len(outcomes)

    assert 0.87 < share < 0.93
