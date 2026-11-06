"""Outbox relay на подставной сессии: порядок, учёт ошибок, таймаут, устойчивость цикла."""

import asyncio

import pytest

from app.outbox.relay import FlushResult, OutboxRelay
from tests.factories import make_outbox_event


class FakeTransaction:
    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._session.committed = exc_type is None
        return False


class FakeScalars:
    def __init__(self, items):
        self._items = items

    def all(self):
        return list(self._items)


class FakeSession:
    """Минимальная замена AsyncSession: отдаёт заготовленные события и помнит commit."""

    def __init__(self, events):
        self.events = events
        self.committed = None

    def begin(self):
        return FakeTransaction(self)

    async def scalars(self, statement):
        return FakeScalars(self.events)


class FakeSessionContext:
    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, *exc):
        return False


def make_relay(events, publish, **kwargs):
    session = FakeSession(events)
    relay = OutboxRelay(lambda: FakeSessionContext(session), publish, **kwargs)
    return relay, session


async def noop_publish(event):
    pass


async def test_empty_outbox():
    relay, _ = make_relay([], noop_publish)

    assert await relay.flush_once() == FlushResult(published=0, failed=False)


async def test_publishes_in_order_and_marks_events_published():
    events = [make_outbox_event() for _ in range(3)]
    published = []

    async def publish(event):
        published.append(event.id)

    relay, session = make_relay(events, publish)

    result = await relay.flush_once()

    assert result == FlushResult(published=3, failed=False)
    assert published == [event.id for event in events]
    assert all(event.published_at is not None for event in events)
    assert session.committed is True


async def test_failure_stops_the_batch_and_records_the_error():
    events = [make_outbox_event() for _ in range(3)]

    async def publish(event):
        if event is events[1]:
            raise ConnectionError("broker is down")

    relay, session = make_relay(events, publish)

    result = await relay.flush_once()

    assert result == FlushResult(published=1, failed=True)
    assert events[0].published_at is not None
    assert events[1].published_at is None
    assert events[1].attempts == 1
    assert "broker is down" in events[1].last_error
    assert events[2].published_at is None and events[2].attempts == 0
    assert session.committed is True  # прогресс и учёт ошибки сохранены


async def test_publish_timeout_counts_as_failure():
    event = make_outbox_event()

    async def slow_publish(event):
        await asyncio.sleep(1)

    relay, _ = make_relay([event], slow_publish, publish_timeout=0.01)

    result = await relay.flush_once()

    assert result.failed is True
    assert event.published_at is None
    assert event.attempts == 1


async def test_run_survives_unexpected_errors():
    relay, _ = make_relay([], noop_publish, poll_interval=0.001, error_delay=0.001)
    calls = 0

    async def flaky_flush():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("database is down")
        return FlushResult(published=0)

    relay.flush_once = flaky_flush
    task = asyncio.create_task(relay.run())
    await asyncio.sleep(0.05)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert calls >= 2
