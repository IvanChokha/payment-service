"""Фабрики тестовых объектов (без обращения к БД)."""

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from app.enums import Currency, PaymentStatus
from app.models import OutboxEvent, Payment

CREATED_AT = datetime(2026, 11, 4, 10, 0, tzinfo=timezone.utc)


def make_payment(**overrides: Any) -> Payment:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "amount": Decimal("100.50"),
        "currency": Currency.RUB,
        "description": "Order #1",
        "meta": {"order_id": 1},
        "status": PaymentStatus.PENDING,
        "idempotency_key": "key-1",
        "webhook_url": "https://client.example/hook",
        "created_at": CREATED_AT,
        "processed_at": None,
    }
    values.update(overrides)
    return Payment(**values)


def make_outbox_event(**overrides: Any) -> OutboxEvent:
    aggregate_id = overrides.pop("aggregate_id", uuid.uuid4())
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "event_type": "payments.new",
        "aggregate_id": aggregate_id,
        "payload": {"payment_id": str(aggregate_id)},
        "created_at": CREATED_AT,
        "attempts": 0,
    }
    values.update(overrides)
    return OutboxEvent(**values)
