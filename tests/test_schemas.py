"""Валидация входных данных и форма ответов."""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.enums import Currency, PaymentStatus
from app.schemas import PaymentAccepted, PaymentCreate, PaymentOut, WebhookPayload
from tests.factories import CREATED_AT, make_payment


def valid_body(**overrides):
    body = {"amount": "100.50", "currency": "RUB", "webhook_url": "https://client.example/hook"}
    body.update(overrides)
    return body


def test_minimal_body_gets_defaults():
    data = PaymentCreate(**valid_body())

    assert data.description == ""
    assert data.metadata == {}
    assert data.currency is Currency.RUB
    assert data.amount == Decimal("100.50")


def test_accepts_json_number_amount():
    data = PaymentCreate.model_validate_json(
        '{"amount": 100.5, "currency": "USD", "webhook_url": "http://localhost:9000/hook"}'
    )

    assert data.amount == Decimal("100.5")


@pytest.mark.parametrize("amount", ["0", "-5", "10.999", "abc", None])
def test_rejects_invalid_amount(amount):
    with pytest.raises(ValidationError):
        PaymentCreate(**valid_body(amount=amount))


@pytest.mark.parametrize("currency", ["GBP", "rub", "", None])
def test_rejects_unsupported_currency(currency):
    with pytest.raises(ValidationError):
        PaymentCreate(**valid_body(currency=currency))


@pytest.mark.parametrize("url", ["not-a-url", "client.example/hook", "ftp://client.example/hook", ""])
def test_rejects_invalid_webhook_url(url):
    with pytest.raises(ValidationError):
        PaymentCreate(**valid_body(webhook_url=url))


def test_payment_accepted_has_the_documented_fields():
    payment = make_payment()

    dumped = PaymentAccepted.from_payment(payment).model_dump(mode="json")

    assert set(dumped) == {"payment_id", "status", "created_at"}
    assert dumped["payment_id"] == str(payment.id)
    assert dumped["status"] == "pending"


def test_payment_out_exposes_metadata_not_the_orm_attribute_name():
    payment = make_payment(status=PaymentStatus.SUCCEEDED, processed_at=CREATED_AT)

    dumped = PaymentOut.from_payment(payment).model_dump(mode="json")

    assert dumped["payment_id"] == str(payment.id)
    assert dumped["metadata"] == {"order_id": 1}
    assert dumped["amount"] == "100.50"
    assert dumped["status"] == "succeeded"
    assert dumped["idempotency_key"] == "key-1"
    assert "meta" not in dumped


@pytest.mark.parametrize(
    ("status", "event"),
    [(PaymentStatus.SUCCEEDED, "payment.succeeded"), (PaymentStatus.FAILED, "payment.failed")],
)
def test_webhook_event_follows_status(status, event):
    payload = WebhookPayload.from_payment(make_payment(status=status, processed_at=CREATED_AT))

    assert payload.event == event
