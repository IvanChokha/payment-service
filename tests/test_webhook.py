"""Отправка webhook: тело, заголовки и обработка сбоев (httpx.MockTransport, без сети)."""

import json

import httpx
import pytest

from app.consumer.webhook import WebhookNotifier
from app.enums import PaymentStatus
from app.exceptions import WebhookDeliveryError
from tests.factories import CREATED_AT, make_payment


def notifier_with(handler) -> WebhookNotifier:
    return WebhookNotifier(httpx.AsyncClient(transport=httpx.MockTransport(handler)))


async def test_posts_result_to_webhook_url():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["body"] = json.loads(request.content)
        seen["event_header"] = request.headers["X-Payment-Event"]
        return httpx.Response(200)

    payment = make_payment(status=PaymentStatus.SUCCEEDED, processed_at=CREATED_AT)

    await notifier_with(handler).notify(payment)

    assert seen["url"] == "https://client.example/hook"
    assert seen["event_header"] == "payment.succeeded"
    assert seen["body"]["event"] == "payment.succeeded"
    assert seen["body"]["payment_id"] == str(payment.id)
    assert seen["body"]["status"] == "succeeded"
    assert seen["body"]["amount"] == "100.50"
    assert seen["body"]["currency"] == "RUB"
    assert seen["body"]["metadata"] == {"order_id": 1}


async def test_failed_payment_is_reported_as_failed_event():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = json.loads(request.content)
        return httpx.Response(204)

    payment = make_payment(status=PaymentStatus.FAILED, processed_at=CREATED_AT)

    await notifier_with(handler).notify(payment)

    assert seen["body"]["event"] == "payment.failed"
    assert seen["body"]["status"] == "failed"


@pytest.mark.parametrize("status_code", [301, 400, 404, 500, 503])
async def test_non_2xx_response_is_a_delivery_error(status_code):
    payment = make_payment(status=PaymentStatus.SUCCEEDED, processed_at=CREATED_AT)
    notifier = notifier_with(lambda request: httpx.Response(status_code))

    with pytest.raises(WebhookDeliveryError, match=str(status_code)):
        await notifier.notify(payment)


async def test_network_error_is_a_delivery_error():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    payment = make_payment(status=PaymentStatus.SUCCEEDED, processed_at=CREATED_AT)

    with pytest.raises(WebhookDeliveryError):
        await notifier_with(handler).notify(payment)


async def test_error_message_does_not_leak_url_secrets():
    payment = make_payment(
        status=PaymentStatus.SUCCEEDED,
        processed_at=CREATED_AT,
        webhook_url="https://user:secret@client.example/hook?token=abc",
    )
    notifier = notifier_with(lambda request: httpx.Response(500))

    with pytest.raises(WebhookDeliveryError) as exc_info:
        await notifier.notify(payment)

    message = str(exc_info.value)
    assert "client.example" in message
    assert "secret" not in message and "token" not in message
