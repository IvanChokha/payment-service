"""Отправка webhook-уведомлений клиенту."""

import logging
from urllib.parse import urlsplit

import httpx

from app.exceptions import WebhookDeliveryError
from app.models import Payment
from app.schemas import WebhookPayload

logger = logging.getLogger("payments.webhook")


def _target(url: str) -> str:
    """Схема + хост + порт для логов: путь и query могут содержать секреты, userinfo — тем более."""
    parts = urlsplit(url)
    port = f":{parts.port}" if parts.port else ""
    return f"{parts.scheme}://{parts.hostname or 'unknown-host'}{port}"


class WebhookNotifier:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def notify(self, payment: Payment) -> None:
        """POST результата на webhook_url. Любой сбой — WebhookDeliveryError (его повторит handler)."""
        payload = WebhookPayload.from_payment(payment)
        target = _target(payment.webhook_url)
        try:
            response = await self._client.post(
                payment.webhook_url,
                json=payload.model_dump(mode="json"),
                headers={"X-Payment-Event": payload.event},
            )
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"request to {target} failed: {exc!r}") from exc

        if not response.is_success:
            raise WebhookDeliveryError(f"{target} responded with HTTP {response.status_code}")
        logger.info(
            "payment %s: %s delivered to %s (HTTP %d)",
            payment.id,
            payload.event,
            target,
            response.status_code,
        )
