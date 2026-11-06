"""Consumer: faststream run app.consumer.main:app"""

import logging

import httpx
from faststream import FastStream
from faststream.exceptions import RejectMessage
from faststream.rabbit import RabbitBroker

from app.config import get_settings
from app.consumer.gateway import PaymentGateway
from app.consumer.handler import PaymentEventHandler
from app.consumer.processor import PaymentProcessor
from app.consumer.webhook import WebhookNotifier
from app.db import get_engine, get_session_factory
from app.events import PaymentCreatedEvent
from app.logging_setup import setup_logging
from app.messaging import PAYMENTS_EXCHANGE, PAYMENTS_NEW_QUEUE, declare_topology

settings = get_settings()
setup_logging(settings.log_level)
logger = logging.getLogger("payments.consumer")

broker = RabbitBroker(settings.rabbitmq_url)
app = FastStream(broker)

http_client = httpx.AsyncClient(
    timeout=settings.webhook_timeout,
    headers={"User-Agent": "payment-service/1.0"},
)
handler = PaymentEventHandler(
    processor=PaymentProcessor(
        get_session_factory(),
        PaymentGateway(
            min_delay=settings.gateway_min_delay,
            max_delay=settings.gateway_max_delay,
            success_rate=settings.gateway_success_rate,
        ),
    ),
    notifier=WebhookNotifier(http_client),
    max_attempts=settings.max_attempts,
    base_delay=settings.retry_base_delay,
    max_concurrency=settings.consumer_concurrency,
)


@broker.subscriber(PAYMENTS_NEW_QUEUE, PAYMENTS_EXCHANGE)
async def on_payment_created(event: PaymentCreatedEvent) -> None:
    """Единственный обработчик: шлюз → статус в БД → webhook (с повторами).

    Нормальное завершение = ack. Исключение после всех попыток превращается в явный reject без
    requeue — RabbitMQ по x-dead-letter-exchange кладёт сообщение в payments.new.dlq.
    Битое сообщение (не прошло валидацию) тоже отклоняется и попадает в DLQ.
    """
    try:
        await handler.handle(event.payment_id)
    except Exception as exc:
        logger.error(
            "payment %s was not processed, message goes to DLQ: %r",
            event.payment_id,
            exc,
            exc_info=exc,
        )
        raise RejectMessage() from exc


@app.after_startup
async def declare_dead_letter_topology() -> None:
    # Очередь payments.new объявляет подписчик; здесь добавляется DLX/DLQ с привязками.
    await declare_topology(broker)
    logger.info("consumer is ready: queue=%s", PAYMENTS_NEW_QUEUE.name)


@app.after_shutdown
async def release_resources() -> None:
    await http_client.aclose()
    await get_engine().dispose()
