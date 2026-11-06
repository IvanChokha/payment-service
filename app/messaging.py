"""Топология RabbitMQ — общая для публикатора (outbox relay) и consumer-а.

    exchange `payments` (direct)
        └─ routing key `payments.new` ─▶ очередь `payments.new` ─▶ consumer
                                              │  reject (попытки исчерпаны / битое сообщение)
                                              ▼  x-dead-letter-exchange
    exchange `payments.dlx` (direct)
        └─ routing key `payments.new.dlq` ─▶ очередь `payments.new.dlq`  (Dead Letter Queue)
"""

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.events import PAYMENT_NEW

DEAD_LETTER_ROUTING_KEY = "payments.new.dlq"

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
DEAD_LETTER_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

PAYMENTS_NEW_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=PAYMENT_NEW,
    arguments={
        "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE.name,
        "x-dead-letter-routing-key": DEAD_LETTER_ROUTING_KEY,
    },
)
DEAD_LETTER_QUEUE = RabbitQueue(
    "payments.new.dlq",
    durable=True,
    routing_key=DEAD_LETTER_ROUTING_KEY,
)


async def declare_topology(broker: RabbitBroker) -> None:
    """Идемпотентно объявляет обменники, очереди и привязки.

    Вызывается и публикатором, и consumer-ом: очередь гарантированно существует и привязана
    ещё до первой публикации, так что сообщение не может оказаться неотправляемым (unroutable).
    Аргументы очереди одинаковы везде, поэтому повторное объявление безопасно.
    """
    dead_letter_exchange = await broker.declare_exchange(DEAD_LETTER_EXCHANGE)
    dead_letter_queue = await broker.declare_queue(DEAD_LETTER_QUEUE)
    await dead_letter_queue.bind(dead_letter_exchange, routing_key=DEAD_LETTER_ROUTING_KEY)

    exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    queue = await broker.declare_queue(PAYMENTS_NEW_QUEUE)
    await queue.bind(exchange, routing_key=PAYMENT_NEW)
