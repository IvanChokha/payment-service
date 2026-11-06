"""Публикация событий outbox в RabbitMQ."""

from contextlib import AsyncExitStack

from faststream.rabbit import RabbitBroker

from app.messaging import PAYMENTS_EXCHANGE, declare_topology
from app.models import OutboxEvent


class RabbitEventPublisher:
    """Callable-публикатор для OutboxRelay.

    Соединение и топология создаются лениво при первой отправке, поэтому API стартует и
    принимает платежи, даже если RabbitMQ ещё/уже недоступен: события ждут в outbox.
    """

    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker
        self._stack = AsyncExitStack()
        self._connected = False
        self._topology_declared = False

    async def __call__(self, event: OutboxEvent) -> None:
        await self._ensure_ready()
        # publish дожидается подтверждения брокера (publisher confirms), и только после этого
        # relay отметит событие опубликованным.
        await self._broker.publish(
            event.payload,
            exchange=PAYMENTS_EXCHANGE,
            routing_key=event.event_type,
            message_id=str(event.id),
            correlation_id=str(event.aggregate_id),
            persist=True,  # delivery_mode=2: сообщение переживёт рестарт брокера
        )

    async def close(self) -> None:
        await self._stack.aclose()

    async def _ensure_ready(self) -> None:
        if not self._connected:
            await self._stack.enter_async_context(self._broker)
            self._connected = True
        if not self._topology_declared:
            await declare_topology(self._broker)
            self._topology_declared = True
