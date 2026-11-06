"""Проведение платежа: чтение из БД → шлюз → фиксация результата."""

import logging
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumer.gateway import PaymentGateway
from app.enums import PaymentStatus
from app.exceptions import PaymentNotFound
from app.models import Payment, utcnow

logger = logging.getLogger("payments.processor")


class PaymentProcessor:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway

    async def process(self, payment_id: UUID) -> Payment:
        """Идемпотентно проводит платёж и возвращает его актуальное состояние.

        Повторная доставка события (или повторная попытка после сбоя webhook) не вызывает
        шлюз заново: платёж уже в финальном статусе, остаётся только уведомить клиента.
        """
        payment = await self._load(payment_id)
        if payment.status != PaymentStatus.PENDING:
            logger.info("payment %s is already %s, gateway call skipped", payment_id, payment.status.value)
            return payment

        # Шлюз вызывается без открытой транзакции: на время 2–5 с ожидания соединение с БД
        # не удерживается.
        outcome = await self._gateway.charge(payment)
        return await self._finalize(payment_id, outcome)

    async def _load(self, payment_id: UUID) -> Payment:
        async with self._session_factory() as session:
            payment = await session.get(Payment, payment_id)
        if payment is None:
            raise PaymentNotFound(str(payment_id))
        return payment

    async def _finalize(self, payment_id: UUID, outcome: PaymentStatus) -> Payment:
        async with self._session_factory() as session, session.begin():
            # Блокировка строки: при конкурентной доставке дубля результат зафиксирует только
            # первый, второй увидит уже финальный статус и ничего не перезапишет.
            payment = await session.get(Payment, payment_id, with_for_update=True)
            if payment is None:
                raise PaymentNotFound(str(payment_id))
            if payment.status == PaymentStatus.PENDING:
                payment.status = outcome
                payment.processed_at = utcnow()
                logger.info("payment %s -> %s", payment_id, outcome.value)
            else:
                logger.info(
                    "payment %s was already finalized as %s by a concurrent delivery",
                    payment_id,
                    payment.status.value,
                )
        return payment
