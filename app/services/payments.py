"""Бизнес-логика платежей: идемпотентное создание + запись события в outbox."""

import uuid

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.enums import PaymentStatus
from app.events import PAYMENT_NEW
from app.exceptions import IdempotencyConflict
from app.models import OutboxEvent, Payment, utcnow
from app.schemas import PaymentCreate


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, data: PaymentCreate, idempotency_key: str) -> tuple[Payment, bool]:
        """Создаёт платёж и событие outbox в ОДНОЙ транзакции.

        Возвращает (платёж, created). Повторный запрос с тем же Idempotency-Key ничего не создаёт
        и возвращает уже существующий платёж (created=False). Тот же ключ с другим телом запроса —
        IdempotencyConflict.
        """
        async with self._session.begin():
            payment = await self._insert_if_absent(data, idempotency_key)
            if payment is not None:
                self._session.add(
                    OutboxEvent(
                        event_type=PAYMENT_NEW,
                        aggregate_id=payment.id,
                        payload={"payment_id": str(payment.id)},
                    )
                )
                return payment, True
        # Выход из блока — commit: либо платёж и событие записаны вместе, либо ничего.

        # Ключ уже занят: прошлым запросом или конкурентным (его транзакция уже зафиксирована,
        # иначе INSERT ... ON CONFLICT дождался бы её завершения).
        existing = await self._session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if existing is None:  # pragma: no cover - платежи не удаляются
            raise RuntimeError(f"idempotency key {idempotency_key!r} disappeared during request")
        if not self._same_request(existing, data):
            raise IdempotencyConflict(idempotency_key)
        return existing, False

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def _insert_if_absent(self, data: PaymentCreate, idempotency_key: str) -> Payment | None:
        """INSERT ... ON CONFLICT (idempotency_key) DO NOTHING RETURNING *.

        Защита от дублей держится на уникальном индексе, а не на проверке «сначала SELECT»,
        поэтому гонка двух одинаковых запросов безопасна: строку вставит ровно один.
        """
        insert_stmt = (
            pg_insert(Payment)
            .values(
                id=uuid.uuid4(),
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                meta=data.metadata,
                status=PaymentStatus.PENDING,
                idempotency_key=idempotency_key,
                webhook_url=str(data.webhook_url),
                created_at=utcnow(),
            )
            .on_conflict_do_nothing(index_elements=["idempotency_key"])
            .returning(Payment)
        )
        return await self._session.scalar(
            select(Payment).from_statement(insert_stmt).execution_options(populate_existing=True)
        )

    @staticmethod
    def _same_request(payment: Payment, data: PaymentCreate) -> bool:
        return (
            payment.amount == data.amount
            and payment.currency == data.currency
            and payment.description == data.description
            and payment.meta == data.metadata
            and payment.webhook_url == str(data.webhook_url)
        )
