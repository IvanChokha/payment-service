"""Контракт события между API (через outbox) и consumer-ом."""

from uuid import UUID

from pydantic import BaseModel

# Тип события одновременно служит routing key в обменнике `payments`.
PAYMENT_NEW = "payments.new"


class PaymentCreatedEvent(BaseModel):
    payment_id: UUID
