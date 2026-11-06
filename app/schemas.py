"""Pydantic-схемы: тело запроса, ответы API и payload webhook-уведомления."""

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Annotated, Any, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from app.enums import Currency, PaymentStatus

if TYPE_CHECKING:
    from app.models import Payment


class PaymentCreate(BaseModel):
    """Тело POST /api/v1/payments."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "amount": "1500.00",
                    "currency": "RUB",
                    "description": "Заказ №42",
                    "metadata": {"order_id": 42},
                    "webhook_url": "https://example.com/hooks/payments",
                }
            ]
        }
    )

    amount: Annotated[
        Decimal,
        Field(gt=0, max_digits=18, decimal_places=2, description="Сумма, до 2 знаков после запятой"),
    ]
    currency: Currency
    description: Annotated[str, Field(max_length=1024)] = ""
    metadata: dict[str, Any] = Field(default_factory=dict, description="Произвольные данные клиента")
    webhook_url: HttpUrl = Field(description="URL (http/https), на который придёт результат")


class PaymentAccepted(BaseModel):
    """Ответ 202 на создание платежа."""

    payment_id: UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def from_payment(cls, payment: "Payment") -> Self:
        return cls(
            payment_id=payment.id,
            status=payment.status,
            created_at=payment.created_at,
        )


class PaymentOut(BaseModel):
    """Подробная информация о платеже (GET /api/v1/payments/{payment_id})."""

    payment_id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_payment(cls, payment: "Payment") -> Self:
        return cls(
            payment_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.meta,
            status=payment.status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )


class WebhookPayload(BaseModel):
    """Тело webhook-уведомления о результате обработки платежа."""

    event: str = Field(description="payment.succeeded или payment.failed")
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_payment(cls, payment: "Payment") -> Self:
        return cls(
            event=f"payment.{payment.status.value}",
            payment_id=payment.id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.meta,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )
