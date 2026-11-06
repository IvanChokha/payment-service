"""Эндпоинты /api/v1/payments."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response, status

from app.api.deps import IdempotencyKey, SessionDep, verify_api_key
from app.exceptions import IdempotencyConflict
from app.schemas import PaymentAccepted, PaymentCreate, PaymentOut
from app.services.payments import PaymentService

router = APIRouter(
    prefix="/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
    responses={401: {"description": "Неверный или отсутствующий X-API-Key"}},
)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    summary="Создать платёж",
    description=(
        "Принимает платёж в асинхронную обработку. Результат придёт на `webhook_url`. "
        "Повтор запроса с тем же `Idempotency-Key` возвращает уже созданный платёж "
        "(заголовок `Idempotent-Replayed: true`)."
    ),
    responses={409: {"description": "Idempotency-Key уже использован с другим телом запроса"}},
)
async def create_payment(
    body: PaymentCreate,
    idempotency_key: IdempotencyKey,
    session: SessionDep,
    response: Response,
) -> PaymentAccepted:
    try:
        payment, created = await PaymentService(session).create(body, idempotency_key)
    except IdempotencyConflict:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail="Idempotency-Key was already used with a different request body",
        ) from None
    if not created:
        response.headers["Idempotent-Replayed"] = "true"
    return PaymentAccepted.from_payment(payment)


@router.get(
    "/{payment_id}",
    response_model=PaymentOut,
    summary="Получить информацию о платеже",
    responses={404: {"description": "Платёж не найден"}},
)
async def get_payment(payment_id: UUID, session: SessionDep) -> PaymentOut:
    payment = await PaymentService(session).get(payment_id)
    if payment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return PaymentOut.from_payment(payment)
