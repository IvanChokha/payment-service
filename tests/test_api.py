"""HTTP-слой: авторизация, коды ответов, валидация. БД не нужна — сервис подменяется."""

from decimal import Decimal

import httpx
import pytest
from fastapi import FastAPI

from app.api import payments as payments_api
from app.api.deps import get_session
from app.config import get_settings
from app.enums import PaymentStatus
from app.exceptions import IdempotencyConflict
from tests.factories import CREATED_AT, make_payment

API_KEY = "test-api-key"
AUTH = {"X-API-Key": API_KEY}
CREATE_HEADERS = {**AUTH, "Idempotency-Key": "key-1"}
BODY = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Order #1",
    "metadata": {"order_id": 1},
    "webhook_url": "https://client.example/hook",
}


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setenv("API_KEY", API_KEY)
    get_settings.cache_clear()

    application = FastAPI()
    application.include_router(payments_api.router, prefix="/api/v1")

    async def no_db():
        yield None

    application.dependency_overrides[get_session] = no_db
    yield application
    get_settings.cache_clear()


def client_for(application: FastAPI) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=application)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


def fake_service(payment=None, created=True, error=None):
    """Возвращает (класс-замену PaymentService, список вызовов create)."""
    calls = []

    class FakeService:
        def __init__(self, session):
            pass

        async def create(self, data, idempotency_key):
            calls.append((data, idempotency_key))
            if error is not None:
                raise error
            return payment, created

        async def get(self, payment_id):
            return payment if payment is not None and payment.id == payment_id else None

    return FakeService, calls


async def test_create_returns_202_with_payment_id(app, monkeypatch):
    payment = make_payment()
    service, calls = fake_service(payment=payment)
    monkeypatch.setattr(payments_api, "PaymentService", service)

    async with client_for(app) as client:
        response = await client.post("/api/v1/payments", json=BODY, headers=CREATE_HEADERS)

    assert response.status_code == 202
    body = response.json()
    assert body["payment_id"] == str(payment.id)
    assert body["status"] == "pending"
    assert body["created_at"].startswith("2026-11-04T10:00:00")
    assert "idempotent-replayed" not in response.headers
    data, key = calls[0]
    assert key == "key-1"
    assert data.amount == Decimal("100.50")
    assert str(data.webhook_url) == "https://client.example/hook"


async def test_replayed_request_is_flagged(app, monkeypatch):
    service, _ = fake_service(payment=make_payment(), created=False)
    monkeypatch.setattr(payments_api, "PaymentService", service)

    async with client_for(app) as client:
        response = await client.post("/api/v1/payments", json=BODY, headers=CREATE_HEADERS)

    assert response.status_code == 202
    assert response.headers["idempotent-replayed"] == "true"


async def test_key_reuse_with_another_body_is_409(app, monkeypatch):
    service, _ = fake_service(error=IdempotencyConflict("key-1"))
    monkeypatch.setattr(payments_api, "PaymentService", service)

    async with client_for(app) as client:
        response = await client.post("/api/v1/payments", json=BODY, headers=CREATE_HEADERS)

    assert response.status_code == 409


async def test_idempotency_key_header_is_required(app, monkeypatch):
    service, calls = fake_service(payment=make_payment())
    monkeypatch.setattr(payments_api, "PaymentService", service)

    async with client_for(app) as client:
        response = await client.post("/api/v1/payments", json=BODY, headers=AUTH)

    assert response.status_code == 422
    assert calls == []


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong-key"}])
async def test_api_key_is_required_on_every_endpoint(app, monkeypatch, headers):
    payment = make_payment()
    service, calls = fake_service(payment=payment)
    monkeypatch.setattr(payments_api, "PaymentService", service)

    async with client_for(app) as client:
        created = await client.post(
            "/api/v1/payments", json=BODY, headers={**headers, "Idempotency-Key": "key-1"}
        )
        fetched = await client.get(f"/api/v1/payments/{payment.id}", headers=headers)

    assert created.status_code == 401
    assert fetched.status_code == 401
    assert calls == []


@pytest.mark.parametrize(
    "overrides",
    [{"amount": "0"}, {"amount": "1.234"}, {"currency": "GBP"}, {"webhook_url": "nope"}],
)
async def test_invalid_body_is_422(app, monkeypatch, overrides):
    service, calls = fake_service(payment=make_payment())
    monkeypatch.setattr(payments_api, "PaymentService", service)

    async with client_for(app) as client:
        response = await client.post(
            "/api/v1/payments", json={**BODY, **overrides}, headers=CREATE_HEADERS
        )

    assert response.status_code == 422
    assert calls == []


async def test_get_payment(app, monkeypatch):
    payment = make_payment(status=PaymentStatus.SUCCEEDED, processed_at=CREATED_AT)
    service, _ = fake_service(payment=payment)
    monkeypatch.setattr(payments_api, "PaymentService", service)

    async with client_for(app) as client:
        response = await client.get(f"/api/v1/payments/{payment.id}", headers=AUTH)

    assert response.status_code == 200
    body = response.json()
    assert body["payment_id"] == str(payment.id)
    assert body["status"] == "succeeded"
    assert body["metadata"] == {"order_id": 1}
    assert body["idempotency_key"] == "key-1"
    assert body["processed_at"].startswith("2026-11-04T10:00:00")


async def test_get_unknown_payment_is_404(app, monkeypatch):
    service, _ = fake_service(payment=None)
    monkeypatch.setattr(payments_api, "PaymentService", service)

    async with client_for(app) as client:
        response = await client.get(
            "/api/v1/payments/6f1d3c0e-0000-4000-8000-000000000000", headers=AUTH
        )

    assert response.status_code == 404


async def test_malformed_payment_id_is_422(app, monkeypatch):
    service, _ = fake_service(payment=None)
    monkeypatch.setattr(payments_api, "PaymentService", service)

    async with client_for(app) as client:
        response = await client.get("/api/v1/payments/not-a-uuid", headers=AUTH)

    assert response.status_code == 422
