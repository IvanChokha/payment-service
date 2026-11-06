"""Доменные исключения."""


class IdempotencyConflict(Exception):
    """Idempotency-Key уже использован с другими параметрами запроса."""


class PaymentNotFound(Exception):
    """Платёж из события не найден в БД: повторять обработку бессмысленно."""


class WebhookDeliveryError(Exception):
    """Webhook не доставлен: сетевая ошибка, таймаут или ответ не 2xx."""
