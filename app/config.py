"""Настройки приложения: переменные окружения и (опционально) файл .env."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    api_key: str = "dev-api-key"
    log_level: str = "INFO"

    # Outbox relay
    outbox_poll_interval: float = Field(0.5, gt=0, description="Пауза между опросами outbox, сек")
    outbox_batch_size: int = Field(100, gt=0)
    outbox_publish_timeout: float = Field(10.0, gt=0, description="Таймаут публикации одного события, сек")

    # Эмуляция платёжного шлюза
    gateway_min_delay: float = Field(2.0, ge=0)
    gateway_max_delay: float = Field(5.0, ge=0)
    gateway_success_rate: float = Field(0.9, ge=0, le=1)

    # Обработка события и доставка webhook
    webhook_timeout: float = Field(10.0, gt=0)
    max_attempts: int = Field(3, ge=1, description="Попыток обработки/доставки до отправки в DLQ")
    retry_base_delay: float = Field(1.0, ge=0, description="Пауза между попытками: base * 2**(n-1)")
    consumer_concurrency: int = Field(20, ge=1, description="Сколько событий обрабатывается параллельно")


@lru_cache
def get_settings() -> Settings:
    return Settings()
