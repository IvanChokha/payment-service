# Payment Processing Service

Асинхронный микросервис обработки платежей. API принимает запрос на оплату и сразу отвечает
`202 Accepted`; платёж «проводится» в фоне через эмуляцию внешнего шлюза, а результат уходит
клиенту webhook-ом.

**Стек:** FastAPI + Pydantic v2 · SQLAlchemy 2.0 (async, asyncpg) · PostgreSQL · RabbitMQ через
FastStream · Alembic · Docker Compose.

## Архитектура

```mermaid
sequenceDiagram
    participant C as Клиент
    participant A as API
    participant DB as PostgreSQL
    participant R as Outbox relay (в процессе API)
    participant MQ as RabbitMQ
    participant W as Consumer

    C->>A: POST /api/v1/payments (Idempotency-Key)
    A->>DB: INSERT payments + INSERT outbox (одна транзакция)
    A-->>C: 202 Accepted
    R->>DB: SELECT ... FOR UPDATE SKIP LOCKED
    R->>MQ: publish payments.new (с подтверждением брокера)
    R->>DB: published_at = now()
    MQ->>W: payments.new
    W->>W: эмуляция шлюза: 2–5 с, 90 % успех
    W->>DB: UPDATE status, processed_at
    W->>C: POST webhook_url (до 3 попыток)
    Note over W,MQ: не вышло → reject → payments.dlx → payments.new.dlq
```

В `docker-compose.yml` четыре сервиса:

| Сервис     | Назначение |
|------------|------------|
| `postgres` | таблицы `payments` и `outbox` |
| `rabbitmq` | обменник `payments`, очередь `payments.new`, DLX `payments.dlx`, DLQ `payments.new.dlq` |
| `api`      | FastAPI; при старте применяет миграции; в фоне крутит outbox relay |
| `consumer` | FastStream-приложение — единственный обработчик очереди `payments.new` |

## Быстрый старт

```bash
docker compose up --build
```

* API: <http://localhost:8000> (Swagger UI: `/docs`)
* RabbitMQ Management UI: <http://localhost:15672> (guest / guest)
* API-ключ по умолчанию — `dev-api-key` (переопределяется переменной `API_KEY`)

Остановить и удалить данные: `docker compose down -v`.

## Примеры

Приёмник webhook-ов на хосте (только stdlib, ставить ничего не нужно):

```bash
python scripts/webhook_receiver.py        # слушает :9000 и печатает полученные уведомления
```

Из контейнера consumer хост доступен как `host.docker.internal`
(в compose для этого добавлен `extra_hosts`). Если приёмник запущен на другой машине или это
публичный сервис вроде webhook.site — подставьте его URL.

### Создать платёж

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-api-key" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Заказ №42",
        "metadata": {"order_id": 42},
        "webhook_url": "http://host.docker.internal:9000/webhook"
      }'
```

```
HTTP/1.1 202 Accepted
{"payment_id":"6b0e0b6e-6a0c-4d57-9a53-0f0d7f3c1d11","status":"pending","created_at":"2026-11-04T10:15:30.123456Z"}
```

* **Повтор** того же запроса (тот же `Idempotency-Key`) снова даёт `202` с тем же `payment_id` и
  заголовком `Idempotent-Replayed: true`; новый платёж и новое событие не создаются.
* **Тот же ключ с другим телом** → `409 Conflict`.
* Нет заголовка `Idempotency-Key` или невалидное тело (сумма ≤ 0, больше двух знаков после запятой,
  валюта не из `RUB/USD/EUR`, `webhook_url` не http/https) → `422`.
* Нет `X-API-Key` или он неверный → `401` (на всех эндпоинтах `/api/v1/*`).

### Получить платёж

```bash
curl -s http://localhost:8000/api/v1/payments/6b0e0b6e-6a0c-4d57-9a53-0f0d7f3c1d11 \
  -H "X-API-Key: dev-api-key"
```

```json
{
  "payment_id": "6b0e0b6e-6a0c-4d57-9a53-0f0d7f3c1d11",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Заказ №42",
  "metadata": {"order_id": 42},
  "status": "succeeded",
  "idempotency_key": "order-42",
  "webhook_url": "http://host.docker.internal:9000/webhook",
  "created_at": "2026-11-04T10:15:30.123456Z",
  "processed_at": "2026-11-04T10:15:34.501223Z"
}
```

Сразу после создания `status` = `pending`, через 2–5 секунд — `succeeded` (≈90 %) или `failed` (≈10 %).
Суммы (`Decimal`) в JSON передаются строкой, чтобы не терять точность.

### Webhook

`POST` на `webhook_url`, заголовок `X-Payment-Event: payment.succeeded | payment.failed`:

```json
{
  "event": "payment.succeeded",
  "payment_id": "6b0e0b6e-6a0c-4d57-9a53-0f0d7f3c1d11",
  "status": "succeeded",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Заказ №42",
  "metadata": {"order_id": 42},
  "created_at": "2026-11-04T10:15:30.123456Z",
  "processed_at": "2026-11-04T10:15:34.501223Z"
}
```

Успехом считается любой ответ `2xx`. Остальное — сетевая ошибка, таймаут (10 с), `3xx/4xx/5xx` —
считается неудачной попыткой доставки.

## Гарантии доставки

### Outbox pattern

* `POST /payments` в **одной транзакции** пишет строку в `payments` и событие в `outbox`. Либо
  записано и то и другое, либо ничего — «платёж без события» и «событие без платежа» невозможны,
  даже если брокер недоступен или процесс упал.
* Relay (`app/outbox/relay.py`, фоновая задача в процессе API) в цикле читает неопубликованные
  события `SELECT ... ORDER BY created_at LIMIT N FOR UPDATE SKIP LOCKED`, публикует их в RabbitMQ,
  **дожидаясь подтверждения брокера** (publisher confirms, сообщение `persistent`), и только после
  этого проставляет `published_at`.
* **At-least-once.** Если процесс упадёт между публикацией и `commit`, событие уйдёт повторно;
  consumer к этому готов (см. ниже).
* `SKIP LOCKED` делает relay безопасным при нескольких репликах API: каждое событие берёт ровно один.
* При ошибке публикации партия останавливается (порядок сохраняется), причина пишется в
  `outbox.attempts` / `outbox.last_error`, следующая попытка — через 2 с.
* API не зависит от доступности RabbitMQ: соединение и топология создаются лениво, а пока брокера нет,
  события просто копятся в `outbox`.
* Опрос идёт по частичному индексу `WHERE published_at IS NULL` — он остаётся маленьким, сколько бы
  опубликованных строк ни накопилось.

### Идемпотентность

* **Создание.** `payments.idempotency_key` — `UNIQUE`; вставка — `INSERT ... ON CONFLICT
  (idempotency_key) DO NOTHING RETURNING`. Защита держится на индексе, а не на проверке «сначала
  SELECT», поэтому гонка двух одинаковых запросов безопасна: платёж и событие создаст ровно один.
  Второй получит уже существующий платёж. Если тело запроса отличается — `409`.
* **Обработка.** Consumer идемпотентен: если платёж уже не `pending`, шлюз повторно не вызывается,
  остаётся только уведомить клиента. Результат фиксируется под `SELECT ... FOR UPDATE`, поэтому
  при конкурентной доставке дубля статус запишет только первый.
* **Webhook** — at-least-once: после сбоя между отправкой и `ack` клиент может получить
  уведомление дважды. У платежа ровно одно финальное событие, так что дедуплицировать можно
  по `payment_id`.

### Retry и Dead Letter Queue

Одна «попытка» = провести платёж (идемпотентно) + отправить webhook. Любая ошибка — повтор.

| Попытка | Что происходит при неудаче |
|---------|----------------------------|
| 1       | пауза 1 с |
| 2       | пауза 2 с |
| 3       | пауза нет — сообщение отклоняется |

Пауза — `RETRY_BASE_DELAY * 2^(n-1)`, число попыток — `MAX_ATTEMPTS` (по умолчанию 3). Повтор
дёшев: платёж уже в финальном статусе, шлюз не вызывается — заново отправляется то, что упало
(обычно webhook).

После третьей неудачи consumer делает `reject` **без requeue**. Очередь `payments.new` объявлена с
`x-dead-letter-exchange=payments.dlx`, поэтому RabbitMQ сам перекладывает сообщение в
`payments.new.dlq` (RabbitMQ добавляет заголовок `x-death` с причиной и числом отклонений; причина
на стороне приложения — в логах consumer). Туда же попадают битые сообщения, не прошедшие валидацию.
`PaymentNotFound` не повторяется — сразу в DLQ.

> DLQ означает «не удалось довести обработку до конца», а не «платёж не обработан»: сам платёж к
> этому моменту уже в финальном статусе, не доставлен только webhook.

Топология (`app/messaging.py`) объявляется и relay-ем, и consumer-ом с одинаковыми аргументами —
очередь существует и привязана ещё до первой публикации, сообщение не может оказаться
неотправляемым.

## Проверка сценариев

**1. Успешный путь.** Запустить `scripts/webhook_receiver.py`, создать платёж (см. выше), через 2–5 с
увидеть уведомление в приёмнике и `status` ≠ `pending` в `GET`.

**2. Retry → DLQ.** Приёмник, который всегда отвечает 500:

```bash
python scripts/webhook_receiver.py --status 500
```

Создать платёж и посмотреть логи — `docker compose logs -f consumer`: после обработки платежа три
неудачных попытки доставки с паузами 1 с и 2 с, затем сообщение уходит в DLQ. Проверить:

```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages
# либо UI: Queues → payments.new.dlq → Get messages
```

**3. Нестабильный получатель.** `python scripts/webhook_receiver.py --fail-rate 0.5` — часть
уведомлений проходит с повторами, часть доходит до DLQ.

**4. RabbitMQ недоступен.** `docker compose stop rabbitmq`, создать платёж — API по-прежнему
отвечает `202`. Событие ждёт в outbox:

```bash
docker compose exec postgres psql -U payments -c \
  "select id, attempts, last_error, published_at from outbox order by created_at"
```

После `docker compose start rabbitmq` relay публикует накопившееся, `published_at` заполняется.

**5. Consumer остановлен.** `docker compose stop consumer` — сообщения копятся в `payments.new` и
обрабатываются после запуска.

## Конфигурация

Переменные окружения (или `.env`, см. `.env.example`):

| Переменная | По умолчанию | Назначение |
|------------|--------------|------------|
| `DATABASE_URL` | `postgresql+asyncpg://payments:payments@localhost:5432/payments` | подключение к PostgreSQL |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | подключение к RabbitMQ |
| `API_KEY` | `dev-api-key` | значение заголовка `X-API-Key` |
| `LOG_LEVEL` | `INFO` | уровень логирования |
| `OUTBOX_POLL_INTERVAL` | `0.5` | пауза между опросами outbox, с |
| `OUTBOX_BATCH_SIZE` | `100` | размер партии relay |
| `OUTBOX_PUBLISH_TIMEOUT` | `10` | таймаут публикации одного события, с |
| `GATEWAY_MIN_DELAY` / `GATEWAY_MAX_DELAY` | `2` / `5` | задержка эмуляции шлюза, с |
| `GATEWAY_SUCCESS_RATE` | `0.9` | доля успешных платежей |
| `WEBHOOK_TIMEOUT` | `10` | таймаут HTTP-запроса webhook, с |
| `MAX_ATTEMPTS` | `3` | попыток обработки/доставки до DLQ |
| `RETRY_BASE_DELAY` | `1` | база экспоненциальной паузы, с (1 → 1 с, 2 с, 4 с, ...) |
| `CONSUMER_CONCURRENCY` | `20` | сколько событий обрабатывается параллельно |

## Разработка

Запуск приложения вне Docker (PostgreSQL и RabbitMQ — из compose; для этого раскомментируйте
`ports` у `postgres` и `rabbitmq` в `docker-compose.yml`):

```bash
docker compose up -d postgres rabbitmq
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env

alembic upgrade head
uvicorn app.api.main:app --reload          # API
faststream run app.consumer.main:app       # consumer (в другом терминале)
```

Миграции:

```bash
alembic revision --autogenerate -m "описание"   # создать
alembic upgrade head                            # применить
alembic downgrade -1                            # откатить
alembic upgrade head --sql                      # показать SQL, не применяя
```

Тесты (`pytest`) не требуют ни PostgreSQL, ни RabbitMQ:

* retry-логика: экспоненциальные паузы, лимит попыток, непереповторяемые ошибки, ограничение параллелизма;
* outbox relay на подставной сессии: порядок, остановка партии при сбое, учёт ошибок, таймаут;
* эмуляция шлюза, отправка webhook (`httpx.MockTransport`), валидация схем;
* HTTP-слой: `401`, `422`, `409`, `202`/`Idempotent-Replayed`, `404`.

## Структура проекта

```
app/
  config.py            настройки (pydantic-settings)
  db.py                async engine и фабрика сессий
  models.py            Payment, OutboxEvent
  schemas.py           Pydantic v2: запрос, ответы, payload webhook
  messaging.py         топология RabbitMQ: обменники, очереди, DLX/DLQ
  api/                 FastAPI: роуты, X-API-Key, Idempotency-Key, lifespan с relay
  services/payments.py идемпотентное создание платежа + запись в outbox
  outbox/              relay (опрос и публикация) и публикатор в RabbitMQ
  consumer/            FastStream-приложение, handler (retry), processor, gateway, webhook
migrations/            Alembic (async): таблицы payments и outbox
scripts/               webhook_receiver.py — приёмник для ручной проверки
tests/                 юнит-тесты
```

## Допущения и что стоит улучшить для продакшена

* **Пауза между попытками — `sleep` внутри обработчика.** Пока идёт ожидание, сообщение остаётся
  неподтверждённым и занимает слот (`CONSUMER_CONCURRENCY`). Для больших объёмов лучше
  retry-очереди с TTL и DLX (или delayed-message plugin): сообщение не висит в памяти consumer-а.
* **Relay живёт в процессе API** — так в compose ровно те четыре сервиса, что в задании. Класс
  `OutboxRelay` от FastAPI не зависит и выносится в отдельный процесс без изменений.
* **Outbox растёт.** Опубликованные события нужно периодически удалять или архивировать
  (например, `DELETE ... WHERE published_at < now() - interval '7 days'`).
* **Настоящий шлюз** должен получать `payment.id` как ключ идемпотентности — иначе повторная
  доставка события теоретически способна привести к двойному списанию.
* **`webhook_url` задаёт клиент** — это риск SSRF. В продакшене нужны allowlist/запрет внутренних
  адресов и подпись уведомлений (HMAC).
* **Авторизация** — один статический ключ, как в задании; `/health` открыт для healthcheck.
* Интеграционные тесты на реальных PostgreSQL/RabbitMQ (например, testcontainers) не включены.
