"""Outbox relay: переносит неопубликованные события из таблицы outbox в брокер."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import OutboxEvent, utcnow

logger = logging.getLogger("payments.outbox")

Publisher = Callable[[OutboxEvent], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class FlushResult:
    published: int
    failed: bool = False


class OutboxRelay:
    """Опрашивает outbox и публикует события в порядке создания.

    Гарантии:
    * at-least-once: published_at проставляется только после подтверждения брокера. Если процесс
      упадёт между публикацией и commit, событие уйдёт повторно — consumer идемпотентен.
    * несколько экземпляров relay (реплики API) безопасны: строки берутся через
      SELECT ... FOR UPDATE SKIP LOCKED, и каждое событие обрабатывает ровно один из них.
    * при ошибке публикации партия останавливается (порядок сохраняется), ошибка записывается
      в outbox.attempts / outbox.last_error, а попытка повторяется на следующем цикле.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publish: Publisher,
        *,
        batch_size: int = 100,
        poll_interval: float = 0.5,
        error_delay: float = 2.0,
        publish_timeout: float = 10.0,
    ) -> None:
        self._session_factory = session_factory
        self._publish = publish
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._error_delay = error_delay
        self._publish_timeout = publish_timeout

    async def run(self) -> None:
        """Бесконечный цикл; останавливается отменой задачи."""
        logger.info("outbox relay started (batch=%d, poll=%.2fs)", self._batch_size, self._poll_interval)
        while True:
            try:
                result = await self.flush_once()
            except Exception:
                logger.exception("outbox relay iteration failed")
                result = FlushResult(published=0, failed=True)

            if result.failed:
                await asyncio.sleep(self._error_delay)
            elif result.published < self._batch_size:
                await asyncio.sleep(self._poll_interval)
            # полная партия — сразу берём следующую, в очереди может быть ещё

    async def flush_once(self) -> FlushResult:
        """Публикует одну партию событий в одной транзакции."""
        published = 0
        failed = False
        async with self._session_factory() as session, session.begin():
            stmt = (
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.created_at, OutboxEvent.id)
                .limit(self._batch_size)
                .with_for_update(skip_locked=True)
            )
            events = (await session.scalars(stmt)).all()

            for event in events:
                try:
                    await asyncio.wait_for(self._publish(event), timeout=self._publish_timeout)
                except Exception as exc:
                    event.attempts += 1
                    event.last_error = repr(exc)[:1000]
                    logger.warning(
                        "outbox event %s was not published (failed attempts: %d): %r",
                        event.id,
                        event.attempts,
                        exc,
                    )
                    failed = True
                    break
                event.published_at = utcnow()
                published += 1
            # Выход из блока — commit: фиксируются и опубликованные события, и учёт неудачи.

        if published:
            logger.debug("outbox relay published %d event(s)", published)
        return FlushResult(published=published, failed=failed)
