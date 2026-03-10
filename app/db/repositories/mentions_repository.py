"""Repository for gdelt_mentions — insert and query EVENTMENTIONS rows."""

from __future__ import annotations

from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.db.models import GdeltMention
from app.db.repositories._upsert import make_insert_ignore

logger = get_logger(__name__)


class MentionsRepository:
    """Data access layer for the gdelt_mentions table."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def bulk_upsert(self, rows: list[dict[str, Any]]) -> int:
        """Insert mentions rows, ignoring duplicates on (global_event_id, mention_identifier).

        Uses dialect-aware INSERT IGNORE / ON CONFLICT DO NOTHING so the same
        code works in both the production PostgreSQL database and the in-memory
        SQLite test environment.

        Returns the number of rows submitted (rowcount is unreliable across dialects
        for INSERT IGNORE, so we return len(rows) when rowcount is negative).
        """
        if not rows:
            return 0
        stmt = make_insert_ignore(self._session, GdeltMention, rows)
        result = await self._session.execute(stmt)
        inserted = (
            result.rowcount if result.rowcount is not None and result.rowcount >= 0 else len(rows)
        )
        logger.info("mentions_upserted", count=inserted)
        return inserted

    async def get_by_event_ids(self, event_ids: list[int]) -> list[GdeltMention]:
        """Return all mentions for the given GDELT event IDs."""
        if not event_ids:
            return []
        result = await self._session.execute(
            select(GdeltMention).where(GdeltMention.global_event_id.in_(event_ids))
        )
        return list(result.scalars().all())

    async def delete_before_dateadded(self, cutoff_dateadded: int) -> int:
        """Delete mentions whose mention_time_date is older than cutoff_dateadded.

        Returns the number of rows deleted.
        """
        result = await self._session.execute(
            delete(GdeltMention).where(GdeltMention.mention_time_date < cutoff_dateadded)
        )
        return result.rowcount or 0
