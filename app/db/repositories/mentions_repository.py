"""Repository for gdelt_mentions — insert and query EVENTMENTIONS rows."""

from __future__ import annotations

import math
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.db.models import GdeltMention
from app.db.repositories._upsert import make_insert_ignore

logger = get_logger(__name__)

# asyncpg hard limit: 32767 bound parameters per statement.
_MAX_PG_ARGS = 32_767
_MAX_SQLITE_ARGS = 999
_MENTION_COLUMNS = len(GdeltMention.__table__.columns)


def _chunk_size(session: AsyncSession) -> int:
    """Return the max rows per INSERT for the active dialect."""
    dialect = session.bind.dialect.name if session.bind is not None else "postgresql"
    max_args = _MAX_SQLITE_ARGS if dialect == "sqlite" else _MAX_PG_ARGS
    return max(1, math.floor(max_args / _MENTION_COLUMNS))


class MentionsRepository:
    """Data access layer for the gdelt_mentions table."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def bulk_upsert(self, rows: list[dict[str, Any]]) -> int:
        """Insert mentions rows, ignoring duplicates on (global_event_id, mention_identifier).

        Chunks rows to stay below the asyncpg 32 767-parameter limit (8 columns
        per row → max ~4 095 rows per statement). Uses dialect-aware INSERT IGNORE
        / ON CONFLICT DO NOTHING so the same code works in both PostgreSQL and the
        in-memory SQLite test environment.

        Returns the number of rows submitted (rowcount is unreliable across dialects
        for INSERT IGNORE, so we return len(rows) when rowcount is negative).
        """
        if not rows:
            return 0
        size = _chunk_size(self._session)
        inserted_total = 0
        for start in range(0, len(rows), size):
            chunk = rows[start : start + size]
            stmt = make_insert_ignore(self._session, GdeltMention, chunk)
            result = await self._session.execute(stmt)
            inserted_total += (
                result.rowcount
                if result.rowcount is not None and result.rowcount >= 0
                else len(chunk)
            )
        logger.info("mentions_upserted", count=inserted_total)
        return inserted_total

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
