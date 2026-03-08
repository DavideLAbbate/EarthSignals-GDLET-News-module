"""Event repository for local GDELT event storage."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import GdeltEvent

if TYPE_CHECKING:
    from collections.abc import Sequence


MAX_QUERY_ARGS_BY_DIALECT = {
    "postgresql": 32_767,
    "sqlite": 999,
}


async def bulk_insert_events(
    session: AsyncSession,
    events: list[dict[str, Any]],
) -> int:
    """
    Bulk insert events with deduplication using ON CONFLICT DO NOTHING.
    Returns the number of events actually inserted (excluding duplicates).
    """
    if not events:
        return 0

    chunk_size = _get_insert_chunk_size(session)
    inserted_total = 0

    for start in range(0, len(events), chunk_size):
        chunk = events[start : start + chunk_size]
        stmt = insert(GdeltEvent).values(chunk)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["global_event_id"],
        )
        result = await session.execute(stmt)
        inserted_total += result.rowcount or 0

    return inserted_total


async def get_latest_watermark(session: AsyncSession) -> int | None:
    """
    Get the maximum DATEADDED value from stored events.
    Returns None if no events exist.
    """
    stmt = select(func.max(GdeltEvent.date_added))
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def delete_events_before(
    session: AsyncSession,
    cutoff_sql_date: int,
) -> int:
    """
    Delete events with SQLDATE before the cutoff.
    Returns the number of deleted events.
    """
    stmt = delete(GdeltEvent).where(GdeltEvent.sql_date < cutoff_sql_date)
    result = await session.execute(stmt)
    return result.rowcount


async def get_event_count(session: AsyncSession) -> int:
    """
    Get total count of events in local store.
    """
    stmt = select(func.count(GdeltEvent.global_event_id))
    result = await session.execute(stmt)
    return result.scalar_one()


async def get_events_paginated(
    session: AsyncSession,
    offset: int = 0,
    limit: int = 100,
) -> Sequence[GdeltEvent]:
    """
    Get events with pagination for testing/debugging.
    """
    stmt = select(GdeltEvent).order_by(GdeltEvent.date_added.desc()).offset(offset).limit(limit)
    result = await session.execute(stmt)
    return result.scalars().all()


def _get_insert_chunk_size(session: AsyncSession) -> int:
    """Return a safe multi-row insert size for the active database dialect."""
    dialect_name = session.bind.dialect.name if session.bind is not None else "postgresql"
    max_query_args = MAX_QUERY_ARGS_BY_DIALECT.get(dialect_name, 32_767)
    column_count = len(GdeltEvent.__table__.columns)
    return max(1, math.floor(max_query_args / column_count))
