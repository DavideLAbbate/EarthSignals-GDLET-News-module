"""Event repository for local GDELT event storage."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import GdeltEvent

if TYPE_CHECKING:
    from collections.abc import Sequence


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

    stmt = insert(GdeltEvent).values(events)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["global_event_id"],
    )
    result = await session.execute(stmt)
    return result.rowcount


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
