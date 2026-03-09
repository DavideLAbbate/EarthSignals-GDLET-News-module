"""Event repository for local GDELT event storage."""

from __future__ import annotations

import math
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, func, select, update
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


async def get_pending_enrichment_candidates(
    session: AsyncSession,
    limit: int,
) -> Sequence[GdeltEvent]:
    """Return pending enrichment candidates in a deterministic order."""
    if limit <= 0:
        return []

    stmt = (
        select(GdeltEvent)
        .where(GdeltEvent.enrichment_status == "pending")
        .order_by(GdeltEvent.date_added.asc(), GdeltEvent.global_event_id.asc())
        .limit(limit)
    )
    result = await session.execute(stmt)
    return result.scalars().all()


async def mark_event_enrichment_processing(
    session: AsyncSession,
    global_event_id: int,
) -> bool:
    """Transition a pending event into processing state."""
    stmt = (
        update(GdeltEvent)
        .where(
            GdeltEvent.global_event_id == global_event_id,
            GdeltEvent.enrichment_status == "pending",
        )
        .values(enrichment_status="processing", enrichment_error=None)
    )
    result = await session.execute(stmt)
    return (result.rowcount or 0) > 0


async def mark_event_enrichment_succeeded(
    session: AsyncSession,
    global_event_id: int,
    *,
    article_title: str | None,
    article_summary: str | None,
    cited_sources: list[str] | None = None,
    main_topics: list[str] | None = None,
    keywords: list[str] | None = None,
    entities: dict[str, list[str]] | None = None,
    enriched_at: datetime,
) -> bool:
    """Persist a successful enrichment result for a processing event."""
    stmt = (
        update(GdeltEvent)
        .where(
            GdeltEvent.global_event_id == global_event_id,
            GdeltEvent.enrichment_status == "processing",
        )
        .values(
            article_title=article_title,
            article_summary=article_summary,
            cited_sources=cited_sources,
            main_topics=main_topics,
            keywords=keywords,
            entities=entities,
            enrichment_status="enriched",
            enriched_at=enriched_at,
            enrichment_error=None,
        )
    )
    result = await session.execute(stmt)
    return (result.rowcount or 0) > 0


async def mark_event_enrichment_failed(
    session: AsyncSession,
    global_event_id: int,
    *,
    error_message: str,
) -> bool:
    """Persist a failed enrichment attempt without clearing semantic fields."""
    stmt = (
        update(GdeltEvent)
        .where(
            GdeltEvent.global_event_id == global_event_id,
            GdeltEvent.enrichment_status.in_(["pending", "processing"]),
        )
        .values(enrichment_status="failed", enrichment_error=error_message)
    )
    result = await session.execute(stmt)
    return (result.rowcount or 0) > 0


def _get_insert_chunk_size(session: AsyncSession) -> int:
    """Return a safe multi-row insert size for the active database dialect."""
    dialect_name = session.bind.dialect.name if session.bind is not None else "postgresql"
    max_query_args = MAX_QUERY_ARGS_BY_DIALECT.get(dialect_name, 32_767)
    column_count = len(GdeltEvent.__table__.columns)
    return max(1, math.floor(max_query_args / column_count))
