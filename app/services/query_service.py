"""Query service for searching GDELT events."""

from __future__ import annotations

import anthropic
import time
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import GdeltEvent, SyncState
from app.integrations.gdelt_result_mapper import map_rows_to_events
from app.integrations.postgres_compiler import PostgresQueryCompiler
from app.schemas.events import ResponseMetadata, SearchResponse
from app.schemas.filters import RawFilterInput

from .filter_service import normalize_filters


async def search_events(
    raw_filters: dict[str, Any],
    session: AsyncSession,
    anthropic_client: anthropic.AsyncAnthropic | None = None,
    limit: int = 100,
    offset: int = 0,
) -> SearchResponse:
    """
    Search events using local PostgreSQL store.

    This is the primary runtime query path - queries against the local
    gdelt_events table instead of live BigQuery.
    """
    start_time = time.perf_counter()

    # Convert dict to RawFilterInput for normalization
    raw_filter_input = RawFilterInput.model_validate(raw_filters)

    # Normalize filters (may use Anthropic for free-text)
    # Note: anthropic_client is guaranteed to be provided when free-text filters are present
    normalized = await normalize_filters(
        raw_filter_input,
        session,
        anthropic_client,  # type: ignore[arg-type]
    )

    # Compile to PostgreSQL query
    compiler = PostgresQueryCompiler()
    stmt = compiler.compile(
        normalized,
        limit=limit,
        offset=offset,
    )

    # Execute query against local store
    result = await session.execute(stmt)
    events = result.scalars().all()

    # Map to response format
    event_dicts = [_event_to_dict(e) for e in events]
    mapped_events = map_rows_to_events(event_dicts)

    # Get total count
    count_stmt = compiler.compile_count(normalized)
    count_result = await session.execute(count_stmt)
    total_count = count_result.scalar_one()

    # Get sync metadata
    latest_sync = await _get_latest_sync(session)

    query_time_ms = int((time.perf_counter() - start_time) * 1000)

    return SearchResponse(
        filters_received=raw_filters,
        filters_normalized=normalized,
        results=mapped_events,
        metadata=ResponseMetadata(
            total_results=total_count,
            query_time_ms=query_time_ms,
            last_gdelt_sync=latest_sync,
            mapping_version=None,
            bq_bytes_processed=None,
        ),
    )


def _event_to_dict(event: GdeltEvent) -> dict[str, Any]:
    """Convert GdeltEvent model to dict for mapper."""
    return {
        "GLOBALEVENTID": event.global_event_id,
        "SQLDATE": event.sql_date,
        "DATEADDED": event.date_added,
        "Actor1CountryCode": event.actor1_country_code,
        "Actor2CountryCode": event.actor2_country_code,
        "EventCode": event.event_code,
        "EventBaseCode": event.event_base_code,
        "EventRootCode": event.event_root_code,
        "QuadClass": event.quad_class,
        "GoldsteinScale": event.goldstein_scale,
        "AvgTone": event.avg_tone,
        "NumMentions": event.num_mentions,
        "NumSources": event.num_sources,
        "NumArticles": event.num_articles,
        "ActionGeo_FullName": event.action_geo_full_name,
        "ActionGeo_CountryCode": event.action_geo_country_code,
        "SOURCEURL": event.source_url,
    }


async def _get_latest_sync(session: AsyncSession) -> str | None:
    """Get the most recent sync timestamp."""
    stmt = (
        select(SyncState)
        .where(SyncState.sync_status == "completed")
        .order_by(SyncState.synced_at.desc())
        .limit(1)
    )
    result = await session.execute(stmt)
    sync = result.scalar_one_or_none()

    if sync and sync.synced_at:
        return sync.synced_at.isoformat()
    return None
