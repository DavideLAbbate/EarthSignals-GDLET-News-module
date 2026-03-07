"""
GDELT query service.

Translates NormalizedFilters into a BigQuery query,
executes it via the async BigQuery client wrapper,
and maps the results into SearchResponse.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.logging import get_logger
from app.db.repositories.sync_repository import get_latest_sync_state
from app.integrations.bigquery_client import BigQueryClientWrapper
from app.integrations.gdelt_query_builder import build_events_query
from app.integrations.gdelt_result_mapper import map_rows_to_events
from app.schemas.events import ResponseMetadata, SearchResponse
from app.schemas.filters import NormalizedFilters, RawFilterInput

logger = get_logger(__name__)


async def search_events(
    raw_filters: RawFilterInput,
    normalized_filters: NormalizedFilters,
    bq_client: BigQueryClientWrapper,
    session: AsyncSession,
) -> SearchResponse:
    """
    Execute a GDELT event search using normalized filters.

    1. Build parameterized BigQuery SQL from NormalizedFilters
    2. Execute via BigQuery client (async, thread pool)
    3. Map rows to GDELTEvent models
    4. Fetch last sync metadata from DB for the response envelope
    5. Return SearchResponse
    """
    settings = get_settings()
    start_time = time.monotonic()

    sql, params = build_events_query(
        date_from_sqldate=normalized_filters.date_from_sqldate,
        date_to_sqldate=normalized_filters.date_to_sqldate,
        fips_country_code=normalized_filters.fips_country_code,
        geo_country_codes=normalized_filters.geo_country_codes or None,
        cameo_country_code=normalized_filters.cameo_country_code,
        actor1_country_code=normalized_filters.actor1_country_code,
        actor2_country_code=normalized_filters.actor2_country_code,
        event_root_codes=normalized_filters.event_root_codes or None,
        event_base_codes=normalized_filters.event_base_codes or None,
        event_codes=normalized_filters.event_codes or None,
        quad_classes=normalized_filters.quad_classes or None,
        source_domains=normalized_filters.source_domains or None,
        tone_min=normalized_filters.tone_min,
        tone_max=normalized_filters.tone_max,
        goldstein_min=normalized_filters.goldstein_min,
        goldstein_max=normalized_filters.goldstein_max,
        min_mentions=normalized_filters.min_mentions,
        min_sources=normalized_filters.min_sources,
        min_articles=normalized_filters.min_articles,
        max_results=settings.bq_max_results,
    )

    logger.info(
        "gdelt_query_start",
        date_from=normalized_filters.date_from_sqldate,
        date_to=normalized_filters.date_to_sqldate,
        geo_countries=normalized_filters.geo_country_codes,
        event_root_codes=normalized_filters.event_root_codes,
        event_codes=normalized_filters.event_codes,
        source_domains=normalized_filters.source_domains,
    )

    rows = await bq_client.run_query(sql, params)
    events = map_rows_to_events(rows)

    elapsed_ms = int((time.monotonic() - start_time) * 1000)

    # Fetch sync metadata for the response envelope
    sync_state = await get_latest_sync_state(session)
    last_gdelt_sync = None
    mapping_version = None
    if sync_state:
        last_gdelt_sync = sync_state.synced_at.isoformat() if sync_state.synced_at else None
        mapping_version = sync_state.mapping_version

    logger.info(
        "gdelt_query_complete",
        events_returned=len(events),
        query_time_ms=elapsed_ms,
    )

    return SearchResponse(
        filters_received=raw_filters.model_dump(by_alias=True, exclude_none=True),
        filters_normalized=normalized_filters,
        results=events,
        metadata=ResponseMetadata(
            total_results=len(events),
            query_time_ms=elapsed_ms,
            last_gdelt_sync=last_gdelt_sync,
            mapping_version=mapping_version,
        ),
    )
