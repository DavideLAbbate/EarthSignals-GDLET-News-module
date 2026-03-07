"""
POST /events/search

Main search endpoint. Accepts frontend filter input, normalizes through Claude,
queries BigQuery, and returns structured JSON results.
Protected by X-API-Key. Rate-limited by slowapi.
"""

from __future__ import annotations

import anthropic
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import (
    get_anthropic_client,
    get_bq_client,
    verify_api_key,
)
from app.db.session import get_async_session
from app.integrations.bigquery_client import BigQueryClientWrapper
from app.schemas.events import SearchResponse
from app.schemas.filters import RawFilterInput
from app.services.filter_service import normalize_filters
from app.services.query_service import search_events

router = APIRouter()


@router.post(
    "/events/search",
    response_model=SearchResponse,
    summary="Search GDELT events with natural-language filters",
    description=(
        "Accepts frontend filter input (country, event type, macro topic, date range). "
        "Normalizes filters via Claude, queries the GDELT 2.0 BigQuery dataset, "
        "and returns structured event results."
    ),
    tags=["Events"],
)
async def search_gdelt_events(
    filters: RawFilterInput,
    session: AsyncSession = Depends(get_async_session),
    bq_client: BigQueryClientWrapper = Depends(get_bq_client),
    anthropic_client: anthropic.AsyncAnthropic = Depends(get_anthropic_client),
    _: str = Depends(verify_api_key),
) -> SearchResponse:
    normalized = await normalize_filters(filters, session, anthropic_client)
    return await search_events(filters, normalized, bq_client, session)
