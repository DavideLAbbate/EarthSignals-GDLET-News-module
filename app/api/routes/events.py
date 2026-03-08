"""
POST /events/search

Main search endpoint. Accepts frontend filter input, normalizes through Claude,
queries local PostgreSQL, and returns structured JSON results.
Protected by X-API-Key. Rate-limited by slowapi.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import (
    get_anthropic_client,
    verify_api_key,
)
from app.db.session import get_async_session
from app.schemas.events import SearchResponse
from app.services.query_service import search_events

router = APIRouter()


@router.post(
    "/events/search",
    response_model=SearchResponse,
    summary="Search GDELT events with natural-language filters",
    description=(
        "Accepts frontend filter input (country, event type, macro topic, date range). "
        "Normalizes filters via Claude, queries the local PostgreSQL store, "
        "and returns structured event results."
    ),
    tags=["Events"],
)
async def search_events_endpoint(
    filters: dict[str, Any],
    session: AsyncSession = Depends(get_async_session),
    anthropic_client: Any = Depends(get_anthropic_client),
    _: str = Depends(verify_api_key),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> SearchResponse:
    """Search GDELT events using local PostgreSQL store."""
    return await search_events(
        filters,
        session,
        anthropic_client=anthropic_client,
        limit=limit,
        offset=offset,
    )
