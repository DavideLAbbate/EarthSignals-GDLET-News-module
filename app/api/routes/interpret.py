"""
POST /filters/interpret

Dry-run endpoint: normalizes filters through Claude and returns
the NormalizedFilters without querying BigQuery.
Useful for frontend filter preview and debugging.
Protected by X-API-Key.
"""

from __future__ import annotations

import anthropic
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_anthropic_client, verify_api_key
from app.db.session import get_async_session
from app.schemas.filters import NormalizedFilters, RawFilterInput
from app.services.filter_service import normalize_filters

router = APIRouter()


@router.post(
    "/filters/interpret",
    response_model=NormalizedFilters,
    summary="Interpret and normalize filters (dry run)",
    description=(
        "Passes the filter input through Claude for normalization and returns the "
        "resulting GDELT query parameters — without executing a BigQuery query. "
        "Use this to preview how filters will be interpreted."
    ),
    tags=["Filters"],
)
async def interpret_filter_dry_run(
    filters: RawFilterInput,
    session: AsyncSession = Depends(get_async_session),
    anthropic_client: anthropic.AsyncAnthropic = Depends(get_anthropic_client),
    _: str = Depends(verify_api_key),
) -> NormalizedFilters:
    return await normalize_filters(filters, session, anthropic_client)
