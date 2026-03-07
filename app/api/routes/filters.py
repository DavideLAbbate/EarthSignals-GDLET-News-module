"""
GET /filters/metadata

Returns the top countries and event codes from the latest sync state.
Useful for populating frontend filter dropdowns.
Protected by X-API-Key.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import verify_api_key
from app.db.repositories.sync_repository import get_latest_sync_state
from app.db.session import get_async_session
from app.integrations.country_codes import get_root_code_label
from app.schemas.sync import FiltersMetadataResponse, TopCountry, TopEventCode

router = APIRouter()


@router.get(
    "/filters/metadata",
    response_model=FiltersMetadataResponse,
    summary="Get available filter metadata",
    description=(
        "Returns the top countries and event types active in the last 30 days, "
        "derived from the most recent GDELT sync. Use this to populate frontend filter dropdowns."
    ),
    tags=["Filters"],
)
async def get_filters_metadata(
    session: AsyncSession = Depends(get_async_session),
    _: str = Depends(verify_api_key),
) -> FiltersMetadataResponse:
    sync_state = await get_latest_sync_state(session)

    if sync_state is None:
        return FiltersMetadataResponse()

    top_countries = [
        TopCountry(fips_code=c["fips_code"], event_count=c["event_count"])
        for c in (sync_state.top_countries or [])
    ]

    top_event_codes = [
        TopEventCode(
            root_code=c["root_code"],
            label=c.get("label") or get_root_code_label(c["root_code"]),
            event_count=c["event_count"],
        )
        for c in (sync_state.top_event_root_codes or [])
    ]

    return FiltersMetadataResponse(
        top_countries=top_countries,
        top_event_root_codes=top_event_codes,
        last_sync_at=sync_state.synced_at.isoformat() if sync_state.synced_at else None,
        mapping_version=sync_state.mapping_version,
    )
