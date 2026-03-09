"""
GET  /sync/status   — current sync state from PostgreSQL
POST /sync/refresh  — manually trigger an immediate sync

POST /sync/refresh is protected and has a 5-minute cooldown to prevent
abuse from repeated metadata refresh requests.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_bq_client, verify_api_key
from app.core.logging import get_logger
from app.db.repositories.sync_repository import get_latest_sync_state
from app.db.session import get_async_session
from app.integrations.bigquery_client import BigQueryClientWrapper
from app.integrations.country_codes import get_root_code_label
from app.scheduler.scheduler import trigger_sync_now
from app.schemas.sync import SyncStatusResponse, TopCountry, TopEventCode

router = APIRouter()
logger = get_logger(__name__)

# In-memory cooldown tracker for POST /sync/refresh
_last_manual_refresh: datetime | None = None
_MANUAL_REFRESH_COOLDOWN_MINUTES = 5


@router.get(
    "/sync/status",
    response_model=SyncStatusResponse,
    summary="Get GDELT sync status",
    description="Returns the state of the most recent GDELT metadata synchronization.",
    tags=["Sync"],
)
async def get_sync_status(
    session: AsyncSession = Depends(get_async_session),
) -> SyncStatusResponse:
    sync_state = await get_latest_sync_state(session)

    if sync_state is None:
        return SyncStatusResponse(sync_status="not_synced_yet")

    top_countries = [
        TopCountry(fips_code=c["fips_code"], event_count=c["event_count"])
        for c in (sync_state.top_countries or [])
    ]
    top_codes = [
        TopEventCode(
            root_code=c["root_code"],
            label=c.get("label") or get_root_code_label(c["root_code"]),
            event_count=c["event_count"],
        )
        for c in (sync_state.top_event_root_codes or [])
    ]

    return SyncStatusResponse(
        last_sync_at=sync_state.synced_at.isoformat() if sync_state.synced_at else None,
        latest_sqldate=sync_state.latest_sqldate,
        mapping_version=sync_state.mapping_version,
        sync_status=sync_state.sync_status,
        error_message=sync_state.error_message,
        top_countries=top_countries,
        top_event_root_codes=top_codes,
    )


@router.post(
    "/sync/refresh",
    summary="Manually trigger a GDELT sync",
    description=(
        "Immediately triggers a metadata refresh outside the configured schedule. "
        "Subject to a 5-minute cooldown to prevent repeated manual refreshes."
    ),
    tags=["Sync"],
)
async def manual_sync_refresh(
    bq_client: BigQueryClientWrapper = Depends(get_bq_client),
    _: str = Depends(verify_api_key),
) -> dict:
    global _last_manual_refresh

    # ── Cooldown guard ─────────────────────────────────────────────────────
    if _last_manual_refresh is not None:
        elapsed = datetime.now(timezone.utc) - _last_manual_refresh
        cooldown = timedelta(minutes=_MANUAL_REFRESH_COOLDOWN_MINUTES)
        if elapsed < cooldown:
            retry_after = int((cooldown - elapsed).total_seconds())
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail={
                    "error": "cooldown_active",
                    "message": f"Manual refresh is on cooldown. Retry in {retry_after}s.",
                    "retry_after_seconds": retry_after,
                },
                headers={"Retry-After": str(retry_after)},
            )

    _last_manual_refresh = datetime.now(timezone.utc)
    logger.info("manual_sync_refresh_triggered")

    # Run sync in background (fire-and-forget via asyncio.create_task)
    import asyncio

    asyncio.create_task(trigger_sync_now(bq_client))

    return {
        "status": "sync_triggered",
        "message": "Metadata refresh started. Check /sync/status for results.",
    }
