"""
POST /enrich/trigger  — manually trigger one enrichment batch
GET  /enrich/status   — last enrichment stats (from scheduler state)
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.dependencies import verify_api_key
from app.core.logging import get_logger
from app.scheduler.scheduler import trigger_enrichment_now

router = APIRouter()
logger = get_logger(__name__)

_last_manual_trigger: datetime | None = None
_COOLDOWN_MINUTES = 1  # enrichment is slow; prevent accidental hammering


@router.post(
    "/enrich/trigger",
    summary="Manually trigger an enrichment batch",
    description=(
        "Immediately runs one enrichment batch (up to EVENT_ENRICHMENT_BATCH_SIZE events) "
        "outside the configured schedule. Subject to a 1-minute cooldown."
    ),
    tags=["Enrichment"],
)
async def manual_enrich_trigger(
    _: str = Depends(verify_api_key),
) -> dict:
    global _last_manual_trigger

    if _last_manual_trigger is not None:
        elapsed = datetime.now(timezone.utc) - _last_manual_trigger
        cooldown = timedelta(minutes=_COOLDOWN_MINUTES)
        if elapsed < cooldown:
            retry_after = int((cooldown - elapsed).total_seconds())
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail={
                    "error": "cooldown_active",
                    "message": f"Enrichment trigger is on cooldown. Retry in {retry_after}s.",
                    "retry_after_seconds": retry_after,
                },
                headers={"Retry-After": str(retry_after)},
            )

    _last_manual_trigger = datetime.now(timezone.utc)
    logger.info("manual_enrichment_trigger_accepted")

    asyncio.create_task(trigger_enrichment_now())

    return {
        "status": "enrichment_triggered",
        "message": "Enrichment batch started in background. Check Docker logs for results.",
    }
