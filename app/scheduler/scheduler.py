"""
APScheduler configuration and lifecycle management.

The AsyncIOScheduler MUST be started inside the FastAPI lifespan context
manager to share uvicorn's event loop. Starting it outside the lifespan
creates a second event loop on Python 3.10+ which causes silent failures.

max_instances=1 on the sync job prevents overlapping executions
if a sync run takes longer than SYNC_INTERVAL_MINUTES.
"""

from __future__ import annotations

import asyncio

import structlog
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.core.config import get_settings
from app.core.logging import get_logger
from app.scheduler.sync_job import run_gdelt_sync

logger = get_logger(__name__)


def _on_job_executed(event) -> None:
    logger.info("scheduler_job_executed", job_id=event.job_id)


def _on_job_error(event) -> None:
    logger.error(
        "scheduler_job_error",
        job_id=event.job_id,
        error=str(event.exception),
        traceback=str(event.traceback),
    )


def create_scheduler() -> AsyncIOScheduler:
    """
    Create and configure the AsyncIOScheduler.

    Does NOT start the scheduler — start() is called in the FastAPI lifespan.
    """
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_listener(_on_job_executed, EVENT_JOB_EXECUTED)
    scheduler.add_listener(_on_job_error, EVENT_JOB_ERROR)
    return scheduler


def add_sync_job(scheduler: AsyncIOScheduler, bq_client) -> None:
    """
    Register the GDELT sync job with the scheduler.

    max_instances=1 ensures the job never overlaps with itself.
    The job is also triggered immediately on startup via trigger_sync_now().
    """
    settings = get_settings()
    scheduler.add_job(
        run_gdelt_sync,
        trigger="interval",
        minutes=settings.sync_interval_minutes,
        id="gdelt_sync",
        max_instances=1,
        replace_existing=True,
        kwargs={"bq_client": bq_client},
    )
    logger.info(
        "sync_job_registered",
        interval_minutes=settings.sync_interval_minutes,
    )


async def trigger_sync_now(bq_client) -> None:
    """
    Trigger an immediate sync outside the scheduler interval.

    Used on startup and by POST /sync/refresh.
    Runs the sync job directly as a coroutine (not via scheduler).
    """
    logger.info("sync_triggered_manually")
    await run_gdelt_sync(bq_client)
