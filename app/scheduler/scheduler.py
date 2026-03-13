"""
APScheduler configuration and lifecycle management.

The AsyncIOScheduler MUST be started inside the FastAPI lifespan context
manager to share uvicorn's event loop. Starting it outside the lifespan
creates a second event loop on Python 3.10+ which causes silent failures.

max_instances=1 on the sync job prevents overlapping executions
if a sync run takes longer than SYNC_INTERVAL_MINUTES.
"""

from __future__ import annotations

from functools import partial
from typing import Any

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import get_settings
from app.core.logging import get_logger
from app.db.session import _get_session_factory
from app.scheduler.cluster_job import run_cluster_job
from app.scheduler.sync_job import run_gdelt_sync
from app.services.event_enrichment_service import run_event_enrichment_batch
from app.services.ingestion_service import (
    run_bootstrap,
    run_incremental,
    should_bootstrap_on_startup,
)

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


def add_sync_job(scheduler: AsyncIOScheduler) -> None:
    """
    Register the GDELT sync job with the scheduler.

    max_instances=1 ensures the job never overlaps with itself.
    The job is also triggered immediately on startup via trigger_sync_now().
    """
    settings = get_settings()
    session_factory = _get_session_factory()

    if settings.enable_metadata_sync:
        scheduler.add_job(
            run_gdelt_sync,
            trigger="interval",
            minutes=settings.sync_interval_minutes,
            id="gdelt_sync",
            max_instances=1,
            replace_existing=True,
        )
        logger.info(
            "sync_job_registered",
            interval_minutes=settings.sync_interval_minutes,
        )
    else:
        logger.info("sync_job_skipped", reason="disabled_by_config")

    # Ingestion job - hourly incremental
    scheduler.add_job(
        partial(run_ingestion_job, session_factory, "incremental"),
        "interval",
        minutes=settings.ingestion_interval_minutes,
        id="gdelt_incremental_ingestion",
        max_instances=1,
        replace_existing=True,
    )
    logger.info(
        "ingestion_job_registered",
        job_type="incremental",
        interval_minutes=settings.ingestion_interval_minutes,
    )

    if settings.enable_event_enrichment:
        scheduler.add_job(
            partial(run_event_enrichment_job, session_factory),
            "interval",
            minutes=settings.event_enrichment_interval_minutes,
            id="gdelt_event_enrichment",
            max_instances=1,
            replace_existing=True,
        )
        logger.info(
            "event_enrichment_job_registered",
            interval_minutes=settings.event_enrichment_interval_minutes,
        )
    else:
        logger.info("event_enrichment_job_skipped", reason="disabled_by_config")

    if settings.enable_cluster_materialisation:
        scheduler.add_job(
            partial(run_cluster_job, session_factory),
            "interval",
            minutes=settings.cluster_interval_minutes,
            id="gdelt_cluster_materialisation",
            max_instances=1,
            replace_existing=True,
        )
        logger.info(
            "cluster_materialisation_job_registered",
            interval_minutes=settings.cluster_interval_minutes,
        )
    else:
        logger.info("cluster_materialisation_job_skipped", reason="disabled_by_config")

    # Retention cleanup - daily
    scheduler.add_job(
        partial(run_retention_job, session_factory),
        "interval",
        hours=24,
        id="gdelt_retention_cleanup",
        max_instances=1,
        replace_existing=True,
    )
    logger.info("retention_job_registered", interval_hours=24)


async def trigger_sync_now() -> None:
    """
    Trigger an immediate sync outside the scheduler interval.

    Used on startup and by POST /sync/refresh.
    Runs the sync job directly as a coroutine (not via scheduler).
    """
    logger.info("sync_triggered_manually")
    await run_gdelt_sync()


async def trigger_startup_ingestion_if_needed() -> None:
    """Run the initial bootstrap ingestion once when local event storage is empty."""
    session_factory = _get_session_factory()

    async with session_factory() as session:
        if not await should_bootstrap_on_startup(session):
            logger.info("startup_bootstrap_skipped")
            return

        logger.info("startup_bootstrap_triggered")
        await run_bootstrap(session)


async def run_ingestion_job(
    session_factory: async_sessionmaker[AsyncSession],
    job_type: str = "incremental",
) -> dict[str, Any]:
    """Run an ingestion job (bootstrap or incremental)."""
    if job_type not in {"bootstrap", "incremental"}:
        raise ValueError(f"invalid ingestion job_type: {job_type}")

    async with session_factory() as session:
        if job_type == "bootstrap":
            return await run_bootstrap(session)

        return await run_incremental(session)


async def run_retention_job(
    session_factory: async_sessionmaker[AsyncSession],
) -> dict[str, Any]:
    """
    Run the retention cleanup job.
    """
    from app.services.ingestion_service import run_retention_cleanup

    async with session_factory() as session:
        return await run_retention_cleanup(session)


async def run_event_enrichment_job(
    session_factory: async_sessionmaker[AsyncSession],
) -> dict[str, Any]:
    """Run the scheduled event enrichment batch with a fresh DB session."""
    settings = get_settings()

    async with session_factory() as session:
        return await run_event_enrichment_batch(
            session,
            batch_size=settings.event_enrichment_batch_size,
        )
