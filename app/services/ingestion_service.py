"""Ingestion service for GDELT event data."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.logging import get_logger
from app.db.repositories import event_repository, ingestion_repository
from app.integrations.gdelt_query_builder import (
    build_ingestion_bootstrap_query,
    build_ingestion_incremental_query,
)

if TYPE_CHECKING:
    from app.integrations.bigquery_client import BigQueryClientWrapper

logger = get_logger(__name__)


def _dateadded_to_sqldate(dateadded: int) -> int:
    """Convert a DATEADDED timestamp (YYYYMMDDHHMMSS) to SQLDATE (YYYYMMDD)."""
    return int(str(dateadded)[:8])


def _iter_bootstrap_windows(
    start: datetime,
    end: datetime,
) -> list[tuple[int, int, int, int]]:
    """Split bootstrap ingestion into non-overlapping daily windows."""
    windows: list[tuple[int, int, int, int]] = []
    cursor = start.replace(hour=0, minute=0, second=0, microsecond=0)

    while cursor < end:
        next_cursor = min(cursor + timedelta(days=1), end)
        windows.append(
            (
                int(cursor.strftime("%Y%m%d%H%M%S")),
                int(next_cursor.strftime("%Y%m%d%H%M%S")),
                int(cursor.strftime("%Y%m%d")),
                int(cursor.strftime("%Y%m%d")),
            )
        )
        cursor = next_cursor

    return windows


def _iter_incremental_windows(
    start: datetime,
    end: datetime,
) -> list[tuple[int, int, int, int]]:
    """Split incremental ingestion into non-overlapping windows from the watermark onward."""
    windows: list[tuple[int, int, int, int]] = []
    cursor = start.replace(microsecond=0)

    while cursor < end:
        next_day = (cursor + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        next_cursor = min(next_day, end)
        windows.append(
            (
                int(cursor.strftime("%Y%m%d%H%M%S")),
                int(next_cursor.strftime("%Y%m%d%H%M%S")),
                int(cursor.strftime("%Y%m%d")),
                int(cursor.strftime("%Y%m%d")),
            )
        )
        cursor = next_cursor

    return windows


async def run_bootstrap(
    bq_client: BigQueryClientWrapper,
    session: AsyncSession,
) -> dict[str, Any]:
    """
    Run bootstrap ingestion - fetch the initial rolling window of events.
    For development, fetches the last 30 days.
    """
    settings = get_settings()
    logger.info("starting_bootstrap_ingestion")

    # Create ingestion run record
    run = await ingestion_repository.create_ingestion_run(
        session,
        ingestion_repository.IngestionType.BOOTSTRAP,
    )
    await session.commit()

    now = datetime.now(timezone.utc)
    start_date = now - timedelta(days=settings.retention_days)

    total_ingested = 0
    last_watermark = int(now.strftime("%Y%m%d%H%M%S"))
    batch_size = settings.ingestion_batch_size

    try:
        for window_start, window_end, date_from_sqldate, date_to_sqldate in _iter_bootstrap_windows(
            start_date,
            now,
        ):
            query, params = build_ingestion_bootstrap_query(
                since_dateadded=window_start,
                date_from_sqldate=date_from_sqldate,
                date_to_sqldate=date_to_sqldate,
                limit=batch_size,
            )

            rows: list[dict[str, Any]] = await bq_client.run_query(query, params)

            if not rows:
                last_watermark = window_end
                continue

            # Transform rows to event dicts
            events = [_row_to_event_dict(row) for row in rows]

            # Insert into local DB
            inserted = await event_repository.bulk_insert_events(
                session,
                events,
            )
            await session.commit()

            total_ingested += inserted
            last_watermark = window_end

            logger.info(
                "bootstrap_batch_ingested",
                batch_size=len(events),
                inserted=inserted,
                total=total_ingested,
                watermark=window_end,
            )

        # Mark as completed
        await ingestion_repository.update_ingestion_run(
            session,
            run.id,
            ingestion_repository.IngestionStatus.COMPLETED,
            watermark_dateadded=last_watermark,
            events_ingested=total_ingested,
        )
        await session.commit()

        logger.info(
            "bootstrap_completed",
            total_ingested=total_ingested,
            watermark=last_watermark,
        )

        return {
            "status": "completed",
            "events_ingested": total_ingested,
            "watermark": last_watermark,
        }

    except Exception as e:
        logger.error("bootstrap_failed", error=str(e))
        await ingestion_repository.update_ingestion_run(
            session,
            run.id,
            ingestion_repository.IngestionStatus.FAILED,
            error_message=str(e),
        )
        await session.commit()
        raise


async def run_incremental(
    bq_client: BigQueryClientWrapper,
    session: AsyncSession,
) -> dict[str, Any]:
    """
    Run incremental ingestion - fetch events newer than the last watermark.
    """
    settings = get_settings()
    logger.info("starting_incremental_ingestion")

    # Get last successful ingestion watermark
    last_ingestion = await ingestion_repository.get_latest_successful_ingestion(
        session,
        ingestion_repository.IngestionType.INCREMENTAL,
    )

    if last_ingestion and last_ingestion.watermark_dateadded:
        since_dateadded = last_ingestion.watermark_dateadded
    else:
        # No previous incremental - check bootstrap
        bootstrap = await ingestion_repository.get_latest_successful_ingestion(
            session,
            ingestion_repository.IngestionType.BOOTSTRAP,
        )
        if bootstrap and bootstrap.watermark_dateadded:
            since_dateadded = bootstrap.watermark_dateadded
        else:
            # No prior ingestion at all - fall back to 1 hour ago
            one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
            since_dateadded = int(one_hour_ago.strftime("%Y%m%d%H%M%S"))

    now = datetime.now(timezone.utc)

    # Create ingestion run record
    run = await ingestion_repository.create_ingestion_run(
        session,
        ingestion_repository.IngestionType.INCREMENTAL,
    )
    await session.commit()

    total_ingested = 0
    last_watermark = since_dateadded
    batch_size = settings.ingestion_batch_size

    try:
        start_dt = datetime.strptime(str(since_dateadded), "%Y%m%d%H%M%S").replace(
            tzinfo=timezone.utc
        )

        for (
            window_start,
            window_end,
            date_from_sqldate,
            date_to_sqldate,
        ) in _iter_incremental_windows(
            start_dt,
            now,
        ):
            query, params = build_ingestion_incremental_query(
                since_dateadded=window_start,
                window_end_dateadded=window_end,
                date_from_sqldate=date_from_sqldate,
                date_to_sqldate=date_to_sqldate,
                limit=batch_size,
            )

            rows: list[dict[str, Any]] = await bq_client.run_query(query, params)

            if not rows:
                last_watermark = window_end
                continue

            events = [_row_to_event_dict(row) for row in rows]

            inserted = await event_repository.bulk_insert_events(
                session,
                events,
            )
            await session.commit()

            total_ingested += inserted
            last_watermark = max(window_end, events[-1]["date_added"])

            logger.info(
                "incremental_batch_ingested",
                batch_size=len(events),
                inserted=inserted,
                total=total_ingested,
                watermark=last_watermark,
            )

        await ingestion_repository.update_ingestion_run(
            session,
            run.id,
            ingestion_repository.IngestionStatus.COMPLETED,
            watermark_dateadded=last_watermark,
            events_ingested=total_ingested,
        )
        await session.commit()

        logger.info(
            "incremental_completed",
            total_ingested=total_ingested,
            watermark=last_watermark,
        )

        return {
            "status": "completed",
            "events_ingested": total_ingested,
            "watermark": last_watermark,
        }

    except Exception as e:
        logger.error("incremental_failed", error=str(e))
        await ingestion_repository.update_ingestion_run(
            session,
            run.id,
            ingestion_repository.IngestionStatus.FAILED,
            error_message=str(e),
        )
        await session.commit()
        raise


async def run_retention_cleanup(session: AsyncSession) -> dict[str, Any]:
    """
    Run retention cleanup - delete events older than retention_days.
    """
    settings = get_settings()
    logger.info("starting_retention_cleanup", retention_days=settings.retention_days)

    # Calculate cutoff SQLDATE (YYYYMMDD format)
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=settings.retention_days)
    cutoff_sqldate = int(cutoff_date.strftime("%Y%m%d"))

    deleted = await event_repository.delete_events_before(
        session,
        cutoff_sqldate,
    )
    await session.commit()

    logger.info(
        "retention_cleanup_completed",
        deleted=deleted,
        cutoff_sqldate=cutoff_sqldate,
    )

    return {
        "deleted": deleted,
        "cutoff_sqldate": cutoff_sqldate,
    }


async def should_bootstrap_on_startup(session: AsyncSession) -> bool:
    """Return True when startup should run the initial retention-window bootstrap."""
    if await event_repository.get_event_count(session) > 0:
        return False

    return not await ingestion_repository.is_bootstrap_complete(session)


def _row_to_event_dict(row: dict[str, Any]) -> dict[str, Any]:
    """Transform a BigQuery row to an event dict for insertion."""
    return {
        "global_event_id": row["GLOBALEVENTID"],
        "sql_date": row["SQLDATE"],
        "date_added": row["DATEADDED"],
        "actor1_country_code": row.get("Actor1CountryCode"),
        "actor2_country_code": row.get("Actor2CountryCode"),
        "event_code": row.get("EventCode"),
        "event_base_code": row.get("EventBaseCode"),
        "event_root_code": row.get("EventRootCode"),
        "quad_class": row.get("QuadClass"),
        "goldstein_scale": row.get("GoldsteinScale"),
        "avg_tone": row.get("AvgTone"),
        "num_mentions": row.get("NumMentions"),
        "num_sources": row.get("NumSources"),
        "num_articles": row.get("NumArticles"),
        "action_geo_full_name": row.get("ActionGeo_FullName"),
        "action_geo_country_code": row.get("ActionGeo_CountryCode"),
        "source_url": row.get("SOURCEURL"),
    }
