"""Ingestion service for GDELT event data.

Fetches GDELT export files directly over HTTP (no BigQuery required).
Bootstrap uses masterfilelist.txt to load the initial retention window.
Incremental uses lastupdate.txt to pull the latest 15-minute file.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.logging import get_logger
from app.db.repositories import event_repository, ingestion_repository
from app.integrations.gdelt_http_client import GdeltHttpClient

logger = get_logger(__name__)


def _get_gdelt_client() -> GdeltHttpClient:
    """Create a production GdeltHttpClient. Extracted for easy mocking in tests."""
    return GdeltHttpClient.create()


def _now_ts() -> int:
    """Return current UTC time as DATEADDED-format int (YYYYMMDDHHMMSS)."""
    return int(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"))


def _days_ago_ts(days: int) -> int:
    """Return (now - days) as DATEADDED-format int."""
    dt = datetime.now(timezone.utc) - timedelta(days=days)
    return int(dt.strftime("%Y%m%d%H%M%S"))


async def run_bootstrap(session: AsyncSession) -> dict[str, Any]:
    """
    Run bootstrap ingestion - fetch the initial rolling retention window.

    Downloads all export files from masterfilelist.txt within [now - retention_days, now].
    """
    settings = get_settings()
    logger.info("starting_bootstrap_ingestion")

    run = await ingestion_repository.create_ingestion_run(
        session, ingestion_repository.IngestionType.BOOTSTRAP
    )
    await session.commit()

    since_ts = _days_ago_ts(settings.retention_days)
    until_ts = _now_ts()
    total_ingested = 0
    last_watermark = until_ts

    gdelt = _get_gdelt_client()
    try:
        file_list = await gdelt.fetch_master_export_urls(since_ts=since_ts, until_ts=until_ts)

        for url, file_ts in file_list:
            rows = await gdelt.download_events(url)
            if not rows:
                last_watermark = file_ts
                continue

            events = [_row_to_event_dict(row) for row in rows]
            inserted = await event_repository.bulk_insert_events(session, events)
            await session.commit()

            total_ingested += inserted
            last_watermark = file_ts
            logger.info(
                "bootstrap_batch_ingested",
                url=url,
                batch_size=len(events),
                inserted=inserted,
                total=total_ingested,
            )

        await ingestion_repository.update_ingestion_run(
            session,
            run.id,
            ingestion_repository.IngestionStatus.COMPLETED,
            watermark_dateadded=last_watermark,
            events_ingested=total_ingested,
        )
        await session.commit()

        logger.info("bootstrap_completed", total_ingested=total_ingested, watermark=last_watermark)
        return {
            "status": "completed",
            "events_ingested": total_ingested,
            "watermark": last_watermark,
        }

    except Exception as e:
        logger.error("bootstrap_failed", error=str(e))
        await session.rollback()
        await ingestion_repository.update_ingestion_run(
            session,
            run.id,
            ingestion_repository.IngestionStatus.FAILED,
            error_message=str(e),
        )
        await session.commit()
        raise
    finally:
        await gdelt.close()


async def run_incremental(session: AsyncSession) -> dict[str, Any]:
    """
    Run incremental ingestion - download latest export file if newer than watermark.
    """
    logger.info("starting_incremental_ingestion")

    last_ingestion = await ingestion_repository.get_latest_successful_ingestion(
        session, ingestion_repository.IngestionType.INCREMENTAL
    )
    if last_ingestion and last_ingestion.watermark_dateadded:
        watermark = last_ingestion.watermark_dateadded
    else:
        bootstrap = await ingestion_repository.get_latest_successful_ingestion(
            session, ingestion_repository.IngestionType.BOOTSTRAP
        )
        if bootstrap and bootstrap.watermark_dateadded:
            watermark = bootstrap.watermark_dateadded
        else:
            one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
            watermark = int(one_hour_ago.strftime("%Y%m%d%H%M%S"))

    run = await ingestion_repository.create_ingestion_run(
        session, ingestion_repository.IngestionType.INCREMENTAL
    )
    await session.commit()

    total_ingested = 0
    last_watermark = watermark

    gdelt = _get_gdelt_client()
    try:
        url, file_ts = await gdelt.fetch_latest_export_url()

        if file_ts <= watermark:
            logger.info("incremental_already_up_to_date", file_ts=file_ts, watermark=watermark)
        else:
            rows = await gdelt.download_events(url)
            if rows:
                events = [_row_to_event_dict(row) for row in rows]
                inserted = await event_repository.bulk_insert_events(session, events)
                await session.commit()
                total_ingested += inserted
                last_watermark = file_ts
                logger.info(
                    "incremental_batch_ingested",
                    url=url,
                    batch_size=len(events),
                    inserted=inserted,
                    watermark=last_watermark,
                )
            else:
                last_watermark = file_ts

        await ingestion_repository.update_ingestion_run(
            session,
            run.id,
            ingestion_repository.IngestionStatus.COMPLETED,
            watermark_dateadded=last_watermark,
            events_ingested=total_ingested,
        )
        await session.commit()

        logger.info(
            "incremental_completed", total_ingested=total_ingested, watermark=last_watermark
        )
        return {
            "status": "completed",
            "events_ingested": total_ingested,
            "watermark": last_watermark,
        }

    except Exception as e:
        logger.error("incremental_failed", error=str(e))
        await session.rollback()
        await ingestion_repository.update_ingestion_run(
            session,
            run.id,
            ingestion_repository.IngestionStatus.FAILED,
            error_message=str(e),
        )
        await session.commit()
        raise
    finally:
        await gdelt.close()


async def run_retention_cleanup(session: AsyncSession) -> dict[str, Any]:
    """Delete events older than retention_days from local storage."""
    settings = get_settings()
    logger.info("starting_retention_cleanup", retention_days=settings.retention_days)

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=settings.retention_days)
    cutoff_sqldate = int(cutoff_date.strftime("%Y%m%d"))

    deleted = await event_repository.delete_events_before(session, cutoff_sqldate)
    await session.commit()

    logger.info("retention_cleanup_completed", deleted=deleted, cutoff_sqldate=cutoff_sqldate)
    return {"deleted": deleted, "cutoff_sqldate": cutoff_sqldate}


async def should_bootstrap_on_startup(session: AsyncSession) -> bool:
    """Return True when startup should run the initial retention-window bootstrap."""
    if await event_repository.get_event_count(session) > 0:
        return False
    return not await ingestion_repository.is_bootstrap_complete(session)


def _row_to_event_dict(row: dict[str, Any]) -> dict[str, Any]:
    """Transform a GDELT row dict to a DB event dict."""
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
