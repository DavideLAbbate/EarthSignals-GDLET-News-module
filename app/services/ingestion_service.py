"""Ingestion service for GDELT event data.

Fetches GDELT export files directly over HTTP (no BigQuery required).
Bootstrap uses masterfilelist.txt to load the initial retention window.
Incremental uses lastupdate.txt to pull the latest 15-minute file.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.logging import get_logger
from app.db.repositories import event_repository, ingestion_repository
from app.db.repositories.gkg_repository import GkgRepository
from app.db.repositories.mentions_repository import MentionsRepository
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
    since_ts = _days_ago_ts(settings.retention_days)
    until_ts = _now_ts()

    return await run_bootstrap_range(session, since_ts=since_ts, until_ts=until_ts)


async def run_bootstrap_range(
    session: AsyncSession, since_ts: int, until_ts: int
) -> dict[str, Any]:
    """Run bootstrap ingestion for an explicit inclusive DATEADDED timestamp range."""
    if since_ts > until_ts:
        raise ValueError("Bootstrap start timestamp must be less than or equal to end timestamp")

    logger.info("starting_bootstrap_ingestion", since_ts=since_ts, until_ts=until_ts)

    run = await ingestion_repository.create_ingestion_run(
        session, ingestion_repository.IngestionType.BOOTSTRAP
    )
    run_id = run.id
    await session.commit()

    total_ingested = 0
    final_watermark = until_ts

    gdelt = _get_gdelt_client()
    mentions_repo = MentionsRepository(session)
    gkg_repo = GkgRepository(session)
    try:
        file_list = await gdelt.fetch_master_export_urls(since_ts=since_ts, until_ts=until_ts)
        requested_files = [
            (url, file_ts) for url, file_ts in file_list if since_ts <= file_ts <= until_ts
        ]
        mentions_index = _index_urls_by_timestamp(
            await gdelt.fetch_master_mentions_urls(since_ts=since_ts, until_ts=until_ts)
        )
        gkg_index = _index_urls_by_timestamp(
            await gdelt.fetch_master_gkg_urls(since_ts=since_ts, until_ts=until_ts)
        )
        total_files = len(requested_files)

        for index, (url, file_ts) in enumerate(requested_files, start=1):
            rows = await gdelt.download_events(url)
            if not rows:
                logger.info(
                    "bootstrap_batch_skipped_empty",
                    file_ts=file_ts,
                    progress=index,
                    total_files=total_files,
                    url=url,
                )
                continue

            events = [_row_to_event_dict(row) for row in rows]
            inserted = await event_repository.bulk_insert_events(session, events)
            await session.commit()

            mentions_inserted = await _ingest_mentions_batch(
                gdelt,
                mentions_repo,
                session,
                file_ts,
                mentions_urls=mentions_index.get(file_ts, []),
            )
            gkg_inserted = await _ingest_gkg_batch(
                gdelt,
                gkg_repo,
                session,
                file_ts,
                gkg_urls=gkg_index.get(file_ts, []),
            )

            total_ingested += inserted
            logger.info(
                "bootstrap_batch_ingested",
                event_rows=len(events),
                file_ts=file_ts,
                gkg_inserted=gkg_inserted,
                mentions_inserted=mentions_inserted,
                progress=index,
                total_files=total_files,
                url=url,
                batch_size=len(events),
                inserted=inserted,
                total=total_ingested,
            )

        await ingestion_repository.update_ingestion_run(
            session,
            run_id,
            ingestion_repository.IngestionStatus.COMPLETED,
            watermark_dateadded=final_watermark,
            events_ingested=total_ingested,
        )
        await session.commit()

        logger.info("bootstrap_completed", total_ingested=total_ingested, watermark=final_watermark)
        return {
            "status": "completed",
            "events_ingested": total_ingested,
            "watermark": final_watermark,
        }

    except Exception as e:
        logger.error("bootstrap_failed", error=str(e))
        await session.rollback()
        await ingestion_repository.update_ingestion_run(
            session,
            run_id,
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
    run_id = run.id
    await session.commit()

    total_ingested = 0
    last_watermark = watermark

    gdelt = _get_gdelt_client()
    mentions_repo = MentionsRepository(session)
    gkg_repo = GkgRepository(session)
    try:
        file_list = await gdelt.fetch_master_export_urls(since_ts=watermark + 1, until_ts=_now_ts())

        if not file_list:
            logger.info("incremental_already_up_to_date", watermark=watermark)
        else:
            for url, file_ts in file_list:
                rows = await gdelt.download_events(url)
                if rows:
                    events = [_row_to_event_dict(row) for row in rows]
                    inserted = await event_repository.bulk_insert_events(session, events)
                    await session.commit()

                    await _ingest_mentions_batch(gdelt, mentions_repo, session, file_ts)
                    await _ingest_gkg_batch(gdelt, gkg_repo, session, file_ts)

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
            run_id,
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
            run_id,
            ingestion_repository.IngestionStatus.FAILED,
            error_message=str(e),
        )
        await session.commit()
        raise
    finally:
        await gdelt.close()


async def run_retention_cleanup(session: AsyncSession) -> dict[str, Any]:
    """Delete events, mentions, and GKG rows older than retention_days from local storage."""
    settings = get_settings()
    logger.info("starting_retention_cleanup", retention_days=settings.retention_days)

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=settings.retention_days)
    cutoff_sqldate = int(cutoff_date.strftime("%Y%m%d"))
    cutoff_dateadded = int(cutoff_date.strftime("%Y%m%d%H%M%S"))

    deleted_events = await event_repository.delete_events_before(session, cutoff_sqldate)
    await session.commit()

    mentions_repo = MentionsRepository(session)
    deleted_mentions = await mentions_repo.delete_before_dateadded(cutoff_dateadded)
    await session.commit()

    gkg_repo = GkgRepository(session)
    deleted_gkg = await gkg_repo.delete_before_date(cutoff_dateadded)
    await session.commit()

    logger.info(
        "retention_cleanup_completed",
        deleted_events=deleted_events,
        deleted_mentions=deleted_mentions,
        deleted_gkg=deleted_gkg,
        cutoff_sqldate=cutoff_sqldate,
    )
    return {
        "deleted_events": deleted_events,
        "deleted_mentions": deleted_mentions,
        "deleted_gkg": deleted_gkg,
        "cutoff_sqldate": cutoff_sqldate,
    }


async def should_bootstrap_on_startup(session: AsyncSession) -> bool:
    """Return True when startup should run the initial retention-window bootstrap."""
    if await event_repository.get_event_count(session) > 0:
        return False
    return not await ingestion_repository.is_bootstrap_complete(session)


async def should_catchup_on_startup(session: AsyncSession) -> bool:
    """Return True when the last watermark is far enough behind now to warrant an immediate
    incremental catch-up run on startup rather than waiting for the first scheduled tick.

    Threshold is 4 hours — large enough to skip normal restarts but catches multi-day gaps
    that occur when the service has been offline for an extended period.
    """
    last = await ingestion_repository.get_latest_successful_ingestion(
        session, ingestion_repository.IngestionType.INCREMENTAL
    )
    if last is None:
        last = await ingestion_repository.get_latest_successful_ingestion(
            session, ingestion_repository.IngestionType.BOOTSTRAP
        )
    if last is None or last.watermark_dateadded is None:
        return False
    watermark_dt = datetime.strptime(str(last.watermark_dateadded), "%Y%m%d%H%M%S").replace(
        tzinfo=timezone.utc
    )
    gap = datetime.now(timezone.utc) - watermark_dt
    return gap.total_seconds() > 4 * 3600


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


def _row_to_mention_dict(row: dict[str, Any]) -> dict[str, Any]:
    """Transform an EVENTMENTIONS row dict to a DB mention dict."""
    return {
        "global_event_id": row.get("GLOBALEVENTID"),
        "event_time_date": row.get("EventTimeDate"),
        "mention_time_date": row.get("MentionTimeDate"),
        "mention_type": row.get("MentionType"),
        "mention_source_name": row.get("MentionSourceName"),
        "mention_identifier": row.get("MentionIdentifier"),
        "mention_doc_len": row.get("MentionDocLen"),
        "mention_doc_tone": row.get("MentionDocTone"),
    }


def _row_to_gkg_dict(row: dict[str, Any]) -> dict[str, Any]:
    """Transform a GKG row dict to a DB GKG dict."""
    return {
        "gkg_record_id": row.get("GKGRECORDID"),
        "date": row.get("DATE"),
        "source_common_name": row.get("SourceCommonName"),
        "document_identifier": row.get("DocumentIdentifier"),
        "themes": row.get("V1Themes") or [],
        "persons": row.get("V1Persons") or [],
        "organizations": row.get("V1Organizations") or [],
        "locations": row.get("V1Locations") or [],
        "document_tone": row.get("AvgTone"),
    }


def _index_urls_by_timestamp(file_list: list[tuple[str, int]]) -> dict[int, list[str]]:
    """Group GDELT file URLs by timestamp for O(1) per-batch sidecar lookup."""
    indexed_urls: dict[int, list[str]] = defaultdict(list)
    for url, file_ts in file_list:
        indexed_urls[file_ts].append(url)
    return dict(indexed_urls)


async def _ingest_mentions_batch(
    gdelt: GdeltHttpClient,
    mentions_repo: MentionsRepository,
    session: AsyncSession,
    file_ts: int,
    *,
    mentions_urls: list[str] | None = None,
) -> int:
    """Best-effort ingest of the batch-aligned EVENTMENTIONS file during bootstrap."""
    inserted_total = 0
    try:
        if mentions_urls is None:
            mentions_urls = [
                mentions_url
                for mentions_url, _ in await gdelt.fetch_master_mentions_urls(
                    since_ts=file_ts,
                    until_ts=file_ts,
                )
            ]
        for mentions_url in mentions_urls:
            mentions_rows = await gdelt.download_mentions(mentions_url)
            mapped = [_row_to_mention_dict(row) for row in mentions_rows if row]
            if mapped:
                inserted_total += await mentions_repo.bulk_upsert(mapped)
                await session.commit()
    except Exception as exc:
        await session.rollback()
        logger.error("mentions_ingestion_error", error=str(exc), file_ts=file_ts)
    return inserted_total


async def _ingest_gkg_batch(
    gdelt: GdeltHttpClient,
    gkg_repo: GkgRepository,
    session: AsyncSession,
    file_ts: int,
    *,
    gkg_urls: list[str] | None = None,
) -> int:
    """Best-effort ingest of the batch-aligned GKG file during bootstrap."""
    inserted_total = 0
    try:
        if gkg_urls is None:
            gkg_urls = [
                gkg_url
                for gkg_url, _ in await gdelt.fetch_master_gkg_urls(
                    since_ts=file_ts,
                    until_ts=file_ts,
                )
            ]
        for gkg_url in gkg_urls:
            gkg_rows = await gdelt.download_gkg(gkg_url)
            mapped = [_row_to_gkg_dict(row) for row in gkg_rows if row]
            if mapped:
                inserted_total += await gkg_repo.bulk_upsert(mapped)
                await session.commit()
    except Exception as exc:
        await session.rollback()
        logger.error("gkg_ingestion_error", error=str(exc), file_ts=file_ts)
    return inserted_total
