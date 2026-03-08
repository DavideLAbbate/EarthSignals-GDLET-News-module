"""
15-minute GDELT metadata sync job.

Queries BigQuery for:
  1. The latest SQLDATE and DATEADDED (freshness indicator)
  2. Top-20 countries by event count (last 30 days)
  3. Top-20 event root codes by event count (last 30 days)

Writes results to PostgreSQL SyncState atomically.
This runs as an APScheduler job every SYNC_INTERVAL_MINUTES.
"""

from __future__ import annotations

from datetime import datetime, timezone

from app.core.exceptions import BigQueryError
from app.core.logging import get_logger
from app.db.repositories.sync_repository import upsert_sync_state
from app.db.session import _get_session_factory
from app.integrations.bigquery_client import BigQueryClientWrapper
from app.integrations.country_codes import get_root_code_label
from app.integrations.gdelt_query_builder import (
    build_sync_latest_timestamp_query,
    build_sync_top_countries_query,
    build_sync_top_event_codes_query,
)

logger = get_logger(__name__)

# Last 30 days in GDELT SQLDATE format
_30_DAYS_SQLDATE_OFFSET = 30 * 100  # Rough approximation: 30 days = ~30 in day field


def _sqldate_30_days_ago() -> int:
    """Return an approximate SQLDATE for 30 days ago (YYYYMMDD integer)."""
    from datetime import timedelta

    d = datetime.now(timezone.utc) - timedelta(days=30)
    return int(d.strftime("%Y%m%d"))


async def run_gdelt_sync(bq_client: BigQueryClientWrapper) -> None:
    """
    Execute the full GDELT metadata sync.

    Guard clause: if bq_client is None, log and return (don't crash scheduler).
    All BQ queries run via the async executor wrapper (non-blocking).
    DB write is atomic (single session commit).
    """
    if bq_client is None:
        logger.error("sync_job_skipped_no_bq_client")
        return

    mapping_version = datetime.now(timezone.utc).isoformat()
    logger.info("gdelt_sync_start", mapping_version=mapping_version)

    try:
        # ── 1. Latest timestamp ────────────────────────────────────────────
        ts_sql, ts_params = build_sync_latest_timestamp_query()
        ts_rows = await bq_client.run_query(ts_sql, ts_params)

        latest_sqldate = None
        latest_dateadded = None
        if ts_rows:
            latest_sqldate = ts_rows[0].get("latest_sqldate")
            latest_dateadded = ts_rows[0].get("latest_dateadded")

        logger.info(
            "sync_latest_timestamp",
            latest_sqldate=latest_sqldate,
            latest_dateadded=latest_dateadded,
        )

        # ── 2. Top countries ───────────────────────────────────────────────
        since_sqldate = _sqldate_30_days_ago()
        country_sql, country_params = build_sync_top_countries_query(since_sqldate)
        country_rows = await bq_client.run_query(country_sql, country_params)

        top_countries = [
            {
                "fips_code": row.get("fips_code", ""),
                "event_count": row.get("event_count", 0),
            }
            for row in country_rows
            if row.get("fips_code")
        ]

        # ── 3. Top event root codes ────────────────────────────────────────
        code_sql, code_params = build_sync_top_event_codes_query(since_sqldate)
        code_rows = await bq_client.run_query(code_sql, code_params)

        top_event_root_codes = [
            {
                "root_code": row.get("root_code", ""),
                "label": get_root_code_label(row.get("root_code", "")),
                "event_count": row.get("event_count", 0),
            }
            for row in code_rows
            if row.get("root_code")
        ]

        # ── 4. Persist to PostgreSQL ───────────────────────────────────────
        session_factory = _get_session_factory()
        async with session_factory() as session:
            await upsert_sync_state(
                session,
                latest_sqldate=latest_sqldate,
                latest_dateadded=latest_dateadded,
                top_countries=top_countries,
                top_event_root_codes=top_event_root_codes,
                mapping_version=mapping_version,
                sync_status="success",
            )
            await session.commit()

        logger.info(
            "gdelt_sync_complete",
            mapping_version=mapping_version,
            top_countries_count=len(top_countries),
            top_codes_count=len(top_event_root_codes),
        )

    except BigQueryError as exc:
        logger.error("gdelt_sync_bigquery_error", error=str(exc))
        await _persist_sync_error(mapping_version, str(exc))

    except Exception as exc:
        logger.error("gdelt_sync_unexpected_error", error=str(exc))
        await _persist_sync_error(mapping_version, str(exc))


async def _persist_sync_error(mapping_version: str, error_message: str) -> None:
    """Write a failed sync state record to the DB (best-effort, non-throwing)."""
    try:
        session_factory = _get_session_factory()
        async with session_factory() as session:
            await upsert_sync_state(
                session,
                latest_sqldate=None,
                latest_dateadded=None,
                top_countries=[],
                top_event_root_codes=[],
                mapping_version=mapping_version,
                sync_status="error",
                error_message=error_message,
            )
            await session.commit()
    except Exception as db_exc:
        logger.error("sync_error_persist_failed", error=str(db_exc))
