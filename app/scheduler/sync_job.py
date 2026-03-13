"""
GDELT metadata refresh job.

Computes metadata from local PostgreSQL event storage:
  1. The latest SQLDATE and DATEADDED (freshness indicator)
  2. Top-20 countries by event count (last 30 days)
  3. Top-20 event root codes by event count (last 30 days)

Writes the resulting snapshot to PostgreSQL SyncState.
The schedule is controlled by SYNC_INTERVAL_MINUTES.
"""

from __future__ import annotations

from datetime import datetime, timezone

from app.core.logging import get_logger
from app.db.repositories.sync_repository import (
    get_latest_event_timestamps,
    get_top_countries_since,
    get_top_event_root_codes_since,
    upsert_sync_state,
)
from app.db.session import _get_session_factory
from app.integrations.country_codes import get_root_code_label

logger = get_logger(__name__)

# Last 30 days in GDELT SQLDATE format
_30_DAYS_SQLDATE_OFFSET = 30 * 100  # Rough approximation: 30 days = ~30 in day field


def _sqldate_30_days_ago() -> int:
    """Return an approximate SQLDATE for 30 days ago (YYYYMMDD integer)."""
    from datetime import timedelta

    d = datetime.now(timezone.utc) - timedelta(days=30)
    return int(d.strftime("%Y%m%d"))


async def run_gdelt_sync() -> None:
    """
    Execute the full GDELT metadata refresh.

    Metadata is derived from the local event store and written atomically.
    """
    mapping_version = datetime.now(timezone.utc).isoformat()
    logger.info("gdelt_sync_start", mapping_version=mapping_version)

    try:
        # ── Compute metadata from local PostgreSQL event storage ───────────
        session_factory = _get_session_factory()
        async with session_factory() as session:
            latest_sqldate, latest_dateadded = await get_latest_event_timestamps(session)

            logger.info(
                "sync_latest_timestamp",
                latest_sqldate=latest_sqldate,
                latest_dateadded=latest_dateadded,
            )

            since_sqldate = _sqldate_30_days_ago()
            top_countries = await get_top_countries_since(session, since_sqldate)
            code_rows = await get_top_event_root_codes_since(session, since_sqldate)
            top_event_root_codes = [
                {
                    "root_code": row.get("root_code", ""),
                    "label": get_root_code_label(str(row.get("root_code", ""))),
                    "event_count": row.get("event_count", 0),
                }
                for row in code_rows
                if row.get("root_code")
            ]

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
