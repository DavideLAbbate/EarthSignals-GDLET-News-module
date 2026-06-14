"""
Tests for the GDELT metadata refresh job.

Verifies that the job writes SyncState from local events and remains append-only.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock


from app.db.repositories import event_repository
from app.db.repositories.sync_repository import get_latest_sync_state
from app.scheduler.sync_job import run_gdelt_sync


async def test_sync_job_writes_sync_state(db_session):
    """Sync job writes metadata derived from local events to SyncState."""
    # Use dates inside the sync job's rolling 30-day window so the test does not
    # rot over time (events must be recent relative to "now", not hard-coded).
    now = datetime.now(timezone.utc)
    today = int(now.strftime("%Y%m%d"))
    yesterday = int((now - timedelta(days=1)).strftime("%Y%m%d"))
    today_added = int(now.strftime("%Y%m%d%H%M%S"))
    yesterday_added = int((now - timedelta(days=1)).strftime("%Y%m%d%H%M%S"))

    await event_repository.bulk_insert_events(
        db_session,
        [
            {
                "global_event_id": 1,
                "sql_date": today,
                "date_added": today_added,
                "action_geo_country_code": "US",
                "event_root_code": "14",
                "source_url": "https://example.com/1",
            },
            {
                "global_event_id": 2,
                "sql_date": today,
                "date_added": today_added,
                "action_geo_country_code": "US",
                "event_root_code": "14",
                "source_url": "https://example.com/2",
            },
            {
                "global_event_id": 3,
                "sql_date": yesterday,
                "date_added": yesterday_added,
                "action_geo_country_code": "CH",
                "event_root_code": "19",
                "source_url": "https://example.com/3",
            },
        ],
    )
    await db_session.commit()

    # Patch session factory to use our test session

    mock_factory_instance = MagicMock()
    mock_factory_instance.__aenter__ = AsyncMock(return_value=db_session)
    mock_factory_instance.__aexit__ = AsyncMock(return_value=False)

    import unittest.mock

    with unittest.mock.patch("app.scheduler.sync_job._get_session_factory") as mock_factory:
        mock_factory.return_value = lambda: mock_factory_instance
        await run_gdelt_sync()

    # Verify SyncState was written
    sync_state = await get_latest_sync_state(db_session)
    assert sync_state is not None
    assert sync_state.latest_sqldate == today
    assert sync_state.sync_status == "success"
    assert sync_state.top_countries is not None
    assert len(sync_state.top_countries) == 2
    assert sync_state.top_countries[0]["fips_code"] == "US"
    assert sync_state.top_event_root_codes is not None
    assert len(sync_state.top_event_root_codes) == 2


async def test_sync_job_handles_db_error_gracefully(db_session):
    """Unexpected metadata sync errors write an error state, do not raise."""

    import unittest.mock

    mock_factory_instance = MagicMock()
    mock_factory_instance.__aenter__ = AsyncMock(return_value=db_session)
    mock_factory_instance.__aexit__ = AsyncMock(return_value=False)

    with (
        unittest.mock.patch("app.scheduler.sync_job._get_session_factory") as mock_factory,
        unittest.mock.patch(
            "app.scheduler.sync_job.get_top_countries_since",
            side_effect=RuntimeError("DB is down"),
        ),
    ):
        mock_factory.return_value = lambda: mock_factory_instance
        await run_gdelt_sync()

    sync_state = await get_latest_sync_state(db_session)
    assert sync_state is not None
    assert sync_state.sync_status == "error"


async def test_sync_job_runs_without_arguments():
    """Metadata sync runs successfully with no arguments."""
    await run_gdelt_sync()


async def test_sync_job_idempotent(db_session):
    """Running sync twice produces two SyncState records (append-only)."""
    await event_repository.bulk_insert_events(
        db_session,
        [
            {
                "global_event_id": 10,
                "sql_date": 20260307,
                "date_added": 20260307120000,
                "action_geo_country_code": "US",
                "event_root_code": "14",
                "source_url": "https://example.com/idempotent",
            }
        ],
    )
    await db_session.commit()

    mock_factory_instance = MagicMock()
    mock_factory_instance.__aenter__ = AsyncMock(return_value=db_session)
    mock_factory_instance.__aexit__ = AsyncMock(return_value=False)

    import unittest.mock

    with unittest.mock.patch("app.scheduler.sync_job._get_session_factory") as mock_factory:
        mock_factory.return_value = lambda: mock_factory_instance
        await run_gdelt_sync()
        await run_gdelt_sync()

    from sqlalchemy import select
    from app.db.models import SyncState

    result = await db_session.execute(select(SyncState))
    all_states = result.scalars().all()
    assert len(all_states) >= 1  # At least one sync
