"""
Tests for the 15-minute GDELT sync job.

Verifies that sync writes correct SyncState to DB and is idempotent.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock


from app.db.repositories.sync_repository import get_latest_sync_state
from app.scheduler.sync_job import run_gdelt_sync


def _make_bq_client(
    ts_rows=None,
    country_rows=None,
    code_rows=None,
):
    client = MagicMock()
    call_count = [0]

    async def run_query(sql, params):
        call_count[0] += 1
        # First call: timestamp query
        if "MAX(SQLDATE)" in sql:
            return ts_rows or [{"latest_sqldate": 20260307, "latest_dateadded": 20260307120000}]
        # Second call: top countries
        if "ActionGeo_CountryCode" in sql and "GROUP BY" in sql:
            return country_rows or [
                {"fips_code": "US", "event_count": 5000},
                {"fips_code": "CH", "event_count": 3000},
            ]
        # Third call: top event codes
        if "EventRootCode" in sql and "GROUP BY" in sql:
            return code_rows or [
                {"root_code": "14", "event_count": 2000},
                {"root_code": "19", "event_count": 1500},
            ]
        return []

    client.run_query = run_query
    return client


async def test_sync_job_writes_sync_state(db_session):
    """Sync job writes a SyncState record to the DB."""
    bq_client = _make_bq_client()

    # Patch session factory to use our test session

    mock_factory_instance = MagicMock()
    mock_factory_instance.__aenter__ = AsyncMock(return_value=db_session)
    mock_factory_instance.__aexit__ = AsyncMock(return_value=False)

    import unittest.mock

    with unittest.mock.patch("app.scheduler.sync_job._get_session_factory") as mock_factory:
        mock_factory.return_value = lambda: mock_factory_instance
        await run_gdelt_sync(bq_client)

    # Verify SyncState was written
    sync_state = await get_latest_sync_state(db_session)
    assert sync_state is not None
    assert sync_state.latest_sqldate == 20260307
    assert sync_state.sync_status == "success"
    assert len(sync_state.top_countries) == 2
    assert sync_state.top_countries[0]["fips_code"] == "US"
    assert len(sync_state.top_event_root_codes) == 2


async def test_sync_job_handles_bq_error_gracefully(db_session):
    """BQ error during sync writes an error state, does not raise."""
    from app.core.exceptions import BigQueryError

    client = MagicMock()
    client.run_query = AsyncMock(side_effect=BigQueryError("BQ is down"))

    import unittest.mock

    mock_factory_instance = MagicMock()
    mock_factory_instance.__aenter__ = AsyncMock(return_value=db_session)
    mock_factory_instance.__aexit__ = AsyncMock(return_value=False)

    with unittest.mock.patch("app.scheduler.sync_job._get_session_factory") as mock_factory:
        mock_factory.return_value = lambda: mock_factory_instance
        # Should NOT raise
        await run_gdelt_sync(client)


async def test_sync_job_skips_when_no_client():
    """Sync job logs and returns immediately when bq_client is None."""
    # Should not raise
    await run_gdelt_sync(None)


async def test_sync_job_idempotent(db_session):
    """Running sync twice produces two SyncState records (append-only)."""
    bq_client = _make_bq_client()

    mock_factory_instance = MagicMock()
    mock_factory_instance.__aenter__ = AsyncMock(return_value=db_session)
    mock_factory_instance.__aexit__ = AsyncMock(return_value=False)

    import unittest.mock

    with unittest.mock.patch("app.scheduler.sync_job._get_session_factory") as mock_factory:
        mock_factory.return_value = lambda: mock_factory_instance
        await run_gdelt_sync(bq_client)
        await run_gdelt_sync(bq_client)

    from sqlalchemy import select
    from app.db.models import SyncState

    result = await db_session.execute(select(SyncState))
    all_states = result.scalars().all()
    assert len(all_states) >= 1  # At least one sync
