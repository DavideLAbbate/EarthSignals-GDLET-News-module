"""Tests for scheduler startup ingestion behavior."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_trigger_startup_ingestion_runs_bootstrap_when_needed(db_session):
    """Startup ingestion should run bootstrap when local storage is empty."""
    from app.scheduler import scheduler

    mock_factory_instance = MagicMock()
    mock_factory_instance.__aenter__ = AsyncMock(return_value=db_session)
    mock_factory_instance.__aexit__ = AsyncMock(return_value=False)

    import unittest.mock

    with (
        unittest.mock.patch("app.scheduler.scheduler._get_session_factory") as mock_factory,
        unittest.mock.patch(
            "app.scheduler.scheduler.run_bootstrap", new_callable=AsyncMock
        ) as run_bootstrap,
    ):
        mock_factory.return_value = lambda: mock_factory_instance

        await scheduler.trigger_startup_ingestion_if_needed(MagicMock())

    run_bootstrap.assert_awaited_once()


@pytest.mark.asyncio
async def test_trigger_startup_ingestion_skips_bootstrap_when_events_exist(db_session):
    """Startup ingestion should not rerun bootstrap after events are already stored."""
    from app.db.repositories import event_repository
    from app.scheduler import scheduler

    await event_repository.bulk_insert_events(
        db_session,
        [
            {
                "global_event_id": 1234567800000,
                "sql_date": 20260301,
                "date_added": 20260301000000,
                "actor1_country_code": "USA",
                "source_url": "https://example.com/already-there",
            }
        ],
    )
    await db_session.commit()

    mock_factory_instance = MagicMock()
    mock_factory_instance.__aenter__ = AsyncMock(return_value=db_session)
    mock_factory_instance.__aexit__ = AsyncMock(return_value=False)

    import unittest.mock

    with (
        unittest.mock.patch("app.scheduler.scheduler._get_session_factory") as mock_factory,
        unittest.mock.patch(
            "app.scheduler.scheduler.run_bootstrap", new_callable=AsyncMock
        ) as run_bootstrap,
    ):
        mock_factory.return_value = lambda: mock_factory_instance

        await scheduler.trigger_startup_ingestion_if_needed(MagicMock())

    run_bootstrap.assert_not_awaited()
