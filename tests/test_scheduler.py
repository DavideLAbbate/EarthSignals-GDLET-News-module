"""Tests for scheduler startup and runtime behavior."""

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

        await scheduler.trigger_startup_ingestion_if_needed()

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

        await scheduler.trigger_startup_ingestion_if_needed()

    run_bootstrap.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_event_enrichment_job_uses_session_and_service_batch_size():
    """The scheduled enrichment job should open a DB session and call the real service."""
    from app.scheduler import scheduler

    session = MagicMock()
    context_manager = MagicMock()
    context_manager.__aenter__ = AsyncMock(return_value=session)
    context_manager.__aexit__ = AsyncMock(return_value=False)
    session_factory = MagicMock(return_value=context_manager)

    import unittest.mock

    with (
        unittest.mock.patch("app.scheduler.scheduler.get_settings") as get_settings,
        unittest.mock.patch(
            "app.scheduler.scheduler.run_event_enrichment_batch",
            new_callable=AsyncMock,
        ) as run_batch,
    ):
        get_settings.return_value.event_enrichment_batch_size = 250
        run_batch.return_value = {"selected": 3, "enriched": 2, "failed": 1, "skipped": 0}

        summary = await scheduler.run_event_enrichment_job(session_factory)

    assert summary == {"selected": 3, "enriched": 2, "failed": 1, "skipped": 0}
    session_factory.assert_called_once_with()
    run_batch.assert_awaited_once_with(session, batch_size=250)


@pytest.mark.asyncio
async def test_run_ingestion_job_rejects_invalid_job_type():
    """Invalid ingestion job types should fail fast instead of silently running incremental."""
    from app.scheduler import scheduler

    session = MagicMock()
    context_manager = MagicMock()
    context_manager.__aenter__ = AsyncMock(return_value=session)
    context_manager.__aexit__ = AsyncMock(return_value=False)
    session_factory = MagicMock(return_value=context_manager)

    with pytest.raises(ValueError, match="invalid ingestion job_type: unexpected"):
        await scheduler.run_ingestion_job(session_factory, "unexpected")
