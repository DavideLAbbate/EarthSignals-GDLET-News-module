"""Tests for FastAPI startup task lifecycle behavior."""

from __future__ import annotations

import asyncio
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_lifespan_schedules_metadata_sync_and_bootstrap_tasks():
    """Application startup should schedule the expected one-off startup tasks."""
    import unittest.mock

    from app.main import lifespan

    app = MagicMock()
    bq_client = MagicMock()
    scheduler = MagicMock(running=True)
    scheduled_tasks = []

    with (
        unittest.mock.patch("app.main.configure_logging"),
        unittest.mock.patch("app.main.get_settings") as get_settings,
        unittest.mock.patch("app.main.create_bigquery_client", return_value=bq_client),
        unittest.mock.patch("app.main.create_anthropic_client", return_value=MagicMock()),
        unittest.mock.patch("app.main.create_scheduler", return_value=scheduler),
        unittest.mock.patch("app.main.add_sync_job"),
        unittest.mock.patch(
            "app.main.trigger_sync_now", new_callable=AsyncMock
        ) as trigger_sync_now,
        unittest.mock.patch(
            "app.main.trigger_startup_ingestion_if_needed",
            new_callable=AsyncMock,
        ) as trigger_startup_ingestion_if_needed,
        unittest.mock.patch("app.main._schedule_startup_task") as schedule_startup_task,
        unittest.mock.patch("app.main._shutdown_startup_tasks", new_callable=AsyncMock),
        unittest.mock.patch("app.main.dispose_engine", new_callable=AsyncMock),
    ):
        get_settings.return_value.log_level = "INFO"
        get_settings.return_value.is_development = False
        get_settings.return_value.app_env = "test"
        schedule_startup_task.side_effect = lambda app, task_name, coroutine: (
            coroutine.close(),
            scheduled_tasks.append(task_name),
        )[1]

        async with lifespan(app):
            pass

    assert scheduled_tasks == ["metadata_sync", "startup_ingestion"]
    trigger_sync_now.assert_called_once_with(bq_client)
    trigger_startup_ingestion_if_needed.assert_called_once_with()


@pytest.mark.asyncio
async def test_lifespan_skips_startup_sync_when_disabled():
    """Application startup should only schedule bootstrap work when sync is disabled."""
    import unittest.mock

    from app.main import lifespan

    app = MagicMock()
    bq_client = MagicMock()
    scheduler = MagicMock(running=True)
    scheduled_tasks = []

    with (
        unittest.mock.patch("app.main.configure_logging"),
        unittest.mock.patch("app.main.get_settings") as get_settings,
        unittest.mock.patch("app.main.create_bigquery_client", return_value=bq_client),
        unittest.mock.patch("app.main.create_anthropic_client", return_value=MagicMock()),
        unittest.mock.patch("app.main.create_scheduler", return_value=scheduler),
        unittest.mock.patch("app.main.add_sync_job"),
        unittest.mock.patch(
            "app.main.trigger_sync_now", new_callable=AsyncMock
        ) as trigger_sync_now,
        unittest.mock.patch(
            "app.main.trigger_startup_ingestion_if_needed",
            new_callable=AsyncMock,
        ) as trigger_startup_ingestion_if_needed,
        unittest.mock.patch("app.main._schedule_startup_task") as schedule_startup_task,
        unittest.mock.patch("app.main._shutdown_startup_tasks", new_callable=AsyncMock),
        unittest.mock.patch("app.main.dispose_engine", new_callable=AsyncMock),
    ):
        get_settings.return_value.log_level = "INFO"
        get_settings.return_value.is_development = False
        get_settings.return_value.app_env = "test"
        get_settings.return_value.enable_metadata_sync = False
        schedule_startup_task.side_effect = lambda app, task_name, coroutine: (
            coroutine.close(),
            scheduled_tasks.append(task_name),
        )[1]

        async with lifespan(app):
            pass

    assert scheduled_tasks == ["startup_ingestion"]
    trigger_sync_now.assert_not_called()
    trigger_startup_ingestion_if_needed.assert_called_once_with()


@pytest.mark.asyncio
async def test_lifespan_does_not_schedule_extra_startup_task_for_event_enrichment():
    """Event enrichment should be scheduler-driven without adding another startup task."""
    import unittest.mock

    from app.main import lifespan

    app = MagicMock()
    bq_client = MagicMock()
    scheduler = MagicMock(running=True)
    scheduled_tasks = []

    with (
        unittest.mock.patch("app.main.configure_logging"),
        unittest.mock.patch("app.main.get_settings") as get_settings,
        unittest.mock.patch("app.main.create_bigquery_client", return_value=bq_client),
        unittest.mock.patch("app.main.create_anthropic_client", return_value=MagicMock()),
        unittest.mock.patch("app.main.create_scheduler", return_value=scheduler),
        unittest.mock.patch("app.main.add_sync_job") as add_sync_job,
        unittest.mock.patch("app.main.trigger_sync_now", new_callable=AsyncMock),
        unittest.mock.patch(
            "app.main.trigger_startup_ingestion_if_needed",
            new_callable=AsyncMock,
        ),
        unittest.mock.patch("app.main._schedule_startup_task") as schedule_startup_task,
        unittest.mock.patch("app.main._shutdown_startup_tasks", new_callable=AsyncMock),
        unittest.mock.patch("app.main.dispose_engine", new_callable=AsyncMock),
    ):
        get_settings.return_value.log_level = "INFO"
        get_settings.return_value.is_development = False
        get_settings.return_value.app_env = "test"
        get_settings.return_value.enable_metadata_sync = True
        get_settings.return_value.enable_event_enrichment = True
        schedule_startup_task.side_effect = lambda app, task_name, coroutine: (
            coroutine.close(),
            scheduled_tasks.append(task_name),
        )[1]

        async with lifespan(app):
            pass

    add_sync_job.assert_called_once_with(scheduler, bq_client)
    assert scheduled_tasks == ["metadata_sync", "startup_ingestion"]


@pytest.mark.asyncio
async def test_lifespan_tracks_and_drains_startup_tasks_before_resource_shutdown():
    """Shutdown should coordinate startup tasks before shared resources are torn down."""
    import unittest.mock

    from app.main import lifespan

    app = MagicMock()
    bq_client = MagicMock()
    scheduler = MagicMock(running=True)
    startup_tasks = [MagicMock(), MagicMock()]
    call_order: list[str] = []

    async def record_shutdown(tasks):
        assert tasks == startup_tasks
        call_order.append("startup_tasks")

    async def record_dispose_engine():
        call_order.append("dispose_engine")

    with (
        unittest.mock.patch("app.main.configure_logging"),
        unittest.mock.patch("app.main.get_settings") as get_settings,
        unittest.mock.patch("app.main.create_bigquery_client", return_value=bq_client),
        unittest.mock.patch("app.main.create_anthropic_client", return_value=MagicMock()),
        unittest.mock.patch("app.main.create_scheduler", return_value=scheduler),
        unittest.mock.patch("app.main.add_sync_job"),
        unittest.mock.patch("app.main._schedule_startup_task") as schedule_startup_task,
        unittest.mock.patch(
            "app.main._shutdown_startup_tasks",
            side_effect=record_shutdown,
            new_callable=AsyncMock,
        ) as shutdown_startup_tasks,
        unittest.mock.patch(
            "app.main.dispose_engine",
            side_effect=record_dispose_engine,
            new_callable=AsyncMock,
        ),
    ):
        get_settings.return_value.log_level = "INFO"
        get_settings.return_value.is_development = False
        get_settings.return_value.app_env = "test"
        schedule_startup_task.side_effect = lambda app, task_name, coroutine: coroutine.close()
        app.state.startup_tasks = startup_tasks
        bq_client.shutdown.side_effect = lambda: call_order.append("bq_shutdown")

        async with lifespan(app):
            app.state.startup_tasks = startup_tasks

    shutdown_startup_tasks.assert_awaited_once_with(startup_tasks)
    scheduler.shutdown.assert_called_once_with(wait=False)
    assert call_order == ["startup_tasks", "bq_shutdown", "dispose_engine"]


@pytest.mark.asyncio
async def test_run_logged_startup_task_logs_failures():
    """Startup task wrapper should log failures instead of leaking unobserved task errors."""
    import unittest.mock

    from app.main import _run_logged_startup_task

    async def failing_task():
        raise RuntimeError("boom")

    with unittest.mock.patch("app.main.logger") as logger:
        await _run_logged_startup_task("metadata_sync", failing_task())

    logger.error.assert_called_once_with(
        "startup_task_failed",
        task_name="metadata_sync",
        error="boom",
    )


@pytest.mark.asyncio
async def test_shutdown_startup_tasks_cancels_pending_work():
    """Pending startup tasks should be cancelled and gathered safely on shutdown."""
    import unittest.mock

    from app.main import _shutdown_startup_tasks

    done_task = MagicMock()
    done_task.done.return_value = True
    pending_task = MagicMock()
    pending_task.done.return_value = False
    startup_tasks = cast(list[asyncio.Task], [done_task, pending_task])

    with (
        unittest.mock.patch("app.main.asyncio.wait", new_callable=AsyncMock) as wait,
        unittest.mock.patch("app.main.asyncio.gather", new_callable=AsyncMock) as gather,
    ):
        wait.return_value = ({done_task}, {pending_task})

        await _shutdown_startup_tasks(startup_tasks, timeout_seconds=0.25)

    wait.assert_awaited_once_with([done_task, pending_task], timeout=0.25)
    pending_task.cancel.assert_called_once_with()
    gather.assert_awaited_once_with(pending_task, return_exceptions=True)
    assert startup_tasks == []
