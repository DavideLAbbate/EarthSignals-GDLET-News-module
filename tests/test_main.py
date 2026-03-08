"""Tests for FastAPI startup behavior."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_lifespan_schedules_startup_bootstrap_check():
    """Application startup should schedule the one-time bootstrap check task."""
    import unittest.mock

    from app.main import lifespan

    app = MagicMock()
    bq_client = MagicMock()
    scheduler = MagicMock(running=True)

    with (
        unittest.mock.patch("app.main.configure_logging"),
        unittest.mock.patch("app.main.get_settings") as get_settings,
        unittest.mock.patch("app.main.create_bigquery_client", return_value=bq_client),
        unittest.mock.patch("app.main.create_anthropic_client", return_value=MagicMock()),
        unittest.mock.patch("app.main.create_scheduler", return_value=scheduler),
        unittest.mock.patch("app.main.add_sync_job"),
        unittest.mock.patch("app.main.trigger_sync_now", new_callable=AsyncMock),
        unittest.mock.patch(
            "app.main.trigger_startup_ingestion_if_needed",
            new_callable=AsyncMock,
        ),
        unittest.mock.patch("app.main.asyncio.create_task") as create_task,
        unittest.mock.patch("app.main.dispose_engine", new_callable=AsyncMock),
    ):
        get_settings.return_value.log_level = "INFO"
        get_settings.return_value.is_development = False
        get_settings.return_value.app_env = "test"
        create_task.side_effect = lambda coro: coro.close()

        async with lifespan(app):
            pass

    assert create_task.call_count == 2


@pytest.mark.asyncio
async def test_lifespan_skips_startup_sync_when_disabled():
    """Application startup should not schedule metadata sync when disabled by config."""
    import unittest.mock

    from app.main import lifespan

    app = MagicMock()
    bq_client = MagicMock()
    scheduler = MagicMock(running=True)

    with (
        unittest.mock.patch("app.main.configure_logging"),
        unittest.mock.patch("app.main.get_settings") as get_settings,
        unittest.mock.patch("app.main.create_bigquery_client", return_value=bq_client),
        unittest.mock.patch("app.main.create_anthropic_client", return_value=MagicMock()),
        unittest.mock.patch("app.main.create_scheduler", return_value=scheduler),
        unittest.mock.patch("app.main.add_sync_job"),
        unittest.mock.patch("app.main.trigger_sync_now", new_callable=AsyncMock),
        unittest.mock.patch(
            "app.main.trigger_startup_ingestion_if_needed",
            new_callable=AsyncMock,
        ),
        unittest.mock.patch("app.main.asyncio.create_task") as create_task,
        unittest.mock.patch("app.main.dispose_engine", new_callable=AsyncMock),
    ):
        get_settings.return_value.log_level = "INFO"
        get_settings.return_value.is_development = False
        get_settings.return_value.app_env = "test"
        get_settings.return_value.enable_metadata_sync = False
        create_task.side_effect = lambda coro: coro.close()

        async with lifespan(app):
            pass

    assert create_task.call_count == 1
