"""Tests for scheduler configuration flags."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from app.core.config import Settings


def _base_settings_overrides() -> dict[str, Any]:
    return {
        "google_application_credentials": "test-service-account.json",
        "gcp_project_id": "test-project",
        "anthropic_api_key": "test-anthropic-key",
        "database_url": "sqlite+aiosqlite:///:memory:",
        "api_key": "test-api-key",
    }


def test_settings_use_event_enrichment_defaults():
    """Event enrichment settings should have the approved defaults."""
    settings = Settings.model_validate(_base_settings_overrides())

    assert settings.enable_event_enrichment is False
    assert settings.event_enrichment_interval_minutes == 30
    assert settings.event_enrichment_batch_size == 100
    assert str(settings.event_enrichment_service_base_url) == "http://localhost:8001/"
    assert settings.event_enrichment_service_timeout_seconds == 10.0
    assert settings.enable_cluster_materialisation is True
    assert settings.cluster_interval_minutes == 1440


@pytest.mark.parametrize(
    "invalid_base_url",
    ["foo", "localhost:8001"],
)
def test_settings_reject_invalid_event_enrichment_service_base_url(invalid_base_url: str):
    """The enrichment service base URL should require an explicit HTTP(S) URL."""
    with pytest.raises(ValidationError):
        Settings.model_validate(
            {
                **_base_settings_overrides(),
                "event_enrichment_service_base_url": invalid_base_url,
            }
        )


def test_settings_accept_explicit_event_enrichment_overrides():
    """Event enrichment overrides should be parsed into strongly typed settings."""
    settings = Settings.model_validate(
        {
            **_base_settings_overrides(),
            "enable_event_enrichment": False,
            "event_enrichment_interval_minutes": 45,
            "event_enrichment_batch_size": 250,
            "event_enrichment_service_base_url": "https://enrichment.internal:8443",
            "event_enrichment_service_timeout_seconds": 22.5,
        }
    )

    assert settings.enable_event_enrichment is False
    assert settings.event_enrichment_interval_minutes == 45
    assert settings.event_enrichment_batch_size == 250
    assert str(settings.event_enrichment_service_base_url) == "https://enrichment.internal:8443/"
    assert settings.event_enrichment_service_timeout_seconds == 22.5


def test_settings_accept_explicit_cluster_materialisation_overrides():
    """Cluster materialisation settings should be configurable."""
    settings = Settings.model_validate(
        {
            **_base_settings_overrides(),
            "enable_cluster_materialisation": False,
            "cluster_interval_minutes": 120,
        }
    )

    assert settings.enable_cluster_materialisation is False
    assert settings.cluster_interval_minutes == 120


def test_add_sync_job_skips_metadata_sync_when_disabled():
    """Disabling metadata sync should avoid registering the gdelt sync job."""
    from app.scheduler import scheduler

    mock_scheduler = MagicMock()

    with (
        patch.object(scheduler, "get_settings") as mock_settings,
        patch.object(scheduler, "_get_session_factory", return_value=MagicMock()),
    ):
        mock_settings.return_value.enable_metadata_sync = False
        mock_settings.return_value.sync_interval_minutes = 15
        mock_settings.return_value.ingestion_interval_minutes = 60
        mock_settings.return_value.enable_event_enrichment = False

        scheduler.add_sync_job(mock_scheduler, MagicMock())

    job_calls = {
        call.kwargs.get("id"): call.kwargs for call in mock_scheduler.add_job.call_args_list
    }
    job_ids = list(job_calls)
    assert "gdelt_sync" not in job_ids
    assert "gdelt_incremental_ingestion" in job_ids
    assert "gdelt_retention_cleanup" in job_ids
    assert job_calls["gdelt_incremental_ingestion"]["max_instances"] == 1
    assert job_calls["gdelt_retention_cleanup"]["max_instances"] == 1


def test_add_sync_job_registers_event_enrichment_when_enabled():
    """Enabling event enrichment should register the enrichment scheduler job."""
    from app.scheduler import scheduler

    mock_scheduler = MagicMock()

    with (
        patch.object(scheduler, "get_settings") as mock_settings,
        patch.object(scheduler, "_get_session_factory", return_value=MagicMock()),
    ):
        mock_settings.return_value.enable_metadata_sync = True
        mock_settings.return_value.sync_interval_minutes = 15
        mock_settings.return_value.ingestion_interval_minutes = 60
        mock_settings.return_value.enable_event_enrichment = True
        mock_settings.return_value.event_enrichment_interval_minutes = 45

        scheduler.add_sync_job(mock_scheduler, MagicMock())

    enrichment_calls = [
        call
        for call in mock_scheduler.add_job.call_args_list
        if call.kwargs.get("id") == "gdelt_event_enrichment"
    ]
    assert len(enrichment_calls) == 1
    assert enrichment_calls[0].kwargs["minutes"] == 45
    assert enrichment_calls[0].kwargs["max_instances"] == 1
    assert enrichment_calls[0].kwargs["replace_existing"] is True

    job_calls = {
        call.kwargs.get("id"): call.kwargs for call in mock_scheduler.add_job.call_args_list
    }
    assert job_calls["gdelt_incremental_ingestion"]["max_instances"] == 1
    assert job_calls["gdelt_retention_cleanup"]["max_instances"] == 1


def test_add_sync_job_skips_event_enrichment_when_disabled():
    """Disabling event enrichment should avoid registering the enrichment job."""
    from app.scheduler import scheduler

    mock_scheduler = MagicMock()

    with (
        patch.object(scheduler, "get_settings") as mock_settings,
        patch.object(scheduler, "_get_session_factory", return_value=MagicMock()),
    ):
        mock_settings.return_value.enable_metadata_sync = True
        mock_settings.return_value.sync_interval_minutes = 15
        mock_settings.return_value.ingestion_interval_minutes = 60
        mock_settings.return_value.enable_event_enrichment = False
        mock_settings.return_value.event_enrichment_interval_minutes = 30

        scheduler.add_sync_job(mock_scheduler, MagicMock())

    job_ids = [call.kwargs.get("id") for call in mock_scheduler.add_job.call_args_list]
    assert "gdelt_event_enrichment" not in job_ids


def test_add_sync_job_registers_cluster_materialisation_when_enabled():
    """Enabling cluster materialisation should register the cluster job."""
    from app.scheduler import scheduler

    mock_scheduler = MagicMock()

    with (
        patch.object(scheduler, "get_settings") as mock_settings,
        patch.object(scheduler, "_get_session_factory", return_value=MagicMock()),
    ):
        mock_settings.return_value.enable_metadata_sync = True
        mock_settings.return_value.sync_interval_minutes = 15
        mock_settings.return_value.ingestion_interval_minutes = 60
        mock_settings.return_value.enable_event_enrichment = False
        mock_settings.return_value.enable_cluster_materialisation = True
        mock_settings.return_value.cluster_interval_minutes = 90

        scheduler.add_sync_job(mock_scheduler, MagicMock())

    cluster_calls = [
        call
        for call in mock_scheduler.add_job.call_args_list
        if call.kwargs.get("id") == "gdelt_cluster_materialisation"
    ]
    assert len(cluster_calls) == 1
    assert cluster_calls[0].kwargs["minutes"] == 90
    assert cluster_calls[0].kwargs["max_instances"] == 1
    assert cluster_calls[0].kwargs["replace_existing"] is True


def test_add_sync_job_skips_cluster_materialisation_when_disabled():
    """Disabling cluster materialisation should avoid registering the cluster job."""
    from app.scheduler import scheduler

    mock_scheduler = MagicMock()

    with (
        patch.object(scheduler, "get_settings") as mock_settings,
        patch.object(scheduler, "_get_session_factory", return_value=MagicMock()),
    ):
        mock_settings.return_value.enable_metadata_sync = True
        mock_settings.return_value.sync_interval_minutes = 15
        mock_settings.return_value.ingestion_interval_minutes = 60
        mock_settings.return_value.enable_event_enrichment = False
        mock_settings.return_value.enable_cluster_materialisation = False
        mock_settings.return_value.cluster_interval_minutes = 60

        scheduler.add_sync_job(mock_scheduler, MagicMock())

    job_ids = [call.kwargs.get("id") for call in mock_scheduler.add_job.call_args_list]
    assert "gdelt_cluster_materialisation" not in job_ids
