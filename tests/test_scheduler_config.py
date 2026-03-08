"""Tests for scheduler configuration flags."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


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

        scheduler.add_sync_job(mock_scheduler, MagicMock())

    job_ids = [call.kwargs.get("id") for call in mock_scheduler.add_job.call_args_list]
    assert "gdelt_sync" not in job_ids
    assert "gdelt_incremental_ingestion" in job_ids
    assert "gdelt_retention_cleanup" in job_ids
