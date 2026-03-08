"""Tests for BigQuery client creation."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest


def test_create_bigquery_client_uses_service_account_json_env():
    """Railway-style inline JSON credentials should be supported without a file mount."""
    from app.integrations import bigquery_client

    service_account_info = {
        "type": "service_account",
        "project_id": "test-project",
        "private_key_id": "key-id",
        "private_key": "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n",
        "client_email": "svc@test-project.iam.gserviceaccount.com",
        "client_id": "1234567890",
        "token_uri": "https://oauth2.googleapis.com/token",
    }

    with (
        patch.object(bigquery_client, "get_settings") as mock_settings,
        patch.object(bigquery_client.bigquery, "Client") as mock_client,
        patch.object(
            bigquery_client.service_account.Credentials,
            "from_service_account_info",
            return_value=MagicMock(),
        ) as from_info,
    ):
        mock_settings.return_value.gcp_project_id = "test-project"
        mock_settings.return_value.gcp_service_account_json = json.dumps(service_account_info)
        mock_settings.return_value.bq_executor_max_workers = 4

        bigquery_client.create_bigquery_client()

    from_info.assert_called_once_with(service_account_info)
    mock_client.assert_called_once()
    assert mock_client.call_args.kwargs["project"] == "test-project"
    assert "credentials" in mock_client.call_args.kwargs


def test_create_bigquery_client_prefers_inline_json_over_file_credentials():
    """Inline JSON credentials should bypass default file-based auth lookup."""
    from app.integrations import bigquery_client

    service_account_info = {
        "type": "service_account",
        "project_id": "test-project",
        "private_key_id": "key-id",
        "private_key": "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n",
        "client_email": "svc@test-project.iam.gserviceaccount.com",
        "client_id": "1234567890",
        "token_uri": "https://oauth2.googleapis.com/token",
    }

    with (
        patch.object(bigquery_client, "get_settings") as mock_settings,
        patch.object(bigquery_client.bigquery, "Client") as mock_client,
        patch.object(
            bigquery_client.service_account.Credentials,
            "from_service_account_info",
            return_value=MagicMock(),
        ),
    ):
        mock_settings.return_value.gcp_project_id = "test-project"
        mock_settings.return_value.gcp_service_account_json = json.dumps(service_account_info)
        mock_settings.return_value.bq_executor_max_workers = 4

        bigquery_client.create_bigquery_client()

    assert mock_client.call_args.kwargs["credentials"] is not None


def test_create_bigquery_client_raises_clear_error_for_invalid_json():
    """Invalid inline JSON should raise a BigQueryError with actionable guidance."""
    from app.core.exceptions import BigQueryError
    from app.integrations import bigquery_client

    with patch.object(bigquery_client, "get_settings") as mock_settings:
        mock_settings.return_value.gcp_project_id = "test-project"
        mock_settings.return_value.gcp_service_account_json = "{not-json}"
        mock_settings.return_value.bq_executor_max_workers = 4

        with pytest.raises(BigQueryError, match="service account JSON"):
            bigquery_client.create_bigquery_client()
