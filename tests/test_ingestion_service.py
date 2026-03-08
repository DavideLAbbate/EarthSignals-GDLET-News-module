"""Tests for ingestion service."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.db.repositories import ingestion_repository


@pytest.fixture
def mock_bq_client():
    """Create a mock BigQuery client."""
    client = MagicMock()
    client.run_query = AsyncMock(return_value=[])
    return client


@pytest.mark.asyncio
async def test_row_to_event_dict():
    """Test transformation of BQ row to event dict."""
    from app.services.ingestion_service import _row_to_event_dict

    row = {
        "GLOBALEVENTID": 1234567890123,
        "SQLDATE": 20260301,
        "DATEADDED": 20260301000000,
        "Actor1CountryCode": "USA",
        "Actor2CountryCode": "CHN",
        "EventCode": "042",
        "EventBaseCode": "04",
        "EventRootCode": "04",
        "QuadClass": 2,
        "GoldsteinScale": 5.0,
        "AvgTone": -2.5,
        "NumMentions": 10,
        "NumSources": 5,
        "NumArticles": 3,
        "ActionGeo_FullName": "Washington, DC",
        "ActionGeo_CountryCode": "US",
        "SOURCEURL": "https://example.com",
    }

    result = _row_to_event_dict(row)

    assert result["global_event_id"] == 1234567890123
    assert result["sql_date"] == 20260301
    assert result["date_added"] == 20260301000000
    assert result["actor1_country_code"] == "USA"
    assert result["event_code"] == "042"


@pytest.mark.asyncio
async def test_run_bootstrap_success(mock_bq_client, db_session):
    """Test successful bootstrap ingestion."""
    from app.services import ingestion_service

    # Mock BQ to return some rows
    mock_bq_client.run_query.return_value = [
        {
            "GLOBALEVENTID": 1234567890123,
            "SQLDATE": 20260301,
            "DATEADDED": 20260301000000,
            "Actor1CountryCode": "USA",
            "Actor2CountryCode": "CHN",
            "EventCode": "042",
            "EventBaseCode": "04",
            "EventRootCode": "04",
            "QuadClass": 2,
            "GoldsteinScale": 5.0,
            "AvgTone": -2.5,
            "NumMentions": 10,
            "NumSources": 5,
            "NumArticles": 3,
            "ActionGeo_FullName": "Washington, DC",
            "ActionGeo_CountryCode": "US",
            "SOURCEURL": "https://example.com",
        },
    ]

    with patch.object(ingestion_service, "get_settings") as mock_settings:
        mock_settings.return_value.retention_days = 30
        mock_settings.return_value.ingestion_batch_size = 10000

        result = await ingestion_service.run_bootstrap(
            mock_bq_client,
            db_session,
        )

    assert result["status"] == "completed"
    assert result["events_ingested"] == 1

    # Verify ingestion run was created
    latest = await ingestion_repository.get_latest_successful_ingestion(
        db_session,
        ingestion_repository.IngestionType.BOOTSTRAP,
    )
    assert latest is not None


@pytest.mark.asyncio
async def test_run_bootstrap_awaits_async_bigquery_client(db_session):
    """Bootstrap should await the async BigQuery client wrapper directly."""
    from app.services import ingestion_service

    mock_bq_client = MagicMock()
    mock_bq_client.run_query = AsyncMock(
        return_value=[
            {
                "GLOBALEVENTID": 1234567890125,
                "SQLDATE": 20260301,
                "DATEADDED": 20260301000000,
                "Actor1CountryCode": "USA",
                "SOURCEURL": "https://example.com/async",
            }
        ]
    )

    with patch.object(ingestion_service, "get_settings") as mock_settings:
        mock_settings.return_value.retention_days = 30
        mock_settings.return_value.ingestion_batch_size = 10000

        result = await ingestion_service.run_bootstrap(
            mock_bq_client,
            db_session,
        )

    assert result["status"] == "completed"
    assert result["events_ingested"] == 1
    assert mock_bq_client.run_query.await_count == 1


@pytest.mark.asyncio
async def test_run_bootstrap_limits_query_to_retention_window(mock_bq_client, db_session):
    """Bootstrap should pass SQLDATE bounds to keep BigQuery scans bounded."""
    from app.services import ingestion_service

    with (
        patch.object(ingestion_service, "get_settings") as mock_settings,
        patch.object(
            ingestion_service,
            "build_ingestion_bootstrap_query",
            return_value=("SELECT 1", []),
        ) as build_query,
    ):
        mock_settings.return_value.retention_days = 30
        mock_settings.return_value.ingestion_batch_size = 10000

        await ingestion_service.run_bootstrap(
            mock_bq_client,
            db_session,
        )

    assert build_query.call_args.kwargs["date_from_sqldate"] == int(
        str(build_query.call_args.kwargs["since_dateadded"])[:8]
    )
    assert (
        build_query.call_args.kwargs["date_from_sqldate"]
        <= build_query.call_args.kwargs["date_to_sqldate"]
    )


@pytest.mark.asyncio
async def test_run_incremental_limits_query_to_recent_sqldate_range(mock_bq_client, db_session):
    """Incremental ingestion should also include SQLDATE bounds."""
    from app.services import ingestion_service

    with (
        patch.object(ingestion_service, "get_settings") as mock_settings,
        patch.object(
            ingestion_service,
            "build_ingestion_incremental_query",
            return_value=("SELECT 1", []),
        ) as build_query,
    ):
        mock_settings.return_value.ingestion_batch_size = 10000

        await ingestion_service.run_incremental(
            mock_bq_client,
            db_session,
        )

    assert build_query.call_args.kwargs["date_from_sqldate"] == int(
        str(build_query.call_args.kwargs["since_dateadded"])[:8]
    )
    assert (
        build_query.call_args.kwargs["date_from_sqldate"]
        <= build_query.call_args.kwargs["date_to_sqldate"]
    )


@pytest.mark.asyncio
async def test_run_incremental_with_watermark(mock_bq_client, db_session):
    """Test incremental ingestion uses existing watermark."""
    from app.services import ingestion_service

    # First complete a bootstrap
    bootstrap_run = await ingestion_repository.create_ingestion_run(
        db_session,
        ingestion_repository.IngestionType.BOOTSTRAP,
    )
    await ingestion_repository.update_ingestion_run(
        db_session,
        bootstrap_run.id,
        ingestion_repository.IngestionStatus.COMPLETED,
        watermark_dateadded=20260301000000,
        events_ingested=100,
    )
    await db_session.commit()

    # Mock BQ for incremental
    mock_bq_client.run_query.return_value = []

    with patch.object(ingestion_service, "get_settings") as mock_settings:
        mock_settings.return_value.ingestion_batch_size = 10000

        result = await ingestion_service.run_incremental(
            mock_bq_client,
            db_session,
        )

    assert result["status"] == "completed"
    # Should have used the bootstrap watermark
    assert result["watermark"] == 20260301000000


@pytest.mark.asyncio
async def test_run_retention_cleanup(db_session):
    """Test retention cleanup deletes old events."""
    from app.db.repositories import event_repository
    from app.services import ingestion_service

    # Insert events with old dates
    old_events = [
        {
            "global_event_id": 1234567890123,
            "sql_date": 20250101,  # Old date
            "date_added": 20250101000000,
            "actor1_country_code": "USA",
            "source_url": "https://example.com/old",
        },
        {
            "global_event_id": 1234567890124,
            "sql_date": 20260301,  # Recent date
            "date_added": 20260301000000,
            "actor1_country_code": "USA",
            "source_url": "https://example.com/recent",
        },
    ]

    await event_repository.bulk_insert_events(db_session, old_events)
    await db_session.commit()

    # Run retention with 30 days
    with patch.object(ingestion_service, "get_settings") as mock_settings:
        mock_settings.return_value.retention_days = 30

        result = await ingestion_service.run_retention_cleanup(db_session)

    # Should have deleted the old event
    assert result["deleted"] == 1

    # Recent event should remain
    count = await event_repository.get_event_count(db_session)
    assert count == 1


@pytest.mark.asyncio
async def test_should_bootstrap_on_startup_when_store_is_empty(db_session):
    """Bootstrap should run when there are no stored events or prior bootstrap runs."""
    from app.services import ingestion_service

    should_bootstrap = await ingestion_service.should_bootstrap_on_startup(db_session)

    assert should_bootstrap is True


@pytest.mark.asyncio
async def test_should_not_bootstrap_on_startup_when_events_exist(db_session):
    """Existing local events should prevent startup bootstrap."""
    from app.db.repositories import event_repository
    from app.services import ingestion_service

    await event_repository.bulk_insert_events(
        db_session,
        [
            {
                "global_event_id": 1234567899999,
                "sql_date": 20260301,
                "date_added": 20260301000000,
                "actor1_country_code": "USA",
                "source_url": "https://example.com/existing",
            }
        ],
    )
    await db_session.commit()

    should_bootstrap = await ingestion_service.should_bootstrap_on_startup(db_session)

    assert should_bootstrap is False
