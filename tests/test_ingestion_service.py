"""Tests for ingestion service (GDELT HTTP ingestion)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.db.repositories import ingestion_repository


@pytest.fixture
def mock_gdelt_client():
    """Create a mock GdeltHttpClient."""
    client = MagicMock()
    client.fetch_master_export_urls = AsyncMock(return_value=[])
    client.fetch_latest_export_url = AsyncMock(
        return_value=("https://fake/20260308120000.export.CSV.zip", 20260308120000)
    )
    client.download_events = AsyncMock(return_value=[])
    client.close = AsyncMock()
    return client


def _make_gdelt_row(**overrides) -> dict:
    base = {
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
    base.update(overrides)
    return base


@pytest.mark.asyncio
async def test_row_to_event_dict():
    """Test transformation of GDELT row dict to event dict."""
    from app.services.ingestion_service import _row_to_event_dict

    row = _make_gdelt_row()
    result = _row_to_event_dict(row)

    assert result["global_event_id"] == 1234567890123
    assert result["sql_date"] == 20260301
    assert result["date_added"] == 20260301000000
    assert result["actor1_country_code"] == "USA"
    assert result["event_code"] == "042"


@pytest.mark.asyncio
async def test_run_bootstrap_success(mock_gdelt_client, db_session):
    """Bootstrap with one file containing one row records events_ingested == 1."""
    from app.services import ingestion_service

    mock_gdelt_client.fetch_master_export_urls = AsyncMock(
        return_value=[("https://fake/20260301000000.export.CSV.zip", 20260301000000)]
    )
    mock_gdelt_client.download_events = AsyncMock(return_value=[_make_gdelt_row()])

    with (
        patch.object(ingestion_service, "_get_gdelt_client", return_value=mock_gdelt_client),
        patch.object(ingestion_service, "get_settings") as mock_settings,
    ):
        mock_settings.return_value.retention_days = 30
        mock_settings.return_value.ingestion_batch_size = 10000

        result = await ingestion_service.run_bootstrap(db_session)

    assert result["status"] == "completed"
    assert result["events_ingested"] == 1

    latest = await ingestion_repository.get_latest_successful_ingestion(
        db_session,
        ingestion_repository.IngestionType.BOOTSTRAP,
    )
    assert latest is not None


@pytest.mark.asyncio
async def test_run_bootstrap_no_files(mock_gdelt_client, db_session):
    """Bootstrap with no files from masterfilelist yields events_ingested == 0 and completed."""
    from app.services import ingestion_service

    # fetch_master_export_urls already returns [] by default in the fixture

    with (
        patch.object(ingestion_service, "_get_gdelt_client", return_value=mock_gdelt_client),
        patch.object(ingestion_service, "get_settings") as mock_settings,
    ):
        mock_settings.return_value.retention_days = 30
        mock_settings.return_value.ingestion_batch_size = 10000

        result = await ingestion_service.run_bootstrap(db_session)

    assert result["status"] == "completed"
    assert result["events_ingested"] == 0


@pytest.mark.asyncio
async def test_run_incremental_downloads_new_file(mock_gdelt_client, db_session):
    """Incremental downloads a file whose timestamp is newer than the watermark."""
    from app.services import ingestion_service

    # Latest file is at 20260308120000; watermark is from bootstrap at 20260301000000
    bootstrap_run = await ingestion_repository.create_ingestion_run(
        db_session,
        ingestion_repository.IngestionType.BOOTSTRAP,
    )
    await ingestion_repository.update_ingestion_run(
        db_session,
        bootstrap_run.id,
        ingestion_repository.IngestionStatus.COMPLETED,
        watermark_dateadded=20260301000000,
        events_ingested=0,
    )
    await db_session.commit()

    mock_gdelt_client.fetch_latest_export_url = AsyncMock(
        return_value=("https://fake/20260308120000.export.CSV.zip", 20260308120000)
    )
    mock_gdelt_client.download_events = AsyncMock(return_value=[_make_gdelt_row()])

    with (
        patch.object(ingestion_service, "_get_gdelt_client", return_value=mock_gdelt_client),
        patch.object(ingestion_service, "get_settings") as mock_settings,
    ):
        mock_settings.return_value.retention_days = 30
        mock_settings.return_value.ingestion_batch_size = 10000

        result = await ingestion_service.run_incremental(db_session)

    assert result["events_ingested"] == 1
    mock_gdelt_client.download_events.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_incremental_skips_already_ingested_file(mock_gdelt_client, db_session):
    """Incremental skips download when the file timestamp is not newer than the watermark."""
    from app.services import ingestion_service

    # Watermark (20260308130000) is NEWER than the latest file (20260308120000)
    bootstrap_run = await ingestion_repository.create_ingestion_run(
        db_session,
        ingestion_repository.IngestionType.BOOTSTRAP,
    )
    await ingestion_repository.update_ingestion_run(
        db_session,
        bootstrap_run.id,
        ingestion_repository.IngestionStatus.COMPLETED,
        watermark_dateadded=20260308130000,
        events_ingested=0,
    )
    await db_session.commit()

    mock_gdelt_client.fetch_latest_export_url = AsyncMock(
        return_value=("https://fake/20260308120000.export.CSV.zip", 20260308120000)
    )

    with (
        patch.object(ingestion_service, "_get_gdelt_client", return_value=mock_gdelt_client),
        patch.object(ingestion_service, "get_settings") as mock_settings,
    ):
        mock_settings.return_value.retention_days = 30
        mock_settings.return_value.ingestion_batch_size = 10000

        result = await ingestion_service.run_incremental(db_session)

    assert result["status"] == "completed"
    mock_gdelt_client.download_events.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_incremental_with_existing_bootstrap_watermark(mock_gdelt_client, db_session):
    """Incremental uses bootstrap watermark and downloads a newer file."""
    from app.services import ingestion_service

    bootstrap_run = await ingestion_repository.create_ingestion_run(
        db_session,
        ingestion_repository.IngestionType.BOOTSTRAP,
    )
    await ingestion_repository.update_ingestion_run(
        db_session,
        bootstrap_run.id,
        ingestion_repository.IngestionStatus.COMPLETED,
        watermark_dateadded=20260301000000,
        events_ingested=0,
    )
    await db_session.commit()

    # Latest file is newer than the bootstrap watermark
    mock_gdelt_client.fetch_latest_export_url = AsyncMock(
        return_value=("https://fake/20990308120000.export.CSV.zip", 20990308120000)
    )
    mock_gdelt_client.download_events = AsyncMock(return_value=[_make_gdelt_row()])

    with (
        patch.object(ingestion_service, "_get_gdelt_client", return_value=mock_gdelt_client),
        patch.object(ingestion_service, "get_settings") as mock_settings,
    ):
        mock_settings.return_value.retention_days = 30
        mock_settings.return_value.ingestion_batch_size = 10000

        result = await ingestion_service.run_incremental(db_session)

    assert result["status"] == "completed"
    mock_gdelt_client.download_events.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_incremental_rolls_back_before_marking_failed(mock_gdelt_client):
    """Incremental rolls back the session before recording a failed run."""
    from app.services import ingestion_service

    session = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()

    mock_gdelt_client.fetch_latest_export_url = AsyncMock(
        return_value=("https://fake/20990308120000.export.CSV.zip", 20990308120000)
    )
    mock_gdelt_client.download_events = AsyncMock(return_value=[_make_gdelt_row()])

    with (
        patch.object(ingestion_service, "_get_gdelt_client", return_value=mock_gdelt_client),
        patch.object(
            ingestion_service.ingestion_repository,
            "get_latest_successful_ingestion",
            AsyncMock(side_effect=[None, None]),
        ),
        patch.object(
            ingestion_service.ingestion_repository,
            "create_ingestion_run",
            AsyncMock(return_value=SimpleNamespace(id=123)),
        ),
        patch.object(
            ingestion_service.event_repository,
            "bulk_insert_events",
            AsyncMock(side_effect=RuntimeError("insert failed")),
        ),
        patch.object(
            ingestion_service.ingestion_repository,
            "update_ingestion_run",
            AsyncMock(),
        ) as mock_update_run,
    ):
        with pytest.raises(RuntimeError, match="insert failed"):
            await ingestion_service.run_incremental(session)

    session.rollback.assert_awaited_once()
    mock_update_run.assert_awaited_once()
    mock_gdelt_client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_retention_cleanup(db_session):
    """Retention cleanup deletes events older than retention_days."""
    from app.db.repositories import event_repository
    from app.services import ingestion_service

    old_events = [
        {
            "global_event_id": 1234567890123,
            "sql_date": 20250101,
            "date_added": 20250101000000,
            "actor1_country_code": "USA",
            "source_url": "https://example.com/old",
        },
        {
            "global_event_id": 1234567890124,
            "sql_date": 20260301,
            "date_added": 20260301000000,
            "actor1_country_code": "USA",
            "source_url": "https://example.com/recent",
        },
    ]

    await event_repository.bulk_insert_events(db_session, old_events)
    await db_session.commit()

    with patch.object(ingestion_service, "get_settings") as mock_settings:
        mock_settings.return_value.retention_days = 30

        result = await ingestion_service.run_retention_cleanup(db_session)

    assert result["deleted"] == 1

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
