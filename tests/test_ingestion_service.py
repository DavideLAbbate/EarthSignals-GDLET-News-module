"""Tests for ingestion service (GDELT HTTP ingestion)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

from app.db.models import GdeltEvent, GdeltGkg, GdeltMention
from app.db.repositories import ingestion_repository


class _SingleAccessRun:
    """Test double that fails if its id is read more than once."""

    def __init__(self, run_id: int) -> None:
        self._run_id = run_id
        self._reads = 0

    @property
    def id(self) -> int:
        self._reads += 1
        if self._reads > 1:
            raise RuntimeError("run.id accessed more than once")
        return self._run_id


class _ExpiringRun:
    """Test double that simulates an ORM instance expired after commit/rollback."""

    def __init__(self, run_id: int) -> None:
        self._run_id = run_id
        self._expired = False

    def expire(self) -> None:
        self._expired = True

    @property
    def id(self) -> int:
        if self._expired:
            raise RuntimeError("run.id unavailable after expiration")
        return self._run_id


@pytest.fixture
def mock_gdelt_client():
    """Create a mock GdeltHttpClient."""
    client = MagicMock()
    client.fetch_master_export_urls = AsyncMock(return_value=[])
    client.fetch_master_mentions_urls = AsyncMock(return_value=[])
    client.fetch_master_gkg_urls = AsyncMock(return_value=[])
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


def _make_mentions_row(**overrides) -> dict:
    base = {
        "GLOBALEVENTID": 1234567890123,
        "EventTimeDate": 20260301000000,
        "MentionTimeDate": 20260301010000,
        "MentionType": 1,
        "MentionSourceName": "example.com",
        "MentionIdentifier": "https://example.com/story",
        "MentionDocLen": 1200,
        "MentionDocTone": -1.5,
    }
    base.update(overrides)
    return base


def _make_gkg_row(**overrides) -> dict:
    base = {
        "GKGRECORDID": "20260301000000-1",
        "DATE": 20260301000000,
        "SourceCommonName": "example.com",
        "DocumentIdentifier": "https://example.com/story",
        "V1Themes": ["IRAN", "CONFLICT"],
        "V1Persons": ["Person A"],
        "V1Organizations": ["Org A"],
        "V1Locations": ["Tehran, Tehran, Iran"],
        "AvgTone": -3.2,
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
async def test_row_to_mention_dict():
    """Test transformation of EVENTMENTIONS row dict to mention dict."""
    from app.services.ingestion_service import _row_to_mention_dict

    row = _make_mentions_row()
    result = _row_to_mention_dict(row)

    assert result["global_event_id"] == 1234567890123
    assert result["mention_identifier"] == "https://example.com/story"
    assert result["mention_source_name"] == "example.com"
    assert result["mention_doc_tone"] == -1.5


@pytest.mark.asyncio
async def test_row_to_gkg_dict():
    """Test transformation of GKG row dict to gkg dict."""
    from app.services.ingestion_service import _row_to_gkg_dict

    row = _make_gkg_row()
    result = _row_to_gkg_dict(row)

    assert result["gkg_record_id"] == "20260301000000-1"
    assert result["document_identifier"] == "https://example.com/story"
    assert result["themes"] == ["IRAN", "CONFLICT"]
    assert result["document_tone"] == -3.2


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
async def test_run_bootstrap_range_rejects_inverted_bounds(db_session):
    """Explicit bootstrap bounds must be ordered chronologically."""
    from app.services import ingestion_service

    with pytest.raises(ValueError, match="start.*end"):
        await ingestion_service.run_bootstrap_range(
            db_session,
            since_ts=20260309000000,
            until_ts=20260308235959,
        )


@pytest.mark.asyncio
async def test_run_bootstrap_range_ingests_requested_window_and_persists_until_watermark(
    mock_gdelt_client, db_session
):
    """Explicit bootstrap range should ignore out-of-window files and persist the requested end watermark."""
    from app.services import ingestion_service

    since_ts = 20260301000000
    until_ts = 20260301235959

    mock_gdelt_client.fetch_master_export_urls = AsyncMock(
        return_value=[
            ("https://fake/20260228234500.export.CSV.zip", 20260228234500),
            ("https://fake/20260301000000.export.CSV.zip", 20260301000000),
            ("https://fake/20260301120000.export.CSV.zip", 20260301120000),
            ("https://fake/20260302000000.export.CSV.zip", 20260302000000),
        ]
    )
    mock_gdelt_client.fetch_master_mentions_urls = AsyncMock(
        return_value=[
            ("https://fake/20260301000000.mentions.CSV.zip", 20260301000000),
            ("https://fake/20260301120000.mentions.CSV.zip", 20260301120000),
        ]
    )
    mock_gdelt_client.fetch_master_gkg_urls = AsyncMock(
        return_value=[
            ("https://fake/20260301000000.gkg.csv.zip", 20260301000000),
            ("https://fake/20260301120000.gkg.csv.zip", 20260301120000),
        ]
    )
    mock_gdelt_client.download_events = AsyncMock(
        side_effect=[
            [_make_gdelt_row(GLOBALEVENTID=1, DATEADDED=20260301000000)],
            [_make_gdelt_row(GLOBALEVENTID=2, DATEADDED=20260301120000)],
        ]
    )
    mock_gdelt_client.download_mentions = AsyncMock(
        side_effect=[
            [_make_mentions_row(GLOBALEVENTID=1, MentionIdentifier="https://example.com/story-1")],
            [_make_mentions_row(GLOBALEVENTID=2, MentionIdentifier="https://example.com/story-2")],
        ]
    )
    mock_gdelt_client.download_gkg = AsyncMock(
        side_effect=[
            [_make_gkg_row(GKGRECORDID="1", DocumentIdentifier="https://example.com/story-1")],
            [_make_gkg_row(GKGRECORDID="2", DocumentIdentifier="https://example.com/story-2")],
        ]
    )

    with patch.object(ingestion_service, "_get_gdelt_client", return_value=mock_gdelt_client):
        result = await ingestion_service.run_bootstrap_range(
            db_session,
            since_ts=since_ts,
            until_ts=until_ts,
        )

    assert result["status"] == "completed"
    assert result["events_ingested"] == 2
    assert result["watermark"] == until_ts
    assert mock_gdelt_client.download_events.await_count == 2

    events_result = await db_session.execute(select(GdeltEvent))
    mentions_result = await db_session.execute(select(GdeltMention))
    gkg_result = await db_session.execute(select(GdeltGkg))

    assert len(events_result.scalars().all()) == 2
    assert len(mentions_result.scalars().all()) == 2
    assert len(gkg_result.scalars().all()) == 2

    latest = await ingestion_repository.get_latest_successful_ingestion(
        db_session,
        ingestion_repository.IngestionType.BOOTSTRAP,
    )
    assert latest is not None
    assert latest.watermark_dateadded == until_ts


@pytest.mark.asyncio
async def test_run_bootstrap_range_uses_bulk_insert_events(mock_gdelt_client, db_session):
    """Explicit range bootstrap should route event writes through the chunk-safe repository helper."""
    from app.services import ingestion_service

    file_ts = 20260301000000
    event_rows = [_make_gdelt_row(GLOBALEVENTID=11, DATEADDED=file_ts)]
    expected_events = [ingestion_service._row_to_event_dict(row) for row in event_rows]

    mock_gdelt_client.fetch_master_export_urls = AsyncMock(
        return_value=[(f"https://fake/{file_ts}.export.CSV.zip", file_ts)]
    )
    mock_gdelt_client.download_events = AsyncMock(return_value=event_rows)

    with (
        patch.object(ingestion_service, "_get_gdelt_client", return_value=mock_gdelt_client),
        patch.object(
            ingestion_service.event_repository,
            "bulk_insert_events",
            AsyncMock(return_value=len(event_rows)),
        ) as mock_bulk_insert,
    ):
        result = await ingestion_service.run_bootstrap_range(
            db_session,
            since_ts=file_ts,
            until_ts=file_ts,
        )

    assert result["status"] == "completed"
    mock_bulk_insert.assert_awaited_once_with(db_session, expected_events)


@pytest.mark.asyncio
async def test_run_bootstrap_range_fetches_mentions_and_gkg_indexes_once(
    mock_gdelt_client, db_session
):
    """Explicit-range bootstrap should fetch each sidecar master index only once per run."""
    from app.services import ingestion_service

    since_ts = 20260301000000
    until_ts = 20260301120000

    mock_gdelt_client.fetch_master_export_urls = AsyncMock(
        return_value=[
            (f"https://fake/{since_ts}.export.CSV.zip", since_ts),
            (f"https://fake/{until_ts}.export.CSV.zip", until_ts),
        ]
    )
    mock_gdelt_client.fetch_master_mentions_urls = AsyncMock(
        return_value=[
            (f"https://fake/{since_ts}.mentions.CSV.zip", since_ts),
            (f"https://fake/{until_ts}.mentions.CSV.zip", until_ts),
        ]
    )
    mock_gdelt_client.fetch_master_gkg_urls = AsyncMock(
        return_value=[
            (f"https://fake/{since_ts}.gkg.csv.zip", since_ts),
            (f"https://fake/{until_ts}.gkg.csv.zip", until_ts),
        ]
    )
    mock_gdelt_client.download_events = AsyncMock(
        side_effect=[
            [_make_gdelt_row(GLOBALEVENTID=1, DATEADDED=since_ts)],
            [_make_gdelt_row(GLOBALEVENTID=2, DATEADDED=until_ts)],
        ]
    )
    mock_gdelt_client.download_mentions = AsyncMock(
        side_effect=[
            [_make_mentions_row(GLOBALEVENTID=1, MentionIdentifier="https://example.com/story-1")],
            [_make_mentions_row(GLOBALEVENTID=2, MentionIdentifier="https://example.com/story-2")],
        ]
    )
    mock_gdelt_client.download_gkg = AsyncMock(
        side_effect=[
            [_make_gkg_row(GKGRECORDID="1", DocumentIdentifier="https://example.com/story-1")],
            [_make_gkg_row(GKGRECORDID="2", DocumentIdentifier="https://example.com/story-2")],
        ]
    )

    with patch.object(ingestion_service, "_get_gdelt_client", return_value=mock_gdelt_client):
        result = await ingestion_service.run_bootstrap_range(
            db_session,
            since_ts=since_ts,
            until_ts=until_ts,
        )

    assert result["status"] == "completed"
    mock_gdelt_client.fetch_master_mentions_urls.assert_awaited_once_with(
        since_ts=since_ts,
        until_ts=until_ts,
    )
    mock_gdelt_client.fetch_master_gkg_urls.assert_awaited_once_with(
        since_ts=since_ts,
        until_ts=until_ts,
    )


@pytest.mark.asyncio
async def test_run_bootstrap_range_keeps_events_when_mentions_and_gkg_fail(
    mock_gdelt_client, db_session
):
    """Best-effort sidecar failures must not roll back explicit-range event ingestion."""
    from app.services import ingestion_service

    file_ts = 20260301000000

    mock_gdelt_client.fetch_master_export_urls = AsyncMock(
        return_value=[(f"https://fake/{file_ts}.export.CSV.zip", file_ts)]
    )
    mock_gdelt_client.fetch_master_mentions_urls = AsyncMock(
        return_value=[(f"https://fake/{file_ts}.mentions.CSV.zip", file_ts)]
    )
    mock_gdelt_client.fetch_master_gkg_urls = AsyncMock(
        return_value=[(f"https://fake/{file_ts}.gkg.csv.zip", file_ts)]
    )
    mock_gdelt_client.download_events = AsyncMock(
        return_value=[_make_gdelt_row(GLOBALEVENTID=1, DATEADDED=file_ts)]
    )
    mock_gdelt_client.download_mentions = AsyncMock(side_effect=RuntimeError("mentions failed"))
    mock_gdelt_client.download_gkg = AsyncMock(side_effect=RuntimeError("gkg failed"))

    with patch.object(ingestion_service, "_get_gdelt_client", return_value=mock_gdelt_client):
        result = await ingestion_service.run_bootstrap_range(
            db_session,
            since_ts=file_ts,
            until_ts=file_ts,
        )

    assert result["status"] == "completed"
    assert result["events_ingested"] == 1
    assert result["watermark"] == file_ts

    events_result = await db_session.execute(select(GdeltEvent))
    mentions_result = await db_session.execute(select(GdeltMention))
    gkg_result = await db_session.execute(select(GdeltGkg))

    assert len(events_result.scalars().all()) == 1
    assert len(mentions_result.scalars().all()) == 0
    assert len(gkg_result.scalars().all()) == 0

    latest = await ingestion_repository.get_latest_successful_ingestion(
        db_session,
        ingestion_repository.IngestionType.BOOTSTRAP,
    )
    assert latest is not None
    assert latest.watermark_dateadded == file_ts


@pytest.mark.asyncio
async def test_run_bootstrap_range_caches_run_id_before_failure(mock_gdelt_client):
    """Bootstrap range must not touch run.id again after rollback starts."""
    from app.services import ingestion_service

    run = _ExpiringRun(321)
    session = AsyncMock()
    session.commit = AsyncMock(side_effect=run.expire)
    session.rollback = AsyncMock()

    file_ts = 20990308120000
    mock_gdelt_client.fetch_master_export_urls = AsyncMock(
        return_value=[(f"https://fake/{file_ts}.export.CSV.zip", file_ts)]
    )
    mock_gdelt_client.download_events = AsyncMock(return_value=[_make_gdelt_row()])

    with (
        patch.object(ingestion_service, "_get_gdelt_client", return_value=mock_gdelt_client),
        patch.object(
            ingestion_service.ingestion_repository,
            "create_ingestion_run",
            AsyncMock(return_value=run),
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
            await ingestion_service.run_bootstrap_range(
                session,
                since_ts=file_ts,
                until_ts=file_ts,
            )

    session.rollback.assert_awaited_once()
    mock_update_run.assert_awaited_once_with(
        session,
        321,
        ingestion_repository.IngestionStatus.FAILED,
        error_message="insert failed",
    )
    mock_gdelt_client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_incremental_downloads_new_file(mock_gdelt_client, db_session):
    """Incremental downloads all files newer than the watermark."""
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

    mock_gdelt_client.fetch_master_export_urls = AsyncMock(
        return_value=[
            ("https://fake/20260308120000.export.CSV.zip", 20260308120000),
            ("https://fake/20260308121500.export.CSV.zip", 20260308121500),
        ]
    )
    mock_gdelt_client.download_events = AsyncMock(
        side_effect=[
            [_make_gdelt_row(GLOBALEVENTID=1, DATEADDED=20260308120000)],
            [_make_gdelt_row(GLOBALEVENTID=2, DATEADDED=20260308121500)],
        ]
    )

    with (
        patch.object(ingestion_service, "_get_gdelt_client", return_value=mock_gdelt_client),
        patch.object(ingestion_service, "get_settings") as mock_settings,
    ):
        mock_settings.return_value.retention_days = 30
        mock_settings.return_value.ingestion_batch_size = 10000

        result = await ingestion_service.run_incremental(db_session)

    assert result["events_ingested"] == 2
    assert mock_gdelt_client.download_events.await_count == 2
    assert result["watermark"] == 20260308121500


@pytest.mark.asyncio
async def test_run_incremental_skips_already_ingested_file(mock_gdelt_client, db_session):
    """Incremental skips download when no files newer than watermark exist."""
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

    mock_gdelt_client.fetch_master_export_urls = AsyncMock(return_value=[])

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
    """Incremental uses bootstrap watermark and downloads missing files after it."""
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

    mock_gdelt_client.fetch_master_export_urls = AsyncMock(
        return_value=[
            ("https://fake/20990308120000.export.CSV.zip", 20990308120000),
            ("https://fake/20990308121500.export.CSV.zip", 20990308121500),
        ]
    )
    mock_gdelt_client.download_events = AsyncMock(
        side_effect=[
            [_make_gdelt_row(GLOBALEVENTID=1, DATEADDED=20990308120000)],
            [_make_gdelt_row(GLOBALEVENTID=2, DATEADDED=20990308121500)],
        ]
    )

    with (
        patch.object(ingestion_service, "_get_gdelt_client", return_value=mock_gdelt_client),
        patch.object(ingestion_service, "get_settings") as mock_settings,
    ):
        mock_settings.return_value.retention_days = 30
        mock_settings.return_value.ingestion_batch_size = 10000

        result = await ingestion_service.run_incremental(db_session)

    assert result["status"] == "completed"
    assert mock_gdelt_client.download_events.await_count == 2
    assert result["watermark"] == 20990308121500


@pytest.mark.asyncio
async def test_run_incremental_ingests_mentions_and_gkg(mock_gdelt_client, db_session):
    """Incremental ingestion stores events, mentions, and GKG rows for each missing file."""
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

    mock_gdelt_client.fetch_master_export_urls = AsyncMock(
        return_value=[
            ("https://fake/20990308120000.export.CSV.zip", 20990308120000),
            ("https://fake/20990308121500.export.CSV.zip", 20990308121500),
        ]
    )
    mock_gdelt_client.fetch_master_mentions_urls = AsyncMock(
        side_effect=[
            [("https://fake/20990308120000.mentions.CSV.zip", 20990308120000)],
            [("https://fake/20990308121500.mentions.CSV.zip", 20990308121500)],
        ]
    )
    mock_gdelt_client.fetch_master_gkg_urls = AsyncMock(
        side_effect=[
            [("https://fake/20990308120000.gkg.csv.zip", 20990308120000)],
            [("https://fake/20990308121500.gkg.csv.zip", 20990308121500)],
        ]
    )
    mock_gdelt_client.download_events = AsyncMock(
        side_effect=[
            [_make_gdelt_row(GLOBALEVENTID=1, DATEADDED=20990308120000)],
            [_make_gdelt_row(GLOBALEVENTID=2, DATEADDED=20990308121500)],
        ]
    )
    mock_gdelt_client.download_mentions = AsyncMock(
        side_effect=[
            [_make_mentions_row(GLOBALEVENTID=1, MentionIdentifier="https://example.com/story-1")],
            [_make_mentions_row(GLOBALEVENTID=2, MentionIdentifier="https://example.com/story-2")],
        ]
    )
    mock_gdelt_client.download_gkg = AsyncMock(
        side_effect=[
            [_make_gkg_row(GKGRECORDID="1", DocumentIdentifier="https://example.com/story-1")],
            [_make_gkg_row(GKGRECORDID="2", DocumentIdentifier="https://example.com/story-2")],
        ]
    )

    with (
        patch.object(ingestion_service, "_get_gdelt_client", return_value=mock_gdelt_client),
        patch.object(ingestion_service, "get_settings") as mock_settings,
    ):
        mock_settings.return_value.retention_days = 30
        mock_settings.return_value.ingestion_batch_size = 10000

        result = await ingestion_service.run_incremental(db_session)

    assert result["events_ingested"] == 2

    events_result = await db_session.execute(select(GdeltEvent))
    mentions_result = await db_session.execute(select(GdeltMention))
    gkg_result = await db_session.execute(select(GdeltGkg))

    assert len(events_result.scalars().all()) == 2
    assert len(mentions_result.scalars().all()) == 2
    assert len(gkg_result.scalars().all()) == 2


@pytest.mark.asyncio
async def test_run_incremental_keeps_processing_later_files_when_one_mentions_batch_fails(
    mock_gdelt_client, db_session
):
    """A mentions failure for one timestamp should not block later files from being ingested."""
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

    mock_gdelt_client.fetch_master_export_urls = AsyncMock(
        return_value=[
            ("https://fake/20990308120000.export.CSV.zip", 20990308120000),
            ("https://fake/20990308121500.export.CSV.zip", 20990308121500),
        ]
    )
    mock_gdelt_client.fetch_master_mentions_urls = AsyncMock(
        side_effect=[
            [("https://fake/20990308120000.mentions.CSV.zip", 20990308120000)],
            [("https://fake/20990308121500.mentions.CSV.zip", 20990308121500)],
        ]
    )
    mock_gdelt_client.fetch_master_gkg_urls = AsyncMock(
        side_effect=[
            [("https://fake/20990308120000.gkg.csv.zip", 20990308120000)],
            [("https://fake/20990308121500.gkg.csv.zip", 20990308121500)],
        ]
    )
    mock_gdelt_client.download_events = AsyncMock(
        side_effect=[
            [_make_gdelt_row(GLOBALEVENTID=1, DATEADDED=20990308120000)],
            [_make_gdelt_row(GLOBALEVENTID=2, DATEADDED=20990308121500)],
        ]
    )
    mock_gdelt_client.download_mentions = AsyncMock(
        side_effect=[
            RuntimeError("mentions failed"),
            [_make_mentions_row(GLOBALEVENTID=2, MentionIdentifier="https://example.com/story-2")],
        ]
    )
    mock_gdelt_client.download_gkg = AsyncMock(
        side_effect=[
            [_make_gkg_row(GKGRECORDID="1", DocumentIdentifier="https://example.com/story-1")],
            [_make_gkg_row(GKGRECORDID="2", DocumentIdentifier="https://example.com/story-2")],
        ]
    )

    with (
        patch.object(ingestion_service, "_get_gdelt_client", return_value=mock_gdelt_client),
        patch.object(ingestion_service, "get_settings") as mock_settings,
    ):
        mock_settings.return_value.retention_days = 30
        mock_settings.return_value.ingestion_batch_size = 10000

        result = await ingestion_service.run_incremental(db_session)

    assert result["events_ingested"] == 2
    assert result["watermark"] == 20990308121500

    events_result = await db_session.execute(select(GdeltEvent))
    mentions_result = await db_session.execute(select(GdeltMention))
    gkg_result = await db_session.execute(select(GdeltGkg))

    assert len(events_result.scalars().all()) == 2
    assert len(mentions_result.scalars().all()) == 1
    assert len(gkg_result.scalars().all()) == 2


@pytest.mark.asyncio
async def test_run_incremental_keeps_events_when_mentions_fail(mock_gdelt_client, db_session):
    """Mentions download failure should not roll back already committed events or GKG."""
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

    mock_gdelt_client.fetch_master_export_urls = AsyncMock(
        return_value=[("https://fake/20990308120000.export.CSV.zip", 20990308120000)]
    )
    mock_gdelt_client.fetch_master_mentions_urls = AsyncMock(
        return_value=[("https://fake/20990308120000.mentions.CSV.zip", 20990308120000)]
    )
    mock_gdelt_client.fetch_master_gkg_urls = AsyncMock(
        return_value=[("https://fake/20990308120000.gkg.csv.zip", 20990308120000)]
    )
    mock_gdelt_client.download_events = AsyncMock(return_value=[_make_gdelt_row()])
    mock_gdelt_client.download_mentions = AsyncMock(side_effect=RuntimeError("mentions failed"))
    mock_gdelt_client.download_gkg = AsyncMock(return_value=[_make_gkg_row()])

    with (
        patch.object(ingestion_service, "_get_gdelt_client", return_value=mock_gdelt_client),
        patch.object(ingestion_service, "get_settings") as mock_settings,
    ):
        mock_settings.return_value.retention_days = 30
        mock_settings.return_value.ingestion_batch_size = 10000

        result = await ingestion_service.run_incremental(db_session)

    assert result["events_ingested"] == 1

    events_result = await db_session.execute(select(GdeltEvent))
    mentions_result = await db_session.execute(select(GdeltMention))
    gkg_result = await db_session.execute(select(GdeltGkg))

    assert len(events_result.scalars().all()) == 1
    assert len(mentions_result.scalars().all()) == 0
    assert len(gkg_result.scalars().all()) == 1


@pytest.mark.asyncio
async def test_run_incremental_rolls_back_before_marking_failed(mock_gdelt_client):
    """Incremental rolls back the session before recording a failed run."""
    from app.services import ingestion_service

    run = _ExpiringRun(123)
    session = AsyncMock()
    session.commit = AsyncMock(side_effect=run.expire)
    session.rollback = AsyncMock()

    mock_gdelt_client.fetch_master_export_urls = AsyncMock(
        return_value=[("https://fake/20990308120000.export.CSV.zip", 20990308120000)]
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
            AsyncMock(return_value=run),
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
    """Retention cleanup deletes events, mentions, and GKG rows older than retention_days."""
    from app.db.repositories import event_repository
    from app.db.repositories.gkg_repository import GkgRepository
    from app.db.repositories.mentions_repository import MentionsRepository
    from app.services import ingestion_service

    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    old_sqldate = int((now - timedelta(days=60)).strftime("%Y%m%d"))
    old_dateadded = int((now - timedelta(days=60)).strftime("%Y%m%d%H%M%S"))
    recent_sqldate = int((now - timedelta(days=5)).strftime("%Y%m%d"))
    recent_dateadded = int((now - timedelta(days=5)).strftime("%Y%m%d%H%M%S"))

    old_events = [
        {
            "global_event_id": 1234567890123,
            "sql_date": old_sqldate,
            "date_added": old_dateadded,
            "actor1_country_code": "USA",
            "source_url": "https://example.com/old",
        },
        {
            "global_event_id": 1234567890124,
            "sql_date": recent_sqldate,
            "date_added": recent_dateadded,
            "actor1_country_code": "USA",
            "source_url": "https://example.com/recent",
        },
    ]
    await event_repository.bulk_insert_events(db_session, old_events)
    await db_session.commit()

    mentions_repo = MentionsRepository(db_session)
    await mentions_repo.bulk_upsert(
        [
            {
                "global_event_id": 1234567890123,
                "event_time_date": old_dateadded,
                "mention_time_date": old_dateadded,
                "mention_type": 1,
                "mention_source_name": "old-source",
                "mention_identifier": "https://example.com/old-article",
            },
            {
                "global_event_id": 1234567890124,
                "event_time_date": recent_dateadded,
                "mention_time_date": recent_dateadded,
                "mention_type": 1,
                "mention_source_name": "recent-source",
                "mention_identifier": "https://example.com/recent-article",
            },
        ]
    )
    await db_session.commit()

    gkg_repo = GkgRepository(db_session)
    await gkg_repo.bulk_upsert(
        [
            {
                "document_identifier": "https://example.com/old-article",
                "date": old_dateadded,
                "themes": [],
            },
            {
                "document_identifier": "https://example.com/recent-article",
                "date": recent_dateadded,
                "themes": [],
            },
        ]
    )
    await db_session.commit()

    with patch.object(ingestion_service, "get_settings") as mock_settings:
        mock_settings.return_value.retention_days = 30

        result = await ingestion_service.run_retention_cleanup(db_session)

    assert result["deleted_events"] == 1
    assert result["deleted_mentions"] == 1
    assert result["deleted_gkg"] == 1

    assert await event_repository.get_event_count(db_session) == 1


@pytest.mark.asyncio
async def test_should_catchup_on_startup_when_watermark_is_stale(db_session):
    """should_catchup_on_startup returns True when the last watermark is > 4 hours old."""
    from datetime import datetime, timedelta, timezone

    from app.db.repositories import ingestion_repository
    from app.services import ingestion_service

    stale_watermark = int(
        (datetime.now(timezone.utc) - timedelta(hours=10)).strftime("%Y%m%d%H%M%S")
    )
    run = await ingestion_repository.create_ingestion_run(
        db_session, ingestion_repository.IngestionType.INCREMENTAL
    )
    await ingestion_repository.update_ingestion_run(
        db_session,
        run.id,
        ingestion_repository.IngestionStatus.COMPLETED,
        watermark_dateadded=stale_watermark,
        events_ingested=100,
    )
    await db_session.commit()

    assert await ingestion_service.should_catchup_on_startup(db_session) is True


@pytest.mark.asyncio
async def test_should_not_catchup_on_startup_when_watermark_is_fresh(db_session):
    """should_catchup_on_startup returns False when the last watermark is recent."""
    from datetime import datetime, timedelta, timezone

    from app.db.repositories import ingestion_repository
    from app.services import ingestion_service

    fresh_watermark = int(
        (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y%m%d%H%M%S")
    )
    run = await ingestion_repository.create_ingestion_run(
        db_session, ingestion_repository.IngestionType.INCREMENTAL
    )
    await ingestion_repository.update_ingestion_run(
        db_session,
        run.id,
        ingestion_repository.IngestionStatus.COMPLETED,
        watermark_dateadded=fresh_watermark,
        events_ingested=100,
    )
    await db_session.commit()

    assert await ingestion_service.should_catchup_on_startup(db_session) is False


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
