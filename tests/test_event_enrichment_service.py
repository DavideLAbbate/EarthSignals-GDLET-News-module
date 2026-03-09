"""Tests for the event enrichment orchestration service."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select

from app.db.models import GdeltEvent
from app.db.repositories import event_repository
from app.schemas.event_enrichment import EventEnrichmentEntities, EventEnrichmentResponse
from app.services import event_enrichment_service


def _enriched_payload(**overrides: object) -> dict[str, object]:
    """Return a strict enrichment payload for service tests."""
    payload: dict[str, object] = {
        "article_title": "Enriched title",
        "article_summary": "Enriched summary",
        "cited_sources": ["Reuters", "AP"],
        "main_topics": ["Diplomacy", "Trade"],
        "keywords": ["summit", "sanctions"],
        "entities": {
            "persons_cited": ["Jane Doe"],
            "organizations_cited": ["United Nations"],
            "locations": ["Geneva"],
            "ethnicities_cited": ["Kurdish"],
            "religions_cited": ["Catholic"],
            "occupations_cited": ["diplomat"],
            "political_affiliations_cited": ["Labour"],
            "industries_cited": ["energy"],
            "products_cited": ["oil futures"],
            "brands_cited": ["Shell"],
        },
    }
    payload.update(overrides)
    return payload


@pytest.fixture
def enrichment_events() -> list[dict]:
    return [
        {
            "global_event_id": 2234567890123,
            "sql_date": 20260301,
            "date_added": 20260301000000,
            "actor1_country_code": "USA",
            "actor2_country_code": "CHN",
            "event_code": "042",
            "event_base_code": "04",
            "event_root_code": "04",
            "quad_class": 2,
            "goldstein_scale": 5.0,
            "avg_tone": -2.5,
            "num_mentions": 10,
            "num_sources": 5,
            "num_articles": 3,
            "action_geo_full_name": "Washington, DC",
            "action_geo_country_code": "US",
            "source_url": "https://example.com/article-1",
        },
        {
            "global_event_id": 2234567890124,
            "sql_date": 20260302,
            "date_added": 20260302000000,
            "actor1_country_code": "GBR",
            "actor2_country_code": "FRA",
            "event_code": "043",
            "event_base_code": "04",
            "event_root_code": "04",
            "quad_class": 2,
            "goldstein_scale": 3.0,
            "avg_tone": 1.0,
            "num_mentions": 8,
            "num_sources": 3,
            "num_articles": 2,
            "action_geo_full_name": "London, UK",
            "action_geo_country_code": "GB",
            "source_url": None,
        },
        {
            "global_event_id": 2234567890125,
            "sql_date": 20260303,
            "date_added": 20260303000000,
            "actor1_country_code": "DEU",
            "actor2_country_code": "UKR",
            "event_code": "190",
            "event_base_code": "19",
            "event_root_code": "19",
            "quad_class": 4,
            "goldstein_scale": -5.0,
            "avg_tone": -4.0,
            "num_mentions": 20,
            "num_sources": 7,
            "num_articles": 5,
            "action_geo_full_name": "Berlin, Germany",
            "action_geo_country_code": "DE",
            "source_url": "https://example.com/article-3",
        },
    ]


@pytest.mark.asyncio
async def test_run_event_enrichment_batch_processes_rows_independently(
    db_session,
    enrichment_events,
):
    """A batch should enrich successes and count persisted row failures as failed."""
    await event_repository.bulk_insert_events(db_session, enrichment_events)
    await db_session.commit()

    extract_mock = AsyncMock(
        side_effect=[
            {"title": "Extracted title", "content": "Extracted body"},
            RuntimeError("extract failed"),
        ]
    )
    enrich_mock = AsyncMock(return_value=_enriched_payload())

    with (
        patch.object(event_enrichment_service, "_extract_article_content", extract_mock),
        patch.object(event_enrichment_service, "_enrich_article_content", enrich_mock),
    ):
        summary = await event_enrichment_service.run_event_enrichment_batch(
            db_session, batch_size=10
        )

    assert summary == {"selected": 3, "enriched": 1, "failed": 2, "skipped": 0}

    result = await db_session.execute(select(GdeltEvent).order_by(GdeltEvent.global_event_id.asc()))
    events = {event.global_event_id: event for event in result.scalars().all()}

    assert events[2234567890123].enrichment_status == "enriched"
    assert events[2234567890123].article_title == "Enriched title"
    assert events[2234567890123].article_summary == "Enriched summary"
    assert events[2234567890123].cited_sources == ["Reuters", "AP"]
    assert events[2234567890123].main_topics == ["Diplomacy", "Trade"]
    assert events[2234567890123].keywords == ["summit", "sanctions"]
    assert events[2234567890123].entities == {
        "persons_cited": ["Jane Doe"],
        "organizations_cited": ["United Nations"],
        "locations": ["Geneva"],
        "ethnicities_cited": ["Kurdish"],
        "religions_cited": ["Catholic"],
        "occupations_cited": ["diplomat"],
        "political_affiliations_cited": ["Labour"],
        "industries_cited": ["energy"],
        "products_cited": ["oil futures"],
        "brands_cited": ["Shell"],
    }
    assert events[2234567890123].enriched_at is not None
    assert events[2234567890123].enrichment_error is None

    assert events[2234567890124].enrichment_status == "failed"
    assert events[2234567890124].enrichment_error == "missing source_url"

    assert events[2234567890125].enrichment_status == "failed"
    assert events[2234567890125].enrichment_error == "extract failed"


@pytest.mark.asyncio
async def test_run_event_enrichment_batch_counts_missing_source_url_as_failed(
    db_session,
    enrichment_events,
):
    """A row persisted as failed for a missing source URL should count as failed in the summary."""
    await event_repository.bulk_insert_events(db_session, [enrichment_events[1]])
    await db_session.commit()

    summary = await event_enrichment_service.run_event_enrichment_batch(db_session, batch_size=10)

    assert summary == {"selected": 1, "enriched": 0, "failed": 1, "skipped": 0}

    result = await db_session.execute(
        select(GdeltEvent).where(GdeltEvent.global_event_id == 2234567890124)
    )
    event = result.scalar_one()

    assert event.enrichment_status == "failed"
    assert event.enrichment_error == "missing source_url"


@pytest.mark.asyncio
async def test_run_event_enrichment_batch_logs_when_missing_source_url_failure_update_is_noop(
    db_session,
    enrichment_events,
):
    """A no-op missing-source failure update should be logged and leave the row retryable."""
    await event_repository.bulk_insert_events(db_session, [enrichment_events[1]])
    await db_session.commit()

    logger_mock = MagicMock()

    with (
        patch.object(event_enrichment_service, "logger", logger_mock),
        patch.object(
            event_repository,
            "mark_event_enrichment_failed",
            AsyncMock(return_value=False),
        ),
    ):
        summary = await event_enrichment_service.run_event_enrichment_batch(
            db_session, batch_size=10
        )

    assert summary == {"selected": 1, "enriched": 0, "failed": 0, "skipped": 1}
    logger_mock.error.assert_called_once_with(
        "event_enrichment_failure_persistence_failed",
        global_event_id=2234567890124,
        error_message="missing source_url",
        persistence_error="failure update returned no rows",
    )

    result = await db_session.execute(
        select(GdeltEvent).where(GdeltEvent.global_event_id == 2234567890124)
    )
    event = result.scalar_one()

    assert event.enrichment_status == "pending"
    assert event.enrichment_error is None


@pytest.mark.asyncio
async def test_run_event_enrichment_batch_only_selects_pending_rows(
    db_session,
    enrichment_events,
):
    """Only pending rows should be loaded into a batch."""
    processed_events = [
        {**enrichment_events[0], "global_event_id": 2234567890200, "enrichment_status": "enriched"},
        {**enrichment_events[1], "global_event_id": 2234567890201, "enrichment_status": "failed"},
        {
            **enrichment_events[2],
            "global_event_id": 2234567890202,
            "enrichment_status": "processing",
        },
        {**enrichment_events[0], "global_event_id": 2234567890203, "enrichment_status": "pending"},
    ]
    await event_repository.bulk_insert_events(db_session, processed_events)
    await db_session.commit()

    extract_mock = AsyncMock(return_value={"title": "Extracted title", "content": "Extracted body"})
    enrich_mock = AsyncMock(
        return_value=_enriched_payload(
            article_title="Done",
            article_summary="Done",
            cited_sources=[],
            main_topics=[],
            keywords=[],
            entities={
                "persons_cited": [],
                "organizations_cited": [],
                "locations": [],
                "ethnicities_cited": [],
                "religions_cited": [],
                "occupations_cited": [],
                "political_affiliations_cited": [],
                "industries_cited": [],
                "products_cited": [],
                "brands_cited": [],
            },
        )
    )

    with (
        patch.object(event_enrichment_service, "_extract_article_content", extract_mock),
        patch.object(event_enrichment_service, "_enrich_article_content", enrich_mock),
    ):
        summary = await event_enrichment_service.run_event_enrichment_batch(
            db_session, batch_size=10
        )

    assert summary == {"selected": 1, "enriched": 1, "failed": 0, "skipped": 0}
    extract_mock.assert_awaited_once()
    enrich_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_event_enrichment_batch_uses_real_extract_composition_path(
    db_session,
    enrichment_events,
):
    """The live path should fetch, extract, enrich, and persist semantic fields."""
    await event_repository.bulk_insert_events(db_session, [enrichment_events[0]])
    await db_session.commit()

    fetched_html = {
        "final_url": "https://example.com/final-article",
        "html": """
        <html>
          <head>
            <meta property="og:title" content=" Phase 3 Title ">
            <title>Ignored title</title>
          </head>
          <body>
            <p> First paragraph. </p>
            <p>Second paragraph.</p>
          </body>
        </html>
        """,
    }

    enrich_client_mock = AsyncMock(
        return_value=EventEnrichmentResponse(
            article_title="Semantic title",
            article_summary="Semantic summary",
            cited_sources=["Reuters", "AP"],
            main_topics=["Diplomacy", "Trade"],
            keywords=["summit", "sanctions"],
            entities=EventEnrichmentEntities(
                persons_cited=["Jane Doe"],
                organizations_cited=["United Nations"],
                locations=["Geneva"],
                ethnicities_cited=["Kurdish"],
                religions_cited=["Catholic"],
                occupations_cited=["diplomat"],
                political_affiliations_cited=["Labour"],
                industries_cited=["energy"],
                products_cited=["oil futures"],
                brands_cited=["Shell"],
            ),
        )
    )

    with (
        patch.object(
            event_enrichment_service, "fetch_article_html", AsyncMock(return_value=fetched_html)
        ),
        patch.object(
            event_enrichment_service,
            "enrich_article_content",
            enrich_client_mock,
            create=True,
        ),
    ):
        summary = await event_enrichment_service.run_event_enrichment_batch(
            db_session, batch_size=10
        )

    assert summary == {"selected": 1, "enriched": 1, "failed": 0, "skipped": 0}

    result = await db_session.execute(
        select(GdeltEvent).where(GdeltEvent.global_event_id == 2234567890123)
    )
    event = result.scalar_one()

    assert event.enrichment_status == "enriched"
    assert event.article_title == "Semantic title"
    assert event.article_summary == "Semantic summary"
    assert event.cited_sources == ["Reuters", "AP"]
    assert event.main_topics == ["Diplomacy", "Trade"]
    assert event.keywords == ["summit", "sanctions"]
    assert event.entities == {
        "persons_cited": ["Jane Doe"],
        "organizations_cited": ["United Nations"],
        "locations": ["Geneva"],
        "ethnicities_cited": ["Kurdish"],
        "religions_cited": ["Catholic"],
        "occupations_cited": ["diplomat"],
        "political_affiliations_cited": ["Labour"],
        "industries_cited": ["energy"],
        "products_cited": ["oil futures"],
        "brands_cited": ["Shell"],
    }
    enrich_client_mock.assert_awaited_once_with(
        {"title": "Phase 3 Title", "content": "First paragraph.\n\nSecond paragraph."}
    )


@pytest.mark.asyncio
async def test_run_event_enrichment_batch_continues_after_persistence_failure(
    db_session,
    enrichment_events,
):
    """A DB write failure for one row should not stop later rows in the batch."""
    pending_events = [enrichment_events[0], enrichment_events[2]]
    await event_repository.bulk_insert_events(db_session, pending_events)
    await db_session.commit()

    extract_mock = AsyncMock(
        side_effect=[
            {"title": "Extracted title 1", "content": "Extracted body 1"},
            {"title": "Extracted title 2", "content": "Extracted body 2"},
        ]
    )
    enrich_mock = AsyncMock(
        side_effect=[
            _enriched_payload(
                article_title="Enriched title 1",
                article_summary="Enriched summary 1",
                cited_sources=[],
            ),
            _enriched_payload(
                article_title="Enriched title 2",
                article_summary="Enriched summary 2",
                cited_sources=[],
            ),
        ]
    )

    original_mark_succeeded = event_repository.mark_event_enrichment_succeeded

    async def flaky_mark_succeeded(session, global_event_id, **kwargs):
        if global_event_id == 2234567890123:
            raise RuntimeError("write failed")
        return await original_mark_succeeded(session, global_event_id, **kwargs)

    with (
        patch.object(event_enrichment_service, "_extract_article_content", extract_mock),
        patch.object(event_enrichment_service, "_enrich_article_content", enrich_mock),
        patch.object(
            event_repository, "mark_event_enrichment_succeeded", side_effect=flaky_mark_succeeded
        ),
    ):
        summary = await event_enrichment_service.run_event_enrichment_batch(
            db_session, batch_size=10
        )

    assert summary == {"selected": 2, "enriched": 1, "failed": 1, "skipped": 0}

    result = await db_session.execute(select(GdeltEvent).order_by(GdeltEvent.global_event_id.asc()))
    events = {event.global_event_id: event for event in result.scalars().all()}

    assert events[2234567890123].enrichment_status == "failed"
    assert events[2234567890123].enrichment_error == "write failed"
    assert events[2234567890125].enrichment_status == "enriched"
    assert events[2234567890125].article_title == "Enriched title 2"


@pytest.mark.asyncio
async def test_run_event_enrichment_batch_does_not_count_false_success_update_as_enriched(
    db_session,
    enrichment_events,
):
    """A false-y success update should count as a row-level failure, not success."""
    pending_events = [enrichment_events[0], enrichment_events[2]]
    await event_repository.bulk_insert_events(db_session, pending_events)
    await db_session.commit()

    extract_mock = AsyncMock(
        side_effect=[
            {"title": "Extracted title 1", "content": "Extracted body 1"},
            {"title": "Extracted title 2", "content": "Extracted body 2"},
        ]
    )
    enrich_mock = AsyncMock(
        side_effect=[
            _enriched_payload(
                article_title="Enriched title 1",
                article_summary="Enriched summary 1",
                cited_sources=[],
            ),
            _enriched_payload(
                article_title="Enriched title 2",
                article_summary="Enriched summary 2",
                cited_sources=[],
            ),
        ]
    )

    original_mark_succeeded = event_repository.mark_event_enrichment_succeeded

    async def false_then_real_success(session, global_event_id, **kwargs):
        if global_event_id == 2234567890123:
            return False
        return await original_mark_succeeded(session, global_event_id, **kwargs)

    with (
        patch.object(event_enrichment_service, "_extract_article_content", extract_mock),
        patch.object(event_enrichment_service, "_enrich_article_content", enrich_mock),
        patch.object(
            event_repository,
            "mark_event_enrichment_succeeded",
            side_effect=false_then_real_success,
        ),
    ):
        summary = await event_enrichment_service.run_event_enrichment_batch(
            db_session, batch_size=10
        )

    assert summary == {"selected": 2, "enriched": 1, "failed": 1, "skipped": 0}

    result = await db_session.execute(select(GdeltEvent).order_by(GdeltEvent.global_event_id.asc()))
    events = {event.global_event_id: event for event in result.scalars().all()}

    assert events[2234567890123].enrichment_status == "failed"
    assert events[2234567890123].enrichment_error == "success update returned no rows"
    assert events[2234567890125].enrichment_status == "enriched"


@pytest.mark.asyncio
async def test_run_event_enrichment_batch_logs_when_failure_persistence_also_fails(
    db_session,
    enrichment_events,
):
    """A second persistence failure should be logged and later rows should continue."""
    pending_events = [enrichment_events[0], enrichment_events[2]]
    await event_repository.bulk_insert_events(db_session, pending_events)
    await db_session.commit()

    extract_mock = AsyncMock(
        side_effect=[
            RuntimeError("extract failed"),
            {"title": "Extracted title 2", "content": "Extracted body 2"},
        ]
    )
    enrich_mock = AsyncMock(
        return_value=_enriched_payload(
            article_title="Enriched title 2",
            article_summary="Enriched summary 2",
            cited_sources=[],
        )
    )
    logger_mock = MagicMock()

    original_mark_failed = event_repository.mark_event_enrichment_failed

    async def always_fail_first_row_mark_failed(session, global_event_id, **kwargs):
        if global_event_id == 2234567890123:
            raise SQLAlchemyError("failed status write failed")
        return await original_mark_failed(session, global_event_id, **kwargs)

    with (
        patch.object(event_enrichment_service, "_extract_article_content", extract_mock),
        patch.object(event_enrichment_service, "_enrich_article_content", enrich_mock),
        patch.object(event_enrichment_service, "logger", logger_mock),
        patch.object(
            event_repository,
            "mark_event_enrichment_failed",
            side_effect=always_fail_first_row_mark_failed,
        ),
    ):
        summary = await event_enrichment_service.run_event_enrichment_batch(
            db_session, batch_size=10
        )

    assert summary == {"selected": 2, "enriched": 1, "failed": 0, "skipped": 1}
    logger_mock.error.assert_called_once_with(
        "event_enrichment_failure_persistence_failed",
        global_event_id=2234567890123,
        error_message="extract failed",
        persistence_error="failed status write failed",
    )

    result = await db_session.execute(select(GdeltEvent).order_by(GdeltEvent.global_event_id.asc()))
    events = {event.global_event_id: event for event in result.scalars().all()}

    assert events[2234567890123].enrichment_status == "pending"
    assert events[2234567890125].enrichment_status == "enriched"


@pytest.mark.asyncio
async def test_run_event_enrichment_batch_logs_when_failure_update_returns_no_rows(
    db_session,
    enrichment_events,
):
    """A no-op failure update after row processing should be logged and later rows should continue."""
    pending_events = [enrichment_events[0], enrichment_events[2]]
    await event_repository.bulk_insert_events(db_session, pending_events)
    await db_session.commit()

    extract_mock = AsyncMock(
        side_effect=[
            RuntimeError("extract failed"),
            {"title": "Extracted title 2", "content": "Extracted body 2"},
        ]
    )
    enrich_mock = AsyncMock(
        return_value=_enriched_payload(
            article_title="Enriched title 2",
            article_summary="Enriched summary 2",
            cited_sources=[],
        )
    )
    logger_mock = MagicMock()

    original_mark_failed = event_repository.mark_event_enrichment_failed

    async def noop_first_row_mark_failed(session, global_event_id, **kwargs):
        if global_event_id == 2234567890123:
            return False
        return await original_mark_failed(session, global_event_id, **kwargs)

    with (
        patch.object(event_enrichment_service, "_extract_article_content", extract_mock),
        patch.object(event_enrichment_service, "_enrich_article_content", enrich_mock),
        patch.object(event_enrichment_service, "logger", logger_mock),
        patch.object(
            event_repository,
            "mark_event_enrichment_failed",
            side_effect=noop_first_row_mark_failed,
        ),
    ):
        summary = await event_enrichment_service.run_event_enrichment_batch(
            db_session, batch_size=10
        )

    assert summary == {"selected": 2, "enriched": 1, "failed": 0, "skipped": 1}
    logger_mock.error.assert_called_once_with(
        "event_enrichment_failure_persistence_failed",
        global_event_id=2234567890123,
        error_message="extract failed",
        persistence_error="failure update returned no rows",
    )

    result = await db_session.execute(select(GdeltEvent).order_by(GdeltEvent.global_event_id.asc()))
    events = {event.global_event_id: event for event in result.scalars().all()}

    assert events[2234567890123].enrichment_status == "pending"
    assert events[2234567890125].enrichment_status == "enriched"


@pytest.mark.asyncio
async def test_run_event_enrichment_batch_keeps_rows_retryable_after_success_persistence_failure(
    db_session,
    enrichment_events,
):
    """A row should roll back to pending if final success persistence cannot be recovered."""
    await event_repository.bulk_insert_events(db_session, [enrichment_events[0]])
    await db_session.commit()

    extract_mock = AsyncMock(return_value={"title": "Extracted title", "content": "Extracted body"})
    enrich_mock = AsyncMock(return_value=_enriched_payload())

    with (
        patch.object(event_enrichment_service, "_extract_article_content", extract_mock),
        patch.object(event_enrichment_service, "_enrich_article_content", enrich_mock),
        patch.object(
            event_repository,
            "mark_event_enrichment_succeeded",
            side_effect=RuntimeError("write failed"),
        ),
        patch.object(
            event_repository,
            "mark_event_enrichment_failed",
            side_effect=RuntimeError("failed status write failed"),
        ),
    ):
        summary = await event_enrichment_service.run_event_enrichment_batch(
            db_session, batch_size=10
        )

    assert summary == {"selected": 1, "enriched": 0, "failed": 0, "skipped": 1}

    result = await db_session.execute(
        select(GdeltEvent).where(GdeltEvent.global_event_id == 2234567890123)
    )
    event = result.scalar_one()

    assert event.enrichment_status == "pending"
    assert event.enrichment_error is None
