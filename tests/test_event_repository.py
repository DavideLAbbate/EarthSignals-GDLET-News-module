"""Tests for event repository."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from app.db.models import GdeltEvent
from app.db.repositories import event_repository


@pytest.fixture
def sample_events() -> list[dict]:
    return [
        {
            "global_event_id": 1234567890123,
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
            "source_url": "https://example.com/article1",
        },
        {
            "global_event_id": 1234567890124,
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
            "source_url": "https://example.com/article2",
        },
    ]


@pytest.mark.asyncio
async def test_bulk_insert_events(db_session, sample_events):
    """Test bulk insert of events."""
    inserted = await event_repository.bulk_insert_events(
        db_session,
        sample_events,
    )
    await db_session.commit()

    assert inserted == 2

    # Verify events were inserted
    count = await event_repository.get_event_count(db_session)
    assert count == 2


@pytest.mark.asyncio
async def test_bulk_insert_deduplication(db_session, sample_events):
    """Test that duplicate events are not inserted."""
    # Insert first batch
    await event_repository.bulk_insert_events(db_session, sample_events)
    await db_session.commit()

    # Try to insert same events again
    inserted = await event_repository.bulk_insert_events(
        db_session,
        sample_events,
    )
    await db_session.commit()

    # Should be 0 since duplicates are ignored
    assert inserted == 0

    # Count should still be 2
    count = await event_repository.get_event_count(db_session)
    assert count == 2


@pytest.mark.asyncio
async def test_get_latest_watermark(db_session, sample_events):
    """Test getting the latest watermark."""
    await event_repository.bulk_insert_events(db_session, sample_events)
    await db_session.commit()

    watermark = await event_repository.get_latest_watermark(db_session)
    assert watermark == 20260302000000


@pytest.mark.asyncio
async def test_get_latest_watermark_empty(db_session):
    """Test watermark when no events exist."""
    watermark = await event_repository.get_latest_watermark(db_session)
    assert watermark is None


@pytest.mark.asyncio
async def test_delete_events_before(db_session, sample_events):
    """Test deletion of events older than cutoff."""
    await event_repository.bulk_insert_events(db_session, sample_events)
    await db_session.commit()

    # Delete events before 20260302 (should delete first event)
    deleted = await event_repository.delete_events_before(
        db_session,
        20260302,
    )
    await db_session.commit()

    assert deleted == 1

    # Should have one event left
    count = await event_repository.get_event_count(db_session)
    assert count == 1


@pytest.mark.asyncio
async def test_get_event_count(db_session, sample_events):
    """Test getting total event count."""
    await event_repository.bulk_insert_events(db_session, sample_events)
    await db_session.commit()

    count = await event_repository.get_event_count(db_session)
    assert count == 2


@pytest.mark.asyncio
async def test_bulk_insert_events_chunks_large_batches(db_session):
    """Large batches should be split into multiple insert statements."""
    large_batch = [
        {
            "global_event_id": 1234567800000 + index,
            "sql_date": 20260301,
            "date_added": 20260301000000 + index,
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
            "action_geo_full_name": f"Location {index}",
            "action_geo_country_code": "US",
            "source_url": f"https://example.com/article-{index}",
        }
        for index in range(2000)
    ]

    with patch.object(
        db_session, "execute", new=AsyncMock(wraps=db_session.execute)
    ) as execute_spy:
        inserted = await event_repository.bulk_insert_events(db_session, large_batch)

    await db_session.commit()

    assert inserted == 2000
    assert execute_spy.await_count > 1

    count = await event_repository.get_event_count(db_session)
    assert count == 2000


def test_gdelt_event_model_includes_enrichment_fields():
    """The event model should expose the Phase 1 enrichment columns."""
    expected_columns = {
        "article_title",
        "article_summary",
        "sources",
        "enrichment_status",
        "enriched_at",
        "enrichment_error",
    }

    assert expected_columns.issubset(GdeltEvent.__table__.columns.keys())


@pytest.mark.asyncio
async def test_bulk_insert_events_sets_default_enrichment_state(db_session, sample_events):
    """New events should default to a pending enrichment state."""
    inserted = await event_repository.bulk_insert_events(db_session, [sample_events[0]])
    await db_session.commit()

    result = await db_session.execute(
        select(GdeltEvent).where(GdeltEvent.global_event_id == sample_events[0]["global_event_id"])
    )
    event = result.scalar_one()

    assert inserted == 1
    assert event.article_title is None
    assert event.article_summary is None
    assert event.sources is None
    assert event.enrichment_status == "pending"
    assert event.enriched_at is None
    assert event.enrichment_error is None


@pytest.mark.asyncio
async def test_get_pending_enrichment_candidates_orders_rows_deterministically(
    db_session,
    sample_events,
):
    """Pending candidates should be selected oldest-first with a stable tie-breaker."""
    pending_events = [
        sample_events[1],
        {
            **sample_events[0],
            "global_event_id": 1234567890125,
            "date_added": 20260301000000,
            "source_url": "https://example.com/article3",
        },
        {
            **sample_events[0],
            "global_event_id": 1234567890122,
            "date_added": 20260301000000,
            "source_url": "https://example.com/article0",
        },
    ]
    await event_repository.bulk_insert_events(db_session, pending_events)
    await event_repository.bulk_insert_events(
        db_session,
        [
            {
                **sample_events[0],
                "global_event_id": 1234567890999,
                "date_added": 20260228000000,
                "source_url": "https://example.com/already-enriched",
                "enrichment_status": "enriched",
            }
        ],
    )
    await db_session.commit()

    selected = await event_repository.get_pending_enrichment_candidates(db_session, limit=3)

    assert [event.global_event_id for event in selected] == [
        1234567890122,
        1234567890125,
        1234567890124,
    ]


@pytest.mark.asyncio
async def test_enrichment_state_transition_helpers_persist_expected_fields(
    db_session, sample_events
):
    """Repository helpers should persist processing, success, and failure states."""
    failure_event = {
        **sample_events[1],
        "article_title": "Existing title",
        "article_summary": "Existing summary",
        "sources": [{"name": "AP", "url": "https://example.com/ap"}],
    }
    await event_repository.bulk_insert_events(db_session, [sample_events[0]])
    await event_repository.bulk_insert_events(db_session, [failure_event])
    await db_session.commit()

    await event_repository.mark_event_enrichment_processing(
        db_session,
        sample_events[0]["global_event_id"],
    )
    await db_session.commit()

    result = await db_session.execute(
        select(GdeltEvent).where(GdeltEvent.global_event_id == sample_events[0]["global_event_id"])
    )
    processing_event = result.scalar_one()

    assert processing_event.enrichment_status == "processing"
    assert processing_event.enrichment_error is None

    enriched_at = datetime(2026, 3, 9, 12, 0, tzinfo=timezone.utc)
    await event_repository.mark_event_enrichment_succeeded(
        db_session,
        sample_events[0]["global_event_id"],
        article_title="Resolved title",
        article_summary="Resolved summary",
        sources=[{"name": "Reuters", "url": "https://example.com/source"}],
        enriched_at=enriched_at,
    )
    await db_session.commit()

    result = await db_session.execute(
        select(GdeltEvent).where(GdeltEvent.global_event_id == sample_events[0]["global_event_id"])
    )
    enriched_event = result.scalar_one()

    assert enriched_event.article_title == "Resolved title"
    assert enriched_event.article_summary == "Resolved summary"
    assert enriched_event.sources == [{"name": "Reuters", "url": "https://example.com/source"}]
    assert enriched_event.enrichment_status == "enriched"
    assert enriched_event.enriched_at == enriched_at
    assert enriched_event.enrichment_error is None

    await event_repository.mark_event_enrichment_processing(
        db_session,
        failure_event["global_event_id"],
    )
    await db_session.commit()

    await event_repository.mark_event_enrichment_failed(
        db_session,
        failure_event["global_event_id"],
        error_message="upstream timeout",
    )
    await db_session.commit()

    result = await db_session.execute(
        select(GdeltEvent).where(GdeltEvent.global_event_id == failure_event["global_event_id"])
    )
    failed_event = result.scalar_one()

    assert failed_event.article_title == "Existing title"
    assert failed_event.article_summary == "Existing summary"
    assert failed_event.sources == [{"name": "AP", "url": "https://example.com/ap"}]
    assert failed_event.enrichment_status == "failed"
    assert failed_event.enrichment_error == "upstream timeout"
