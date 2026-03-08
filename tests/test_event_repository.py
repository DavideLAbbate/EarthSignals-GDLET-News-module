"""Tests for event repository."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

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
