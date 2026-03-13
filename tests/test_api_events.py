"""
End-to-end tests for POST /events/search.

Uses mocked Anthropic client and PostgreSQL database.
Verifies JSON response structure, auth behavior, and error handling.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest


VALID_CLAUDE_RESPONSE = {
    "cameo_country_code": "ITA",
    "fips_country_code": "IT",
    "event_root_codes": ["14"],
    "event_base_codes": ["141"],
    "date_from_sqldate": 20180101,
    "date_to_sqldate": 20241231,
    "normalization_notes": "Italy protest events",
}

SAMPLE_EVENT = {
    "global_event_id": 123456789,
    "sql_date": 20240315,
    "date_added": 20240315000000,
    "actor1_country_code": "USA",  # Matches the actor1_country filter
    "actor2_country_code": None,
    "event_code": "141",
    "event_base_code": "141",
    "event_root_code": "14",
    "quad_class": 3,
    "goldstein_scale": -3.8,
    "avg_tone": -2.4,
    "num_mentions": 5,
    "num_sources": 3,
    "num_articles": 4,
    "action_geo_full_name": "Rome, Italy",
    "action_geo_country_code": "IT",  # Matches the country normalization
    "source_url": "https://www.ansa.it/article/example",
}


@pytest.fixture
def anthropic_mock_success(mock_anthropic_client):
    """Configure Anthropic mock to return a valid response."""
    mock_message = MagicMock()
    mock_message.content = [MagicMock(text=json.dumps(VALID_CLAUDE_RESPONSE))]
    mock_anthropic_client.messages.create = AsyncMock(return_value=mock_message)
    return mock_anthropic_client


@pytest.fixture
async def db_with_events(db_session):
    """Insert sample events into the test database."""
    from app.db.repositories import event_repository

    await event_repository.bulk_insert_events(db_session, [SAMPLE_EVENT])
    await db_session.commit()
    return db_session


async def test_search_events_success(async_client, api_headers, anthropic_mock_success, db_session):
    """Valid request returns properly structured SearchResponse."""
    # Insert test data into database
    from app.db.repositories import event_repository

    result = await event_repository.bulk_insert_events(db_session, [SAMPLE_EVENT])
    await db_session.commit()
    print(f"\n[DEBUG] Inserted {result} events")

    # Verify data is there
    count = await event_repository.get_event_count(db_session)
    print(f"[DEBUG] Event count: {count}")

    response = await async_client.post(
        "/events/search",
        json={
            "country": "Italy",
            "event_type": "protest",
            "macro_topic": "energy",
            "date_range": {"from": 2018, "to": 2024},
            "sentiment": {"tone_min": -5},
            "impact": {"min_mentions": 3},
            "actors": {"actor1_country": "USA"},
            "source": {"domains": ["ansa.it"]},
            "event_codes": {"full_codes": ["141"]},
            "quad_classes": [3],
        },
        headers=api_headers,
    )
    print(f"[DEBUG] Response status: {response.status_code}")
    data = response.json()
    print(f"[DEBUG] Response data: {data}")

    # Check top-level structure
    assert "filters_received" in data
    assert "filters_normalized" in data
    assert "results" in data
    assert "metadata" in data

    # Check filters_received
    assert data["filters_received"]["country"] == "Italy"

    # Check filters_normalized
    normalized = data["filters_normalized"]
    assert normalized["cameo_country_code"] == "ITA"
    assert normalized["fips_country_code"] == "IT"
    assert normalized["geo_country_codes"] == ["IT"]
    assert normalized["actor1_country_code"] == "USA"
    assert normalized["source_domains"] == ["ansa.it"]
    assert normalized["event_codes"] == ["141"]
    assert normalized["quad_classes"] == [3]
    assert normalized["tone_min"] == -5
    assert normalized["min_mentions"] == 3
    assert "14" in normalized["event_root_codes"]

    # Check results
    assert len(data["results"]) == 1
    event = data["results"][0]
    assert event["event_id"] == "123456789"
    assert event["date"] == "2024-03-15"
    assert event["actor1_country"] == "USA"  # Matches the filter
    assert event["source_name"] == "ansa.it"
    assert event["tone"] == -2.4
    assert event["goldstein_scale"] == -3.8

    # Check metadata
    assert data["metadata"]["total_results"] == 1
    assert "query_time_ms" in data["metadata"]


async def test_search_events_missing_api_key(async_client, anthropic_mock_success):
    """Request without API key returns 401."""
    response = await async_client.post(
        "/events/search",
        json={"country": "Italy"},
    )
    assert response.status_code == 401


async def test_search_events_invalid_api_key(async_client, anthropic_mock_success):
    """Request with wrong API key returns 403."""
    response = await async_client.post(
        "/events/search",
        json={"country": "Italy"},
        headers={"X-API-Key": "wrong-key"},
    )
    assert response.status_code == 403


async def test_search_events_empty_filters(async_client, api_headers, mock_anthropic_client):
    """Request with no filter fields returns 422."""
    response = await async_client.post(
        "/events/search",
        json={},
        headers=api_headers,
    )
    assert response.status_code == 422


async def test_search_events_empty_results(async_client, api_headers, anthropic_mock_success):
    """Request returning no local DB rows returns empty results array."""
    response = await async_client.post(
        "/events/search",
        json={"country": "Italy", "date_range": {"from": 2023, "to": 2024}},
        headers=api_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["results"] == []
    assert data["metadata"]["total_results"] == 0


async def test_search_events_anthropic_unavailable(
    async_client, api_headers, mock_anthropic_client
):
    """Anthropic unavailable returns 503 with Retry-After header."""
    import anthropic

    mock_anthropic_client.messages.create = AsyncMock(
        side_effect=anthropic.APITimeoutError(request=MagicMock())
    )

    response = await async_client.post(
        "/events/search",
        json={"country": "Italy"},
        headers=api_headers,
    )
    assert response.status_code == 503
    assert "Retry-After" in response.headers
