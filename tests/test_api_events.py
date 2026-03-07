"""
End-to-end tests for POST /events/search.

Uses mocked BigQuery client and mocked Anthropic client.
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

SAMPLE_BQ_ROW = {
    "GLOBALEVENTID": 123456789,
    "SQLDATE": 20240315,
    "Actor1CountryCode": "ITA",
    "Actor2CountryCode": None,
    "EventCode": "141",
    "EventBaseCode": "141",
    "EventRootCode": "14",
    "QuadClass": 3,
    "GoldsteinScale": -3.8,
    "AvgTone": -2.4,
    "NumMentions": 5,
    "NumSources": 3,
    "NumArticles": 4,
    "ActionGeo_FullName": "Rome, Italy",
    "ActionGeo_CountryCode": "IT",
    "SOURCEURL": "https://www.ansa.it/article/example",
}


@pytest.fixture
def anthropic_mock_success(mock_anthropic_client):
    """Configure Anthropic mock to return a valid response."""
    mock_message = MagicMock()
    mock_message.content = [MagicMock(text=json.dumps(VALID_CLAUDE_RESPONSE))]
    mock_anthropic_client.messages.create = AsyncMock(return_value=mock_message)
    return mock_anthropic_client


@pytest.fixture
def bq_mock_with_results(mock_bq_client):
    """Configure BQ mock to return sample rows."""
    mock_bq_client.run_query = AsyncMock(return_value=[SAMPLE_BQ_ROW])
    return mock_bq_client


async def test_search_events_success(async_client, api_headers, anthropic_mock_success, bq_mock_with_results):
    """Valid request returns properly structured SearchResponse."""
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
    assert response.status_code == 200
    data = response.json()

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
    assert event["actor1_country"] == "ITA"
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


async def test_search_events_empty_bq_results(async_client, api_headers, anthropic_mock_success, mock_bq_client):
    """Request returning no BQ rows returns empty results array."""
    mock_bq_client.run_query = AsyncMock(return_value=[])
    response = await async_client.post(
        "/events/search",
        json={"country": "Italy", "date_range": {"from": 2023, "to": 2024}},
        headers=api_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["results"] == []
    assert data["metadata"]["total_results"] == 0


async def test_search_events_anthropic_unavailable(async_client, api_headers, mock_anthropic_client):
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
