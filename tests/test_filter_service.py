"""
Tests for the filter normalization service.

Covers: valid normalization, cache hit, cache miss, malformed Claude response,
missing fields, and Claude timeout (AnthropicUnavailableError).
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.core.exceptions import AnthropicUnavailableError, FilterInterpretationError
from app.schemas.filters import DateRange, RawFilterInput
from app.services.filter_service import normalize_filters


VALID_CLAUDE_RESPONSE = json.dumps(
    {
        "cameo_country_code": "ITA",
        "fips_country_code": "IT",
        "event_root_codes": ["14"],
        "event_base_codes": ["141", "143"],
        "date_from_sqldate": 20180101,
        "date_to_sqldate": 20241231,
        "normalization_notes": "Italy protest events about energy policy",
    }
)


def _make_anthropic_mock(response_text: str):
    """Build a mock Anthropic client that returns the given text."""
    mock_message = MagicMock()
    mock_message.content = [MagicMock(text=response_text)]
    client = AsyncMock()
    client.messages.create = AsyncMock(return_value=mock_message)
    return client


async def test_normalize_filters_valid(db_session):
    """Valid filters are normalized correctly."""
    raw = RawFilterInput(
        country="Italy",
        event_type="protest",
        macro_topic="energy",
        date_range=DateRange(**{"from": 2018, "to": 2024}),
    )
    client = _make_anthropic_mock(VALID_CLAUDE_RESPONSE)

    result = await normalize_filters(raw, db_session, client)

    assert result.cameo_country_code == "ITA"
    assert result.fips_country_code == "IT"
    assert "14" in result.event_root_codes
    assert result.date_from_sqldate == 20180101
    assert result.date_to_sqldate == 20241231


async def test_normalize_filters_empty_raises(db_session):
    """No filter fields → FilterInterpretationError."""
    raw = RawFilterInput()
    client = AsyncMock()

    with pytest.raises(FilterInterpretationError, match="At least one filter field"):
        await normalize_filters(raw, db_session, client)


async def test_normalize_filters_malformed_json(db_session):
    """Malformed Claude JSON → FilterInterpretationError."""
    raw = RawFilterInput(country="Germany")
    client = _make_anthropic_mock("This is not JSON at all!!!")

    with pytest.raises(FilterInterpretationError):
        await normalize_filters(raw, db_session, client)


async def test_normalize_filters_missing_required_field(db_session):
    """Claude response missing required field → FilterInterpretationError."""
    # date_from_sqldate is required — omit it
    partial_response = json.dumps(
        {
            "cameo_country_code": "DEU",
            "fips_country_code": "GM",
            "event_root_codes": ["14"],
            "event_base_codes": [],
            # Missing date_from_sqldate and date_to_sqldate
            "normalization_notes": "test",
        }
    )
    raw = RawFilterInput(country="Germany")
    client = _make_anthropic_mock(partial_response)

    with pytest.raises(FilterInterpretationError):
        await normalize_filters(raw, db_session, client)


async def test_normalize_filters_cache_hit(db_session):
    """Second call with same filters uses cache, not Claude."""
    raw = RawFilterInput(country="France")
    client = _make_anthropic_mock(
        json.dumps(
            {
                "cameo_country_code": "FRA",
                "fips_country_code": "FR",
                "event_root_codes": ["14"],
                "event_base_codes": [],
                "date_from_sqldate": 20150101,
                "date_to_sqldate": 20261231,
                "normalization_notes": "France",
            }
        )
    )

    # First call — hits Claude
    result1 = await normalize_filters(raw, db_session, client)
    assert client.messages.create.call_count == 1

    # Second call — should use cache
    result2 = await normalize_filters(raw, db_session, client)
    # Claude should not be called again
    assert client.messages.create.call_count == 1

    assert result1.fips_country_code == result2.fips_country_code


async def test_normalize_filters_anthropic_unavailable_propagates(db_session):
    """AnthropicUnavailableError from interpreter propagates correctly."""
    raw = RawFilterInput(country="Syria")
    client = AsyncMock()
    client.messages.create = AsyncMock(side_effect=AnthropicUnavailableError("Anthropic API down"))

    with pytest.raises(AnthropicUnavailableError):
        await normalize_filters(raw, db_session, client)


async def test_normalize_structured_filters_without_claude(db_session):
    """Structured filters should bypass Claude and normalize locally."""
    raw = RawFilterInput(
        countries=["Italy", "France"],
        date_range=DateRange(**{"from": 2023, "to": 2024}),
        sentiment={
            "tone_min": -5,
            "goldstein_max": 2,
        },
        impact={
            "min_mentions": 10,
            "min_sources": 2,
        },
        actors={
            "actor1_country": "USA",
            "actor2_country": "Italy",
        },
        source={"domains": ["https://www.ansa.it", "reuters.com"]},
        event_codes={
            "root_codes": ["14"],
            "base_codes": ["141"],
            "full_codes": ["1411"],
        },
        quad_classes=[3, 4],
    )
    client = AsyncMock()

    result = await normalize_filters(raw, db_session, client)

    assert result.geo_country_codes == ["FR", "IT"]
    assert result.actor1_country_code == "USA"
    assert result.actor2_country_code == "ITA"
    assert result.source_domains == ["ansa.it", "reuters.com"]
    assert result.event_root_codes == ["14"]
    assert result.event_base_codes == ["141"]
    assert result.event_codes == ["1411"]
    assert result.quad_classes == [3, 4]
    assert result.tone_min == -5
    assert result.goldstein_max == 2
    assert result.min_mentions == 10
    assert result.min_sources == 2
    assert result.date_from_sqldate == 20230101
    assert result.date_to_sqldate == 20241231
    assert client.messages.create.call_count == 0
