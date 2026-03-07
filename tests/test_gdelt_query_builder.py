"""
Tests for the GDELT query builder.

Verifies SQL generation correctness and the MAX_BQ_SCAN_DAYS guard.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/fake-key.json")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-test")
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///./test.db")
os.environ.setdefault("API_KEY", "test-api-key")

from app.core.exceptions import QueryValidationError
from app.integrations.gdelt_query_builder import build_events_query


def test_build_events_query_basic():
    """Query with only date range generates valid SQL."""
    sql, params = build_events_query(
        date_from_sqldate=20230101,
        date_to_sqldate=20231231,
    )
    assert "SQLDATE >= @date_from" in sql
    assert "SQLDATE <= @date_to" in sql
    assert "gdelt-bq.gdeltv2.events" in sql
    assert "SELECT" in sql
    assert "GLOBALEVENTID" in sql
    assert "SOURCEURL" in sql
    # Should NOT contain optional WHERE filters
    assert "ActionGeo_CountryCode = @fips_country_code" not in sql
    assert "EventRootCode IN UNNEST" not in sql


def test_build_events_query_with_country():
    """Query with FIPS country code includes geographic filter."""
    sql, params = build_events_query(
        date_from_sqldate=20230101,
        date_to_sqldate=20231231,
        fips_country_code="IT",
    )
    assert "ActionGeo_CountryCode IN UNNEST(@geo_country_codes)" in sql
    param_names = [p.name for p in params]
    assert "geo_country_codes" in param_names


def test_build_events_query_with_event_codes():
    """Query with event root codes includes UNNEST filter."""
    sql, params = build_events_query(
        date_from_sqldate=20230101,
        date_to_sqldate=20231231,
        event_root_codes=["14", "19"],
    )
    assert "EventRootCode IN UNNEST(@event_root_codes)" in sql
    param_names = [p.name for p in params]
    assert "event_root_codes" in param_names


def test_build_events_query_date_from_gt_to_raises():
    """date_from > date_to raises QueryValidationError."""
    with pytest.raises(QueryValidationError, match="must be <="):
        build_events_query(
            date_from_sqldate=20241231,
            date_to_sqldate=20230101,
        )


def test_build_events_query_max_scan_days_guard():
    """Date range exceeding MAX_BQ_SCAN_DAYS raises QueryValidationError."""
    with patch("app.integrations.gdelt_query_builder.get_settings") as mock_settings:
        mock_settings.return_value.max_bq_scan_days = 30
        mock_settings.return_value.bq_max_results = 500
        with pytest.raises(QueryValidationError, match="exceeds the maximum"):
            build_events_query(
                date_from_sqldate=20200101,
                date_to_sqldate=20241231,
            )


def test_build_events_query_full():
    """Full query with all filters generates complete SQL."""
    sql, params = build_events_query(
        date_from_sqldate=20180101,
        date_to_sqldate=20241231,
        fips_country_code="IT",
        geo_country_codes=["FR"],
        cameo_country_code="ITA",
        actor1_country_code="USA",
        actor2_country_code="ITA",
        event_root_codes=["14"],
        event_base_codes=["141", "143"],
        event_codes=["1411"],
        quad_classes=[3, 4],
        source_domains=["ansa.it", "reuters.com"],
        tone_min=-5,
        tone_max=1,
        goldstein_min=-10,
        goldstein_max=2,
        min_mentions=10,
        min_sources=2,
        min_articles=4,
    )
    assert "ActionGeo_CountryCode IN UNNEST(@geo_country_codes)" in sql
    assert "Actor1CountryCode = @cameo_country_code" in sql
    assert "Actor1CountryCode = @actor1_country_code" in sql
    assert "Actor2CountryCode = @actor2_country_code" in sql
    assert "EventRootCode IN UNNEST(@event_root_codes)" in sql
    assert "EventBaseCode IN UNNEST(@event_base_codes)" in sql
    assert "EventCode IN UNNEST(@event_codes)" in sql
    assert "QuadClass IN UNNEST(@quad_classes)" in sql
    assert "AvgTone >= @tone_min" in sql
    assert "AvgTone <= @tone_max" in sql
    assert "GoldsteinScale >= @goldstein_min" in sql
    assert "GoldsteinScale <= @goldstein_max" in sql
    assert "NumMentions >= @min_mentions" in sql
    assert "NumSources >= @min_sources" in sql
    assert "NumArticles >= @min_articles" in sql
    assert "source_domains" in sql
    assert "ORDER BY SQLDATE DESC" in sql
    assert "LIMIT @max_results" in sql


def test_build_events_query_with_structured_filters():
    """Structured filters are translated to the correct SQL predicates."""
    sql, params = build_events_query(
        date_from_sqldate=20230101,
        date_to_sqldate=20231231,
        geo_country_codes=["IT", "FR"],
        actor1_country_code="USA",
        source_domains=["ansa.it"],
        quad_classes=[3],
        tone_min=-2.5,
        min_articles=3,
    )
    assert "ActionGeo_CountryCode IN UNNEST(@geo_country_codes)" in sql
    assert "Actor1CountryCode = @actor1_country_code" in sql
    assert "QuadClass IN UNNEST(@quad_classes)" in sql
    assert "AvgTone >= @tone_min" in sql
    assert "NumArticles >= @min_articles" in sql
    assert "source_domains" in sql

    param_names = [p.name for p in params]
    assert "geo_country_codes" in param_names
    assert "actor1_country_code" in param_names
    assert "quad_classes" in param_names
    assert "tone_min" in param_names
    assert "min_articles" in param_names
    assert "source_domains" in param_names


def test_build_events_query_never_select_star():
    """Query never uses SELECT *."""
    sql, _ = build_events_query(
        date_from_sqldate=20230101,
        date_to_sqldate=20231231,
    )
    assert "SELECT *" not in sql
    assert "SELECT\n    GLOBALEVENTID" in sql or "SELECT\n" in sql
