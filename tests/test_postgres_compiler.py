"""Tests for PostgresQueryCompiler."""

from __future__ import annotations

import pytest

from app.schemas.filters import NormalizedFilters
from app.integrations.postgres_compiler import PostgresQueryCompiler


@pytest.fixture
def compiler():
    return PostgresQueryCompiler()


def test_compile_empty_filters(compiler):
    """Test compiling with no filters returns all events."""
    filters = NormalizedFilters(
        date_from_sqldate=20200101,
        date_to_sqldate=20251231,
    )

    stmt = compiler.compile(filters, limit=10, offset=0)

    # Should have SELECT and ORDER BY
    query_str = str(stmt.compile(compile_kwargs={"literal_binds": True}))
    assert "SELECT" in query_str
    assert "gdelt_events" in query_str
    assert "ORDER BY" in query_str


def test_compile_with_date_range(compiler):
    """Test compiling with date range filter."""
    filters = NormalizedFilters(
        date_from_sqldate=20260301,
        date_to_sqldate=20260308,
    )

    stmt = compiler.compile(filters, limit=10, offset=0)
    query_str = str(stmt.compile(compile_kwargs={"literal_binds": True}))

    assert "sql_date" in query_str
    assert "20260301" in query_str
    assert "20260308" in query_str


def test_compile_with_actor1_country_filter(compiler):
    """Test compiling with actor1 country filter."""
    filters = NormalizedFilters(
        date_from_sqldate=20200101,
        date_to_sqldate=20251231,
        actor1_country_code="USA",
    )

    stmt = compiler.compile(filters, limit=10, offset=0)
    query_str = str(stmt.compile(compile_kwargs={"literal_binds": True}))

    assert "actor1_country_code" in query_str
    assert "USA" in query_str


def test_compile_with_actor2_country_filter(compiler):
    """Test compiling with actor2 country filter."""
    filters = NormalizedFilters(
        date_from_sqldate=20200101,
        date_to_sqldate=20251231,
        actor2_country_code="FRA",
    )

    stmt = compiler.compile(filters, limit=10, offset=0)
    query_str = str(stmt.compile(compile_kwargs={"literal_binds": True}))

    assert "actor2_country_code" in query_str


def test_compile_with_geo_country_filter(compiler):
    """Test compiling with action geo country filter."""
    filters = NormalizedFilters(
        date_from_sqldate=20200101,
        date_to_sqldate=20251231,
        geo_country_codes=["US", "GB"],
    )

    stmt = compiler.compile(filters, limit=10, offset=0)
    query_str = str(stmt.compile(compile_kwargs={"literal_binds": True}))

    assert "action_geo_country_code" in query_str


def test_compile_with_event_codes(compiler):
    """Test compiling with event codes."""
    filters = NormalizedFilters(
        date_from_sqldate=20200101,
        date_to_sqldate=20251231,
        event_codes=["042", "043"],
    )

    stmt = compiler.compile(filters, limit=10, offset=0)
    query_str = str(stmt.compile(compile_kwargs={"literal_binds": True}))

    assert "event_code" in query_str


def test_compile_with_event_root_codes(compiler):
    """Test compiling with event root codes."""
    filters = NormalizedFilters(
        date_from_sqldate=20200101,
        date_to_sqldate=20251231,
        event_root_codes=["04", "05"],
    )

    stmt = compiler.compile(filters, limit=10, offset=0)
    query_str = str(stmt.compile(compile_kwargs={"literal_binds": True}))

    assert "event_root_code" in query_str


def test_compile_with_goldstein_range(compiler):
    """Test compiling with Goldstein scale range."""
    filters = NormalizedFilters(
        date_from_sqldate=20200101,
        date_to_sqldate=20251231,
        goldstein_min=-5.0,
        goldstein_max=5.0,
    )

    stmt = compiler.compile(filters, limit=10, offset=0)
    query_str = str(stmt.compile(compile_kwargs={"literal_binds": True}))

    assert "goldstein_scale" in query_str


def test_compile_with_tone_range(compiler):
    """Test compiling with tone (sentiment) range."""
    filters = NormalizedFilters(
        date_from_sqldate=20200101,
        date_to_sqldate=20251231,
        tone_min=-10.0,
        tone_max=10.0,
    )

    stmt = compiler.compile(filters, limit=10, offset=0)
    query_str = str(stmt.compile(compile_kwargs={"literal_binds": True}))

    assert "avg_tone" in query_str


def test_compile_count(compiler):
    """Test compiling a count query."""
    filters = NormalizedFilters(
        date_from_sqldate=20200101,
        date_to_sqldate=20251231,
        actor1_country_code="USA",
    )

    stmt = compiler.compile_count(filters)
    query_str = str(stmt.compile(compile_kwargs={"literal_binds": True}))

    assert "count" in query_str.lower()


def test_compile_with_min_thresholds(compiler):
    """Test compiling with min thresholds for mentions/sources/articles."""
    filters = NormalizedFilters(
        date_from_sqldate=20200101,
        date_to_sqldate=20251231,
        min_mentions=5,
        min_sources=2,
        min_articles=1,
    )

    stmt = compiler.compile(filters, limit=10, offset=0)
    query_str = str(stmt.compile(compile_kwargs={"literal_binds": True}))

    assert "num_mentions" in query_str
    assert "num_sources" in query_str
    assert "num_articles" in query_str


def test_compile_with_quad_classes(compiler):
    """Test compiling with quad classes filter."""
    filters = NormalizedFilters(
        date_from_sqldate=20200101,
        date_to_sqldate=20251231,
        quad_classes=[2, 3],
    )

    stmt = compiler.compile(filters, limit=10, offset=0)
    query_str = str(stmt.compile(compile_kwargs={"literal_binds": True}))

    assert "quad_class" in query_str


def test_compile_with_all_filters(compiler):
    """Test compiling with all filter types."""
    filters = NormalizedFilters(
        date_from_sqldate=20260301,
        date_to_sqldate=20260308,
        actor1_country_code="USA",
        actor2_country_code="GBR",
        geo_country_codes=["US"],
        event_codes=["042"],
        event_root_codes=["04"],
        quad_classes=[2, 3],
        goldstein_min=-3.0,
        goldstein_max=3.0,
        tone_min=-10.0,
        tone_max=10.0,
        min_mentions=5,
        min_sources=2,
        min_articles=1,
    )

    stmt = compiler.compile(filters, limit=50, offset=10)
    query_str = str(stmt.compile(compile_kwargs={"literal_binds": True}))

    # All conditions should be present
    assert "sql_date" in query_str
    assert "actor1_country_code" in query_str
    assert "actor2_country_code" in query_str
    assert "action_geo_country_code" in query_str
    assert "goldstein_scale" in query_str
    assert "avg_tone" in query_str
    assert "num_mentions" in query_str
