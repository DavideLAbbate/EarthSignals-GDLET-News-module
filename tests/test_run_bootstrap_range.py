"""Tests for the bootstrap range CLI parsing helpers."""

from __future__ import annotations

import importlib
import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


def _import_run_bootstrap_range_module():
    sys.modules.pop("run_bootstrap_range", None)
    return importlib.import_module("run_bootstrap_range")


def test_run_bootstrap_range_module_import_is_safe(monkeypatch: pytest.MonkeyPatch) -> None:
    """Importing the module should not load dotenv or rewrite DATABASE_URL."""
    load_dotenv = MagicMock()
    original_database_url = "postgresql://user:pass@localhost/testdb"
    monkeypatch.setenv("DATABASE_URL", original_database_url)

    with patch.dict(sys.modules, {"dotenv": SimpleNamespace(load_dotenv=load_dotenv)}):
        module = _import_run_bootstrap_range_module()

    assert load_dotenv.call_count == 0
    assert os.environ["DATABASE_URL"] == original_database_url
    assert module.parse_bootstrap_range("20260301", "20260308") == (
        20260301000000,
        20260308235959,
    )


def test_parse_bootstrap_range_normalizes_date_only_bounds() -> None:
    """Date-only inputs expand to full-day start and end timestamps."""
    from run_bootstrap_range import parse_bootstrap_range

    start_ts, end_ts = parse_bootstrap_range("20260301", "20260308")

    assert start_ts == 20260301000000
    assert end_ts == 20260308235959


def test_parse_bootstrap_range_accepts_full_timestamps() -> None:
    """Full timestamps pass through unchanged."""
    from run_bootstrap_range import parse_bootstrap_range

    start_ts, end_ts = parse_bootstrap_range("20260301123045", "20260308112233")

    assert start_ts == 20260301123045
    assert end_ts == 20260308112233


@pytest.mark.parametrize(
    ("start", "end", "expected_start", "expected_end"),
    [
        ("20260301", "20260308112233", 20260301000000, 20260308112233),
        ("20260301123045", "20260308", 20260301123045, 20260308235959),
    ],
)
def test_parse_bootstrap_range_accepts_mixed_timestamp_formats(
    start: str,
    end: str,
    expected_start: int,
    expected_end: int,
) -> None:
    """Date-only and full timestamps can be mixed across CLI bounds."""
    from run_bootstrap_range import parse_bootstrap_range

    start_ts, end_ts = parse_bootstrap_range(start, end)

    assert start_ts == expected_start
    assert end_ts == expected_end


@pytest.mark.parametrize(
    ("start", "end"),
    [
        ("202603", "20260308"),
        ("2026-03-01", "20260308"),
        ("202603011200", "20260308"),
        ("20261301", "20260308"),
        ("20260301", "20260308246000"),
    ],
)
def test_parse_bootstrap_range_rejects_malformed_timestamps(start: str, end: str) -> None:
    """Only valid YYYYMMDD and YYYYMMDDHHMMSS inputs are accepted."""
    from run_bootstrap_range import parse_bootstrap_range

    with pytest.raises(ValueError, match="timestamp"):
        parse_bootstrap_range(start, end)


def test_parse_bootstrap_range_rejects_inverted_bounds() -> None:
    """Start must not be later than end."""
    from run_bootstrap_range import parse_bootstrap_range

    with pytest.raises(ValueError, match="start.*end"):
        parse_bootstrap_range("20260309", "20260308")
