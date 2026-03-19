"""Tests for event enrichment labels and score formulas."""

from __future__ import annotations

import math

import pytest

from app.integrations.event_enrichment_mapper import (
    compute_severity_score,
    compute_topic_score,
    get_event_root_code_label,
    get_quad_class_label,
)


def test_get_quad_class_label_returns_known_label() -> None:
    assert get_quad_class_label(4) == "Conflitto materiale"


def test_get_quad_class_label_returns_unknown_for_missing() -> None:
    assert get_quad_class_label(None) == "Sconosciuto"


def test_get_event_root_code_label_returns_known_label() -> None:
    assert get_event_root_code_label("19") == "Combattimento"


def test_get_event_root_code_label_returns_unknown_for_unmapped() -> None:
    assert get_event_root_code_label("99") == "Sconosciuto"


def test_compute_severity_score_caps_at_twenty() -> None:
    assert compute_severity_score(4, -20.0, -20.0) == pytest.approx(20.0)


def test_compute_severity_score_positive_goldstein_contributes_zero() -> None:
    """A positive Goldstein scale (cooperation/peace) must not increase severity."""
    # quad=1 (0.0 weight), goldstein=+10 (positive → zero contribution), avg_tone=0
    assert compute_severity_score(1, 10.0, 0.0) == pytest.approx(0.0)


def test_compute_severity_score_negative_goldstein_increases_severity() -> None:
    """A negative Goldstein scale (conflict) must increase severity."""
    # quad=1 (0.0 weight), goldstein=-10 → max(0, 10)*0.5 = 5.0, avg_tone=0
    assert compute_severity_score(1, -10.0, 0.0) == pytest.approx(5.0)


def test_compute_severity_score_symmetric_events_differ() -> None:
    """Peace treaty (+10) and bombardment (-10) must produce different severity scores."""
    peace = compute_severity_score(1, 10.0, 0.0)
    war = compute_severity_score(1, -10.0, 0.0)
    assert peace == pytest.approx(0.0)
    assert war == pytest.approx(5.0)
    assert peace != war


def test_compute_topic_score_uses_logarithmic_formula() -> None:
    expected = round(
        math.log(3 + 1) * 0.4
        + math.log(10 + 1) * 0.3
        + math.log(20 + 1) * 0.2
        + math.log(5 + 1) * 0.1,
        4,
    )
    assert compute_topic_score(3, 10, 20, 5) == pytest.approx(expected)
