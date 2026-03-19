"""Label mappings and score formulas for GDELT event enrichment.

Provides human-readable labels for GDELT quad_class and event_root_code values,
plus two score functions used by ClusterService to enrich and rank clusters.
"""

from __future__ import annotations

import math

# ── Quad-class labels ─────────────────────────────────────────────────────────
_QUAD_CLASS_LABELS: dict[int, str] = {
    1: "Cooperazione diplomatica",
    2: "Cooperazione concreta",
    3: "Tensione verbale",
    4: "Conflitto materiale",
}

# ── Event root-code labels (CAMEO standard) ───────────────────────────────────
_EVENT_ROOT_CODE_LABELS: dict[str, str] = {
    "01": "Dichiarazione",
    "02": "Appello",
    "03": "Esprimi intenzione",
    "04": "Consulta",
    "05": "Cooperazione diplomatica",
    "06": "Cooperazione concreta",
    "07": "Accordo",
    "08": "Accusa",
    "09": "Rifiuto",
    "10": "Domanda",
    "11": "Critica",
    "12": "Disapprovazione",
    "13": "Minaccia",
    "14": "Protesta",
    "15": "Esibizione di forza",
    "16": "Riduzione relazioni",
    "17": "Coercizione",
    "18": "Attacco",
    "19": "Combattimento",
    "20": "Violenza di massa",
}


def get_quad_class_label(quad_class: int | None) -> str:
    """Return the Italian label for a GDELT quad_class value, or 'Sconosciuto'."""
    if quad_class is None:
        return "Sconosciuto"
    return _QUAD_CLASS_LABELS.get(quad_class, "Sconosciuto")


def get_event_root_code_label(code: str | None) -> str:
    """Return the Italian label for a GDELT event_root_code, or 'Sconosciuto'."""
    if code is None:
        return "Sconosciuto"
    return _EVENT_ROOT_CODE_LABELS.get(code, "Sconosciuto")


def compute_severity_score(
    quad_class: int | None,
    goldstein_scale: float | None,
    avg_tone: float | None,
) -> float:
    """Compute a severity score in [0, 20] for a single event.

    Formula:
        severity = quad_weight + max(0, -goldstein_scale) * 0.5 + abs(avg_tone) * 0.3

    A negative Goldstein scale (conflict, coercion) increases severity; a positive
    scale (cooperation, peace) contributes zero — using abs() would wrongly treat a
    peace treaty (+10) with the same weight as a bombardment (-10).

    quad_weight mapping: {1: 0.0, 2: 2.0, 3: 5.0, 4: 10.0}
    Result is capped at 20.0 and rounded to 2 decimal places.
    """
    quad_weight = {1: 0.0, 2: 2.0, 3: 5.0, 4: 10.0}.get(quad_class or 0, 0.0)
    raw = quad_weight + max(0.0, -(goldstein_scale or 0.0)) * 0.5 + abs(avg_tone or 0.0) * 0.3
    return round(min(raw, 20.0), 2)


def compute_topic_score(
    event_count: int,
    num_articles: int,
    num_mentions: int,
    num_sources: int,
) -> float:
    """Compute a logarithmic topic score for a cluster.

    Formula:
        topic_score =
            ln(events + 1)   * 0.4
          + ln(articles + 1) * 0.3
          + ln(mentions + 1) * 0.2
          + ln(sources + 1)  * 0.1

    Uses natural log to dampen outlier dominance.
    Result is rounded to 4 decimal places.
    """
    return round(
        math.log(event_count + 1) * 0.4
        + math.log(num_articles + 1) * 0.3
        + math.log(num_mentions + 1) * 0.2
        + math.log(num_sources + 1) * 0.1,
        4,
    )
