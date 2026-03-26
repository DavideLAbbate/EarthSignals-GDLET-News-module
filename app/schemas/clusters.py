"""Cluster-related Pydantic schemas."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ClusterScore(BaseModel):
    """Score and aggregate metrics for a materialised cluster."""

    events: int = Field(description="Number of distinct events in the cluster")
    num_articles: int = Field(description="Aggregated NumArticles across events")
    num_mentions: int = Field(description="Aggregated NumMentions across events")
    num_sources: int = Field(description="Aggregated NumSources across events")
    topic_score: float | None = Field(default=None, description="Logarithmic cluster score")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "events": 14,
                "num_articles": 312,
                "num_mentions": 874,
                "num_sources": 58,
                "topic_score": 7.42,
            }
        }
    )


class ClusterEventEnrichment(BaseModel):
    """Aggregated event-layer enrichment fields."""

    dominant_event_types: list[str] = Field(default_factory=list)
    dominant_quad_classes: list[str] = Field(default_factory=list)
    avg_severity_score: float | None = None
    dominant_countries: list[str] = Field(default_factory=list)
    dominant_locations: list[str] = Field(default_factory=list)

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "dominant_event_types": ["PROTEST", "MAKE_STATEMENT"],
                "dominant_quad_classes": ["3", "1"],
                "avg_severity_score": -2.8,
                "dominant_countries": ["US", "UA"],
                "dominant_locations": ["Washington D.C.", "Kyiv"],
            }
        }
    )


class ClusterMentionsEnrichment(BaseModel):
    """Aggregated mentions-layer enrichment fields. Always present."""

    mention_count: int = 0
    distinct_mention_sources: list[str] = Field(default_factory=list)
    mention_identifiers: list[str] = Field(
        default_factory=list,
        description="Candidate article URLs used as LLM enrichment sources",
    )
    first_mention_at: datetime | None = None
    last_mention_at: datetime | None = None

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "mention_count": 874,
                "distinct_mention_sources": ["reuters.com", "bbc.co.uk", "apnews.com"],
                "mention_identifiers": [
                    "https://www.reuters.com/world/example-article",
                    "https://apnews.com/article/example",
                ],
                "first_mention_at": "2024-03-15T08:12:00Z",
                "last_mention_at": "2024-03-16T22:47:00Z",
            }
        }
    )


class ClusterGkgEnrichment(BaseModel):
    """Aggregated GKG-layer enrichment. Present only when LLM enrichment is absent."""

    themes: list[str] = Field(default_factory=list)
    persons: list[str] = Field(default_factory=list)
    organizations: list[str] = Field(default_factory=list)
    locations: list[str] = Field(default_factory=list)
    document_tone_avg: float | None = None

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "themes": ["PROTEST", "HUMAN_RIGHTS", "LEGISLATION"],
                "persons": ["Joe Biden", "Volodymyr Zelenskyy"],
                "organizations": ["NATO", "United Nations"],
                "locations": ["Ukraine", "Washington D.C."],
                "document_tone_avg": -3.1,
            }
        }
    )


class ClusterLlmEnrichment(BaseModel):
    """LLM-derived semantic enrichment. Present only when enrichment_status == 'success'."""

    article_title: str | None = None
    article_summary: str | None = None
    cited_sources: list[str] = Field(default_factory=list)
    main_topics: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    entities: dict[str, Any] = Field(default_factory=dict)
    enriched_at: datetime | None = None

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "article_title": "NATO allies pledge additional military aid to Ukraine amid escalating conflict",
                "article_summary": (
                    "NATO member states convened in Brussels to announce a new package of military "
                    "and financial support for Ukraine. The agreement includes air-defence systems "
                    "and long-range artillery. Ukrainian President Zelenskyy addressed the summit "
                    "via video link, urging faster delivery of pledged equipment."
                ),
                "cited_sources": ["Reuters", "BBC News", "Associated Press"],
                "main_topics": ["International Relations", "Armed Conflict", "Military Aid", "Diplomacy"],
                "keywords": [
                    "NATO", "Ukraine", "military aid", "air defence", "Brussels summit",
                    "Zelenskyy", "artillery", "sanctions",
                ],
                "entities": {
                    "persons_cited": ["Volodymyr Zelenskyy", "Jens Stoltenberg"],
                    "organizations_cited": ["NATO", "European Union"],
                    "locations": ["Brussels", "Kyiv", "Ukraine"],
                    "ethnicities_cited": [],
                    "religions_cited": [],
                    "occupations_cited": ["President", "Secretary General"],
                    "political_affiliations_cited": [],
                    "industries_cited": ["Defense"],
                    "products_cited": ["Patriot missile system", "HIMARS"],
                    "brands_cited": [],
                },
                "enriched_at": "2024-03-16T10:05:33Z",
            }
        }
    )


class StoryClusterResponse(BaseModel):
    """Public API response for a single story cluster.

    The enrichment block is mutually exclusive:
    - ``gkg_enrichment`` is populated when ``enrichment_status != 'success'`` (LLM not yet run).
    - ``llm_enrichment`` is populated when ``enrichment_status == 'success'`` (LLM completed).
    ``mentions_enrichment`` is always present regardless of enrichment status.
    """

    cluster_id: str = Field(description="Deterministic cluster identifier: {YYYYMMDD}_{sha256(source_url)[:12]}")
    source_url: str = Field(description="Canonical source URL for the cluster pivot event")
    enrichment_status: str = Field(description="LLM enrichment state: pending | processing | success | failed")
    score: ClusterScore
    event_enrichment: ClusterEventEnrichment
    gkg_enrichment: ClusterGkgEnrichment
    llm_enrichment: ClusterLlmEnrichment
    event_date_ref_start: int | None = Field(
        default=None, description="Earliest event date in YYYYMMDD integer format"
    )
    event_date_ref_end: int | None = Field(
        default=None, description="Latest event date in YYYYMMDD integer format"
    )
    computed_at: datetime = Field(description="Timestamp when this cluster was last materialised")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                        "cluster_id": "20240315_a3f9c12b8e4d",
                        "source_url": "https://www.reuters.com/world/nato-ukraine-aid-2024-03-15/",
                        "enrichment_status": "success",
                        "score": {
                            "events": 14,
                            "num_articles": 312,
                            "num_mentions": 874,
                            "num_sources": 58,
                            "topic_score": 7.42,
                        },
                        "event_enrichment": {
                            "dominant_event_types": ["PROTEST", "MAKE_STATEMENT"],
                            "dominant_quad_classes": ["3", "1"],
                            "avg_severity_score": -2.8,
                            "dominant_countries": ["US", "UA"],
                            "dominant_locations": ["Washington D.C.", "Kyiv"],
                        },
                        "gkg_enrichment": None,
                        "llm_enrichment": {
                            "article_title": "NATO allies pledge additional military aid to Ukraine amid escalating conflict",
                            "article_summary": "NATO member states convened in Brussels to announce a new package of military and financial support for Ukraine.",
                            "cited_sources": ["Reuters", "BBC News", "Associated Press"],
                            "main_topics": ["International Relations", "Armed Conflict", "Military Aid"],
                            "keywords": ["NATO", "Ukraine", "military aid", "Brussels summit", "Zelenskyy"],
                            "entities": {
                                "persons_cited": ["Volodymyr Zelenskyy", "Jens Stoltenberg"],
                                "organizations_cited": ["NATO", "European Union"],
                                "locations": ["Brussels", "Kyiv"],
                                "ethnicities_cited": [],
                                "religions_cited": [],
                                "occupations_cited": ["President", "Secretary General"],
                                "political_affiliations_cited": [],
                                "industries_cited": ["Defense"],
                                "products_cited": ["Patriot missile system"],
                                "brands_cited": [],
                            },
                            "enriched_at": "2024-03-16T10:05:33Z",
                        },
                        "event_date_ref_start": 20240315,
                        "event_date_ref_end": 20240316,
                        "computed_at": "2024-03-16T12:00:00Z",
            }
        }
    )


class ClusterSearchResponse(BaseModel):
    """Paginated response for GET /clusters/search."""

    clusters: list[StoryClusterResponse] = Field(default_factory=list)
    total: int = Field(description="Total clusters matching the applied filters (before pagination)")
    limit: int = Field(description="Maximum number of clusters returned in this page")
    offset: int = Field(description="Number of clusters skipped before this page")
