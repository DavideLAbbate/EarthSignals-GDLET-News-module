"""Cluster-related Pydantic schemas."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class ClusterScore(BaseModel):
    """Score and aggregate metrics for a materialised cluster."""

    events: int = Field(description="Number of distinct events in the cluster")
    num_articles: int = Field(description="Aggregated NumArticles across events")
    num_mentions: int = Field(description="Aggregated NumMentions across events")
    num_sources: int = Field(description="Aggregated NumSources across events")
    topic_score: float | None = Field(default=None, description="Logarithmic cluster score")


class ClusterEventEnrichment(BaseModel):
    """Aggregated event-layer enrichment fields."""

    dominant_event_types: list[str] = Field(default_factory=list)
    dominant_quad_classes: list[str] = Field(default_factory=list)
    avg_severity_score: float | None = None
    dominant_countries: list[str] = Field(default_factory=list)
    dominant_locations: list[str] = Field(default_factory=list)


class ClusterMentionsEnrichment(BaseModel):
    """Aggregated mentions-layer enrichment fields."""

    mention_count: int = 0
    distinct_mention_sources: list[str] = Field(default_factory=list)
    first_mention_at: datetime | None = None
    last_mention_at: datetime | None = None


class ClusterGkgEnrichment(BaseModel):
    """Aggregated GKG-layer enrichment fields."""

    themes: list[str] = Field(default_factory=list)
    persons: list[str] = Field(default_factory=list)
    organizations: list[str] = Field(default_factory=list)
    locations: list[str] = Field(default_factory=list)
    document_tone_avg: float | None = None


class StoryClusterResponse(BaseModel):
    """Public API response for a single story cluster."""

    cluster_id: str
    source_url: str
    score: ClusterScore
    event_ids: list[str] = Field(default_factory=list)
    event_enrichment: ClusterEventEnrichment
    mentions_enrichment: ClusterMentionsEnrichment
    gkg_enrichment: ClusterGkgEnrichment
    computed_at: datetime


class ClusterSearchResponse(BaseModel):
    """Paginated response for GET /clusters/search."""

    clusters: list[StoryClusterResponse] = Field(default_factory=list)
    total: int
    limit: int
    offset: int
