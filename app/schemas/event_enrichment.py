"""Strict schemas for event enrichment service responses."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class EventEnrichmentEntities(BaseModel):
    """Strict entity buckets returned by the internal enrichment service."""

    model_config = ConfigDict(extra="forbid")

    persons_cited: list[str]
    organizations_cited: list[str]
    locations: list[str]
    ethnicities_cited: list[str]
    religions_cited: list[str]
    occupations_cited: list[str]
    political_affiliations_cited: list[str]
    industries_cited: list[str]
    products_cited: list[str]
    brands_cited: list[str]


class EventEnrichmentResponse(BaseModel):
    """Semantic enrichment fields returned by the internal service."""

    model_config = ConfigDict(extra="forbid")

    article_title: str | None
    article_summary: str | None
    cited_sources: list[str]
    main_topics: list[str]
    keywords: list[str]
    entities: EventEnrichmentEntities
