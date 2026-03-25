"""
Request / response schemas for the enrichment service.

All models use pydantic v2 semantics.
"""

from __future__ import annotations

from pydantic import BaseModel


class EnrichRequest(BaseModel):
    """Payload sent by the main app to POST /enrich."""

    extracted_title: str | None = None
    extracted_content: str


class EnrichmentEntities(BaseModel, extra="forbid"):
    """Structured entity buckets extracted from the article."""

    persons_cited: list[str] = []
    organizations_cited: list[str] = []
    locations: list[str] = []
    ethnicities_cited: list[str] = []
    religions_cited: list[str] = []
    occupations_cited: list[str] = []
    political_affiliations_cited: list[str] = []
    industries_cited: list[str] = []
    products_cited: list[str] = []
    brands_cited: list[str] = []


class EnrichResponse(BaseModel, extra="forbid"):
    """Semantic enrichment payload returned by POST /enrich."""

    article_title: str | None = None
    article_summary: str | None = None
    cited_sources: list[str] = []
    main_topics: list[str] = []
    keywords: list[str] = []
    entities: EnrichmentEntities = EnrichmentEntities()
