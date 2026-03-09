"""Compact schemas for event enrichment service responses."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class EventEnrichmentResponse(BaseModel):
    """Semantic enrichment fields returned by the internal service."""

    model_config = ConfigDict(extra="forbid")

    article_title: str | None
    article_summary: str | None
    sources: list[str]
