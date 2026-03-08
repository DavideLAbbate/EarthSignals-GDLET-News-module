"""
Event-related Pydantic schemas.

GDELTEvent    — a single event record returned in search results
SearchResponse — the full /events/search response envelope
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from app.schemas.filters import NormalizedFilters


class GDELTEvent(BaseModel):
    """A single GDELT event as returned to the frontend."""

    event_id: str
    date: str | None = None

    # Actor countries (CAMEO 3-letter codes)
    actor1_country: str | None = None
    actor2_country: str | None = None

    # Event classification (CAMEO codes)
    event_code: str | None = None
    event_base_code: str | None = None
    event_root_code: str | None = None

    # QuadClass: 1=Verbal Cooperation, 2=Material Cooperation,
    #            3=Verbal Conflict, 4=Material Conflict
    quad_class: int | None = None

    # Sentiment/impact scores
    goldstein_scale: float | None = None  # -10.0 (destabilizing) to +10.0 (stabilizing)
    tone: float | None = None  # AvgTone: article sentiment

    # Coverage metrics
    num_mentions: int | None = None
    num_sources: int | None = None
    num_articles: int | None = None

    # Geographic location of the event (FIPS 2-letter)
    action_geo_fullname: str | None = None
    action_geo_country: str | None = None

    # Source (domain derived from SOURCEURL)
    source_name: str | None = None
    source_url: str | None = None


class ResponseMetadata(BaseModel):
    """Metadata envelope included in all search responses."""

    total_results: int
    query_time_ms: int = 0
    last_gdelt_sync: str | None = None  # ISO 8601 UTC
    mapping_version: str | None = None  # ISO 8601 UTC
    bq_bytes_processed: int | None = None


class SearchResponse(BaseModel):
    """
    Full response envelope for POST /events/search.

    Includes the original filter input, the normalized filters
    Claude produced, the list of matching events, and metadata.
    """

    filters_received: dict = Field(description="The raw filter input as received from the frontend")
    filters_normalized: NormalizedFilters
    results: list[GDELTEvent] = Field(default_factory=list)
    metadata: ResponseMetadata
