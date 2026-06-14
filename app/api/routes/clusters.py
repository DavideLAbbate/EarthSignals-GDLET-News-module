"""GET /clusters/search for pre-materialised story clusters."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import verify_api_key
from app.db.repositories.cluster_repository import ClusterRepository
from app.db.repositories.root_cluster_repository import RootClusterRepository
from app.db.session import get_async_session
from app.schemas.clusters import (
    ClusterEventEnrichment,
    ClusterGkgEnrichment,
    ClusterLlmEnrichment,
    ClusterScore,
    ClusterSearchResponse,
    StoryClusterResponse,
)

router = APIRouter()

# ---------------------------------------------------------------------------
# Reusable Annotated query-parameter types (FastAPI recommended pattern)
# ---------------------------------------------------------------------------

MinScore = Annotated[
    float | None,
    Query(ge=0.0, description="Return only clusters with topic_score ≥ this value.", example=3.5),
]
MinEventCount = Annotated[
    int | None,
    Query(
        ge=1, description="Return only clusters with at least this many GDELT events.", example=5
    ),
]
MinMentions = Annotated[
    int | None,
    Query(
        ge=1,
        description="Return only clusters with at least this many document mentions.",
        example=50,
    ),
]
CountryCode = Annotated[
    str | None,
    Query(
        max_length=2,
        description="ISO 3166-1 alpha-2 country code. Filters on dominant_countries.",
        example="US",
    ),
]
DateFrom = Annotated[
    int | None,
    Query(
        description="Earliest event date (YYYYMMDD, inclusive). Filters on event_date_ref_start.",
        example=20240301,
    ),
]
DateTo = Annotated[
    int | None,
    Query(
        description="Latest event date (YYYYMMDD, inclusive). Filters on event_date_ref_end.",
        example=20240331,
    ),
]
MentionedAfter = Annotated[
    datetime | None,
    Query(
        description="Return only clusters whose first_mention_at >= this ISO-8601 timestamp. Use this to filter by when the story first appeared in the news.",
        example="2024-03-15T00:00:00Z",
    ),
]
MentionedBefore = Annotated[
    datetime | None,
    Query(
        description="Return only clusters whose last_mention_at <= this ISO-8601 timestamp.",
        example="2024-03-16T23:59:59Z",
    ),
]
EnrichmentStatusFilter = Annotated[
    str | None,
    Query(
        description="Filter by LLM enrichment state: pending | processing | success | failed.",
        example="success",
    ),
]
EventType = Annotated[
    str | None,
    Query(
        description="Filter clusters where dominant_event_types contains this GDELT root event code.",
        example="PROTEST",
    ),
]
QuadClass = Annotated[
    str | None,
    Query(
        description="Filter clusters where dominant_quad_classes contains this value (1=Verbal Coop … 4=Material Conflict).",
        example="3",
    ),
]
Theme = Annotated[
    str | None,
    Query(
        description="Filter clusters where GKG themes contains this value (case-sensitive GDELT theme code).",
        example="HUMAN_RIGHTS",
    ),
]
Keyword = Annotated[
    str | None,
    Query(
        description="Filter clusters where LLM keywords contains this value. Effective only for enrichment_status=success clusters.",
        example="sanctions",
    ),
]
Topic = Annotated[
    str | None,
    Query(
        description="Filter clusters where LLM main_topics contains this value. Effective only for enrichment_status=success clusters.",
        example="Armed Conflict",
    ),
]
Limit = Annotated[
    int, Query(ge=1, le=500, description="Maximum number of clusters to return.", example=20)
]
Offset = Annotated[
    int, Query(ge=0, description="Number of clusters to skip (for pagination).", example=0)
]


def _map_cluster(cluster) -> StoryClusterResponse:
    """Map a StoryCluster/RootCluster ORM row to the public API schema.

    When enrichment_status == 'success', the LLM enrichment block is included
    and the GKG enrichment block is omitted.  For all other statuses the GKG
    block is included and the LLM block is omitted.  Mentions are always present.
    """
    return StoryClusterResponse(
        cluster_id=cluster.cluster_id,
        source_url=cluster.source_url,
        enrichment_status=cluster.enrichment_status,
        score=ClusterScore(
            events=cluster.event_count,
            num_articles=cluster.num_articles,
            num_mentions=cluster.num_mentions,
            num_sources=cluster.num_sources,
            topic_score=cluster.topic_score,
        ),
        event_enrichment=ClusterEventEnrichment(
            dominant_event_types=cluster.dominant_event_types or [],
            dominant_quad_classes=cluster.dominant_quad_classes or [],
            avg_severity_score=cluster.avg_severity_score,
            dominant_countries=cluster.dominant_countries or [],
            dominant_locations=cluster.dominant_locations or [],
        ),
        gkg_enrichment=ClusterGkgEnrichment(
            themes=cluster.themes or [],
            persons=cluster.persons or [],
            organizations=cluster.organizations or [],
            locations=cluster.gkg_locations or [],
            document_tone_avg=cluster.document_tone_avg,
        ),
        llm_enrichment=ClusterLlmEnrichment(
            article_title=cluster.article_title,
            article_summary=cluster.article_summary,
            cited_sources=cluster.cited_sources or [],
            main_topics=cluster.main_topics or [],
            keywords=cluster.keywords or [],
            entities=cluster.entities or {},
            enriched_at=cluster.enriched_at,
        ),
        event_date_ref_start=cluster.event_date_ref_start,
        event_date_ref_end=cluster.event_date_ref_end,
        computed_at=cluster.computed_at,
    )


@router.get(
    "/clusters/search",
    response_model=ClusterSearchResponse,
    summary="Search materialised story clusters",
    description=(
        "Search and filter pre-materialised story clusters. All filter parameters are optional "
        "and combinable.\n\n"
        "**Enrichment logic:**\n"
        "- When `enrichment_status=success` the response includes `llm_enrichment` and omits `gkg_enrichment`.\n"
        "- For all other statuses `gkg_enrichment` is included and `llm_enrichment` is `null`.\n"
        "- `mentions_enrichment` is always present.\n\n"
        "**Example request (LLM-enriched clusters in the US from March 2024):**\n"
        "```\n"
        "GET /clusters/search?country_code=US&enrichment_status=success"
        "&date_from=20240301&date_to=20240331&min_score=3.5&limit=20\n"
        "```"
    ),
    tags=["Clusters"],
)
async def search_clusters(
    min_score: MinScore = None,
    min_event_count: MinEventCount = None,
    min_mentions: MinMentions = None,
    country_code: CountryCode = None,
    date_from: DateFrom = None,
    date_to: DateTo = None,
    mentioned_after: MentionedAfter = None,
    mentioned_before: MentionedBefore = None,
    enrichment_status: EnrichmentStatusFilter = None,
    event_type: EventType = None,
    quad_class: QuadClass = None,
    theme: Theme = None,
    keyword: Keyword = None,
    topic: Topic = None,
    limit: Limit = 50,
    offset: Offset = 0,
    session: AsyncSession = Depends(get_async_session),
    _: str = Depends(verify_api_key),
) -> ClusterSearchResponse:
    repo = ClusterRepository(session)
    clusters, total = await repo.search(
        min_score=min_score,
        min_event_count=min_event_count,
        min_mentions=min_mentions,
        country_code=country_code,
        date_from=date_from,
        date_to=date_to,
        mentioned_after=mentioned_after,
        mentioned_before=mentioned_before,
        enrichment_status=enrichment_status,
        event_type=event_type,
        quad_class=quad_class,
        theme=theme,
        keyword=keyword,
        topic=topic,
        limit=limit,
        offset=offset,
    )
    return ClusterSearchResponse(
        clusters=[_map_cluster(c) for c in clusters],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/root-clusters/search",
    response_model=ClusterSearchResponse,
    summary="Search materialised root clusters",
    description=(
        "Search and filter pre-materialised root clusters (very large merged stories). "
        "Accepts the same filter parameters as `/clusters/search`.\n\n"
        "**Example request (high-score conflict clusters in Ukraine):**\n"
        "```\n"
        "GET /root-clusters/search?country_code=UA&quad_class=4&min_score=5.0&limit=10\n"
        "```"
    ),
    tags=["Clusters"],
)
async def search_root_clusters(
    min_score: MinScore = None,
    min_event_count: MinEventCount = None,
    min_mentions: MinMentions = None,
    country_code: CountryCode = None,
    date_from: DateFrom = None,
    date_to: DateTo = None,
    mentioned_after: MentionedAfter = None,
    mentioned_before: MentionedBefore = None,
    enrichment_status: EnrichmentStatusFilter = None,
    event_type: EventType = None,
    quad_class: QuadClass = None,
    theme: Theme = None,
    keyword: Keyword = None,
    topic: Topic = None,
    limit: Limit = 50,
    offset: Offset = 0,
    session: AsyncSession = Depends(get_async_session),
    _: str = Depends(verify_api_key),
) -> ClusterSearchResponse:
    repo = RootClusterRepository(session)
    clusters, total = await repo.search(
        min_score=min_score,
        min_event_count=min_event_count,
        min_mentions=min_mentions,
        country_code=country_code,
        date_from=date_from,
        date_to=date_to,
        mentioned_after=mentioned_after,
        mentioned_before=mentioned_before,
        enrichment_status=enrichment_status,
        event_type=event_type,
        quad_class=quad_class,
        theme=theme,
        keyword=keyword,
        topic=topic,
        limit=limit,
        offset=offset,
    )
    return ClusterSearchResponse(
        clusters=[_map_cluster(c) for c in clusters],
        total=total,
        limit=limit,
        offset=offset,
    )
