"""GET /clusters/search for pre-materialised story clusters."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import verify_api_key
from app.db.repositories.cluster_repository import ClusterRepository
from app.db.session import get_async_session
from app.schemas.clusters import (
    ClusterEventEnrichment,
    ClusterGkgEnrichment,
    ClusterMentionsEnrichment,
    ClusterScore,
    ClusterSearchResponse,
    StoryClusterResponse,
)

router = APIRouter()


def _map_cluster(cluster) -> StoryClusterResponse:
    """Map a StoryCluster ORM row to the public API schema."""
    return StoryClusterResponse(
        cluster_id=cluster.cluster_id,
        source_url=cluster.source_url,
        score=ClusterScore(
            events=cluster.event_count,
            num_articles=cluster.num_articles,
            num_mentions=cluster.num_mentions,
            num_sources=cluster.num_sources,
            topic_score=cluster.topic_score,
        ),
        event_ids=cluster.event_ids or [],
        event_enrichment=ClusterEventEnrichment(
            dominant_event_types=cluster.dominant_event_types or [],
            dominant_quad_classes=cluster.dominant_quad_classes or [],
            avg_severity_score=cluster.avg_severity_score,
            dominant_countries=cluster.dominant_countries or [],
            dominant_locations=cluster.dominant_locations or [],
        ),
        mentions_enrichment=ClusterMentionsEnrichment(
            mention_count=cluster.mention_count,
            distinct_mention_sources=cluster.distinct_mention_sources or [],
            first_mention_at=cluster.first_mention_at,
            last_mention_at=cluster.last_mention_at,
        ),
        gkg_enrichment=ClusterGkgEnrichment(
            themes=cluster.themes or [],
            persons=cluster.persons or [],
            organizations=cluster.organizations or [],
            locations=cluster.gkg_locations or [],
            document_tone_avg=cluster.document_tone_avg,
        ),
        computed_at=cluster.computed_at,
    )


@router.get(
    "/clusters/search",
    response_model=ClusterSearchResponse,
    summary="Search materialised story clusters",
    tags=["Clusters"],
)
async def search_clusters(
    min_score: float | None = Query(default=None, ge=0.0),
    country_code: str | None = Query(default=None, max_length=2),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_async_session),
    _: str = Depends(verify_api_key),
) -> ClusterSearchResponse:
    """Search clusters with optional score and country filters."""
    repo = ClusterRepository(session)
    clusters, total = await repo.search(
        min_score=min_score,
        country_code=country_code,
        limit=limit,
        offset=offset,
    )
    return ClusterSearchResponse(
        clusters=[_map_cluster(cluster) for cluster in clusters],
        total=total,
        limit=limit,
        offset=offset,
    )
