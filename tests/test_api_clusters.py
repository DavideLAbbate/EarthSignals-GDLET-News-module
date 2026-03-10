"""End-to-end tests for GET /clusters/search."""

from __future__ import annotations

from datetime import UTC, datetime

from app.db.models import StoryCluster


def _make_cluster(
    cluster_id: str,
    *,
    topic_score: float,
    dominant_countries: list[str] | None = None,
) -> StoryCluster:
    return StoryCluster(
        cluster_id=cluster_id,
        source_url=f"https://example.com/{cluster_id}",
        event_count=2,
        num_articles=5,
        num_mentions=8,
        num_sources=3,
        topic_score=topic_score,
        event_ids=["1", "2"],
        dominant_event_types=["Combattimento"],
        dominant_quad_classes=["Conflitto materiale"],
        avg_severity_score=8.5,
        dominant_countries=dominant_countries or ["IR"],
        dominant_locations=["Tehran, Tehran, Iran"],
        mention_count=2,
        distinct_mention_sources=["example.com"],
        mention_identifiers=[f"https://example.com/{cluster_id}"],
        themes=["IRAN"],
        persons=["Person A"],
        organizations=["Org A"],
        gkg_locations=["Tehran, Tehran, Iran"],
        document_tone_avg=-4.2,
        computed_at=datetime(2026, 3, 10, tzinfo=UTC),
    )


async def test_search_clusters_success(async_client, api_headers, db_session):
    db_session.add(_make_cluster("cluster-a", topic_score=4.2))
    await db_session.commit()

    response = await async_client.get("/clusters/search", headers=api_headers)

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert data["limit"] == 50
    assert data["offset"] == 0
    assert len(data["clusters"]) == 1
    assert data["clusters"][0]["cluster_id"] == "cluster-a"
    assert data["clusters"][0]["score"]["topic_score"] == 4.2
    assert data["clusters"][0]["event_enrichment"]["dominant_event_types"] == ["Combattimento"]


async def test_search_clusters_requires_api_key(async_client):
    response = await async_client.get("/clusters/search")
    assert response.status_code == 401


async def test_search_clusters_filters_by_min_score(async_client, api_headers, db_session):
    db_session.add_all(
        [
            _make_cluster("cluster-high", topic_score=5.0),
            _make_cluster("cluster-low", topic_score=0.4),
        ]
    )
    await db_session.commit()

    response = await async_client.get("/clusters/search?min_score=1.0", headers=api_headers)

    assert response.status_code == 200
    data = response.json()
    assert [cluster["cluster_id"] for cluster in data["clusters"]] == ["cluster-high"]
    assert data["total"] == 1


async def test_search_clusters_returns_pagination_fields(async_client, api_headers, db_session):
    db_session.add_all(
        [
            _make_cluster("cluster-1", topic_score=5.0),
            _make_cluster("cluster-2", topic_score=4.0),
            _make_cluster("cluster-3", topic_score=3.0),
        ]
    )
    await db_session.commit()

    response = await async_client.get("/clusters/search?limit=1&offset=1", headers=api_headers)

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 3
    assert data["limit"] == 1
    assert data["offset"] == 1
    assert len(data["clusters"]) == 1
    assert data["clusters"][0]["cluster_id"] == "cluster-2"


async def test_search_clusters_filters_by_country_code(async_client, api_headers, db_session):
    db_session.add_all(
        [
            _make_cluster("cluster-ir", topic_score=5.0, dominant_countries=["IR"]),
            _make_cluster("cluster-us", topic_score=4.0, dominant_countries=["US"]),
        ]
    )
    await db_session.commit()

    response = await async_client.get("/clusters/search?country_code=US", headers=api_headers)

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert [cluster["cluster_id"] for cluster in data["clusters"]] == ["cluster-us"]
