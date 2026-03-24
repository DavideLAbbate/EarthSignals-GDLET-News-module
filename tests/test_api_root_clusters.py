"""End-to-end tests for GET /root-clusters/search."""

from __future__ import annotations

from datetime import UTC, datetime

from app.db.models import RootCluster, StoryCluster


async def test_search_root_clusters_success(async_client, api_headers, db_session) -> None:
    db_session.add(
        RootCluster(
            cluster_id="root-a",
            source_url="https://example.com/root-a",
            event_count=7000,
            num_articles=5,
            num_mentions=8,
            num_sources=3,
            topic_score=8.2,
            event_ids=["1", "2"],
            dominant_event_types=["Combattimento"],
            dominant_quad_classes=["Conflitto materiale"],
            avg_severity_score=8.5,
            dominant_countries=["IR"],
            dominant_locations=["Tehran, Tehran, Iran"],
            mention_count=2,
            distinct_mention_sources=["example.com"],
            mention_identifiers=["https://example.com/root-a"],
            themes=["IRAN"],
            persons=["Person A"],
            organizations=["Org A"],
            gkg_locations=["Tehran, Tehran, Iran"],
            document_tone_avg=-4.2,
            event_date_ref_start=20260309,
            event_date_ref_end=20260310,
            computed_at=datetime(2026, 3, 10, tzinfo=UTC),
        )
    )
    await db_session.commit()

    response = await async_client.get("/root-clusters/search?country_code=IR", headers=api_headers)

    assert response.status_code == 200
    data = response.json()
    assert data["clusters"][0]["cluster_id"] == "root-a"
    assert data["clusters"][0]["event_date_ref_start"] == 20260309
    assert data["clusters"][0]["event_date_ref_end"] == 20260310


async def test_search_root_clusters_excludes_story_clusters(
    async_client, api_headers, db_session
) -> None:
    db_session.add(
        StoryCluster(
            cluster_id="story-a",
            source_url="https://example.com/story-a",
            event_count=10,
            num_articles=5,
            num_mentions=8,
            num_sources=3,
            topic_score=4.2,
            dominant_countries=["IR"],
            computed_at=datetime(2026, 3, 10, tzinfo=UTC),
        )
    )
    db_session.add(
        RootCluster(
            cluster_id="root-b",
            source_url="https://example.com/root-b",
            event_count=7001,
            num_articles=5,
            num_mentions=8,
            num_sources=3,
            topic_score=8.4,
            dominant_countries=["IR"],
            computed_at=datetime(2026, 3, 10, tzinfo=UTC),
        )
    )
    await db_session.commit()

    response = await async_client.get("/root-clusters/search?country_code=IR", headers=api_headers)

    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert [cluster["cluster_id"] for cluster in data["clusters"]] == ["root-b"]
