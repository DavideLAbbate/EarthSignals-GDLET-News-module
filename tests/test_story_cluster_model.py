"""Tests for the StoryCluster ORM model."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from app.db.models import StoryCluster


async def test_story_cluster_insert_and_retrieve(db_session):
    cluster = StoryCluster(
        cluster_id="cluster_20260310_test_001",
        source_url="https://example.com/article",
        event_count=5,
        topic_score=3.14,
        themes=["ARMEDCONFLICT"],
        computed_at=datetime.now(UTC),
    )
    db_session.add(cluster)
    await db_session.commit()

    result = await db_session.execute(
        select(StoryCluster).where(StoryCluster.cluster_id == "cluster_20260310_test_001")
    )
    row = result.scalar_one()
    assert row.topic_score == pytest.approx(3.14)
    assert "ARMEDCONFLICT" in row.themes
    assert row.source_url == "https://example.com/article"


async def test_story_cluster_defaults(db_session):
    cluster = StoryCluster(
        cluster_id="cluster_20260310_test_002",
        source_url="https://example.com/story2",
        computed_at=datetime.now(UTC),
    )
    db_session.add(cluster)
    await db_session.commit()

    result = await db_session.execute(
        select(StoryCluster).where(StoryCluster.cluster_id == "cluster_20260310_test_002")
    )
    row = result.scalar_one()
    assert row.event_count == 0
    assert row.mention_count == 0
    assert row.topic_score is None
    assert row.themes is None


async def test_story_cluster_all_enrichment_fields(db_session):
    now = datetime.now(UTC)
    cluster = StoryCluster(
        cluster_id="cluster_20260310_full_001",
        source_url="https://thenationalnews.com/story",
        event_count=128,
        num_articles=572,
        num_mentions=572,
        num_sources=128,
        topic_score=5.60,
        event_ids=["1292890050", "1292890121"],
        dominant_event_types=["Minaccia", "Attacco"],
        dominant_quad_classes=["Conflitto materiale"],
        avg_severity_score=8.8,
        dominant_countries=["IR", "BH"],
        dominant_locations=["Tehran, Tehran, Iran"],
        mention_count=42,
        distinct_mention_sources=["thenationalnews.com", "fnnews.com"],
        mention_identifiers=["https://thenationalnews.com/story"],
        first_mention_at=now,
        last_mention_at=now,
        themes=["ARMEDCONFLICT", "MIDDLE_EAST"],
        persons=["Mojtaba Khamenei"],
        organizations=["Arab Foreign Ministers Council"],
        gkg_locations=["Tehran, Tehran, Iran", "Bahrain"],
        document_tone_avg=-8.1,
        computed_at=now,
    )
    db_session.add(cluster)
    await db_session.commit()

    result = await db_session.execute(
        select(StoryCluster).where(StoryCluster.cluster_id == "cluster_20260310_full_001")
    )
    row = result.scalar_one()
    assert row.topic_score == pytest.approx(5.60)
    assert "IR" in row.dominant_countries
    assert row.mention_count == 42
    assert "ARMEDCONFLICT" in row.themes
    assert row.document_tone_avg == pytest.approx(-8.1)
