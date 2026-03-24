"""Tests for the RootCluster ORM model."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select

from app.db.models import RootCluster


async def test_root_cluster_model_persists_core_fields(db_session) -> None:
    root = RootCluster(
        cluster_id="root-1",
        source_url="https://example.com/root",
        event_count=6001,
        num_articles=100,
        num_mentions=200,
        num_sources=50,
        topic_score=9.1,
        dominant_countries=["IR"],
        computed_at=datetime(2026, 3, 20, tzinfo=UTC),
    )
    db_session.add(root)
    await db_session.commit()

    rows = (await db_session.execute(select(RootCluster))).scalars().all()
    assert len(rows) == 1
    assert rows[0].event_count == 6001
