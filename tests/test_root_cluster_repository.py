"""Tests for RootClusterRepository."""

from __future__ import annotations

from datetime import UTC, datetime

from app.db.repositories.root_cluster_repository import RootClusterRepository


def _make_root_cluster(
    cluster_id: str,
    score: float,
    countries: list[str] | None = None,
) -> dict:
    return {
        "cluster_id": cluster_id,
        "source_url": f"https://example.com/{cluster_id}",
        "event_count": 6001,
        "num_articles": 10,
        "num_mentions": 20,
        "num_sources": 5,
        "topic_score": score,
        "dominant_countries": countries or [],
        "computed_at": datetime.now(UTC),
    }


async def test_root_repository_upsert_and_search(db_session) -> None:
    repo = RootClusterRepository(db_session)
    await repo.upsert(_make_root_cluster("root-c1", 8.0, ["US"]))
    await db_session.commit()

    rows, total = await repo.search(country_code="US")
    assert total == 1
    assert rows[0].cluster_id == "root-c1"


async def test_root_repository_delete_by_cluster_ids(db_session) -> None:
    repo = RootClusterRepository(db_session)
    await repo.bulk_upsert(
        [
            _make_root_cluster("root-keep", 8.0, ["US"]),
            _make_root_cluster("root-drop", 7.0, ["IR"]),
        ]
    )
    await db_session.commit()

    deleted = await repo.delete_by_cluster_ids({"root-drop"})
    await db_session.commit()

    rows, total = await repo.search()
    assert deleted == 1
    assert total == 1
    assert [row.cluster_id for row in rows] == ["root-keep"]


async def test_root_repository_ignores_merge_evidence_transient_key(db_session) -> None:
    repo = RootClusterRepository(db_session)

    await repo.upsert(
        {**_make_root_cluster("root-merge-evidence", 9.0), "merge_evidence": [{"jaccard": 0.7}]}
    )
    await db_session.commit()

    rows, total = await repo.search()
    assert total == 1
    assert rows[0].cluster_id == "root-merge-evidence"
