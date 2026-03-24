"""Tests for ClusterRepository."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.db.repositories.cluster_repository import ClusterRepository


def _make_cluster(cluster_id: str, score: float, countries: list[str] | None = None) -> dict:
    return {
        "cluster_id": cluster_id,
        "source_url": f"https://example.com/{cluster_id}",
        "event_count": 3,
        "num_articles": 10,
        "num_mentions": 20,
        "num_sources": 5,
        "topic_score": score,
        "dominant_countries": countries or [],
        "computed_at": datetime.now(UTC),
    }


async def test_upsert_and_search_returns_inserted_cluster(db_session):
    """A single upserted cluster is returned by search."""
    repo = ClusterRepository(db_session)
    await repo.upsert(_make_cluster("c1", 5.0, ["US"]))
    await db_session.commit()

    clusters, total = await repo.search()
    assert total >= 1
    ids = [c.cluster_id for c in clusters]
    assert "c1" in ids


async def test_search_filters_by_min_score(db_session):
    """search(min_score=N) returns only clusters with topic_score >= N."""
    repo = ClusterRepository(db_session)
    await repo.bulk_upsert(
        [
            _make_cluster("high", 8.0, ["US"]),
            _make_cluster("low", 1.0, ["IR"]),
        ]
    )
    await db_session.commit()

    clusters, total = await repo.search(min_score=5.0)
    ids = [c.cluster_id for c in clusters]
    assert "high" in ids
    assert "low" not in ids


async def test_search_returns_ordered_by_score_desc(db_session):
    """Results are ordered by topic_score descending."""
    repo = ClusterRepository(db_session)
    await repo.bulk_upsert(
        [
            _make_cluster("ca", 1.0),
            _make_cluster("cb", 8.0),
            _make_cluster("cc", 4.0),
        ]
    )
    await db_session.commit()

    clusters, _ = await repo.search()
    scores = [c.topic_score for c in clusters if c.topic_score is not None]
    assert scores == sorted(scores, reverse=True)


async def test_upsert_overwrites_existing_cluster(db_session):
    """Upserting a cluster_id that already exists updates the row."""
    repo = ClusterRepository(db_session)
    await repo.upsert(_make_cluster("dup1", 3.0, ["US"]))
    await db_session.commit()

    updated = _make_cluster("dup1", 9.9, ["US", "IR"])
    await repo.upsert(updated)
    await db_session.commit()

    clusters, _ = await repo.search()
    dup = next((c for c in clusters if c.cluster_id == "dup1"), None)
    assert dup is not None
    assert dup.topic_score == pytest.approx(9.9)


async def test_search_total_count_reflects_all_rows(db_session):
    """The total count in the result reflects all matching rows, not just the page."""
    repo = ClusterRepository(db_session)
    await repo.bulk_upsert([_make_cluster(f"c{i}", float(i)) for i in range(5)])
    await db_session.commit()

    _, total = await repo.search(limit=2, offset=0)
    assert total >= 5


async def test_search_filters_by_country_code_sql(db_session):
    """search(country_code=X) must return only clusters whose dominant_countries contains X.

    The filter is applied in SQL (not Python-side), so clusters without the country
    must be excluded even when the table has many rows.
    """
    repo = ClusterRepository(db_session)
    await repo.bulk_upsert(
        [
            _make_cluster("match", 7.0, ["US", "IR"]),
            _make_cluster("no_match", 8.0, ["FR", "DE"]),
            _make_cluster("no_countries", 6.0, []),
        ]
    )
    await db_session.commit()

    clusters, total = await repo.search(country_code="US")
    ids = [c.cluster_id for c in clusters]
    assert "match" in ids
    assert "no_match" not in ids
    assert "no_countries" not in ids
    assert total == 1


async def test_search_country_code_total_reflects_sql_filter(db_session):
    """total returned with country_code filter equals rows matching in DB, not all rows."""
    repo = ClusterRepository(db_session)
    await repo.bulk_upsert(
        [
            _make_cluster("ir1", 5.0, ["IR"]),
            _make_cluster("ir2", 4.0, ["IR"]),
            _make_cluster("us1", 3.0, ["US"]),
        ]
    )
    await db_session.commit()

    _, total = await repo.search(country_code="IR")
    assert total == 2


async def test_delete_computed_before_removes_old_clusters(db_session):
    """delete_computed_before removes clusters with computed_at < cutoff."""
    repo = ClusterRepository(db_session)
    old_cluster = _make_cluster("old_c", 1.0)
    old_cluster["computed_at"] = datetime(2026, 1, 1, tzinfo=UTC)
    await repo.upsert(old_cluster)
    await db_session.commit()

    deleted = await repo.delete_computed_before(datetime(2026, 6, 1, tzinfo=UTC))
    assert deleted >= 1

    clusters, _ = await repo.search()
    assert all(c.cluster_id != "old_c" for c in clusters)


async def test_upsert_and_retrieve_event_date_ref_fields(db_session):
    """Upserting a cluster with event_date_ref_start/end must persist and round-trip correctly."""
    repo = ClusterRepository(db_session)
    cluster = _make_cluster("date_ref_c1", 5.0, ["US"])
    cluster["event_date_ref_start"] = 20260305
    cluster["event_date_ref_end"] = 20260310
    await repo.upsert(cluster)
    await db_session.commit()

    clusters, _ = await repo.search()
    row = next((c for c in clusters if c.cluster_id == "date_ref_c1"), None)
    assert row is not None
    assert row.event_date_ref_start == 20260305
    assert row.event_date_ref_end == 20260310


async def test_upsert_event_date_ref_fields_null_by_default(db_session):
    """A cluster upserted without event_date_ref fields must store NULL for both."""
    repo = ClusterRepository(db_session)
    await repo.upsert(_make_cluster("date_ref_null", 4.0))
    await db_session.commit()

    clusters, _ = await repo.search()
    row = next((c for c in clusters if c.cluster_id == "date_ref_null"), None)
    assert row is not None
    assert row.event_date_ref_start is None
    assert row.event_date_ref_end is None


async def test_cluster_repository_ignores_merge_evidence_transient_key(db_session):
    repo = ClusterRepository(db_session)
    cluster = _make_cluster("c-merge-evidence", 7.0)
    cluster["merge_evidence"] = [{"mention_overlap": 2}]
    await repo.upsert(cluster)
    await db_session.commit()

    clusters, total = await repo.search()
    assert total >= 1
    assert any(cluster.cluster_id == "c-merge-evidence" for cluster in clusters)
