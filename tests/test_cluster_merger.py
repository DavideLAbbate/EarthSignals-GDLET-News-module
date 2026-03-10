"""Unit tests for ClusterMerger — graph-based cluster fusion."""

from __future__ import annotations

from app.services.cluster_merger import ClusterMerger, _jaccard


# ── _jaccard helper ─────────────────────────────────────────────────────────


def test_jaccard_identical_sets():
    assert _jaccard({"a", "b"}, {"a", "b"}) == 1.0


def test_jaccard_disjoint_sets():
    assert _jaccard({"a"}, {"b"}) == 0.0


def test_jaccard_partial_overlap():
    # |{a,b} ∩ {b,c}| / |{a,b} ∪ {b,c}| = 1/3
    assert abs(_jaccard({"a", "b"}, {"b", "c"}) - 1 / 3) < 1e-9


def test_jaccard_empty_sets():
    assert _jaccard(set(), set()) == 0.0


# ── helpers ──────────────────────────────────────────────────────────────────


def _make_cluster(cluster_id: str, mention_ids: list[str], themes: list[str], **kwargs):
    return {
        "cluster_id": cluster_id,
        "source_url": f"https://example.com/{cluster_id}",
        "mention_identifiers": mention_ids,
        "themes": themes,
        "event_count": kwargs.get("event_count", 1),
        "num_articles": kwargs.get("num_articles", 0),
        "num_mentions": kwargs.get("num_mentions", 0),
        "num_sources": kwargs.get("num_sources", 0),
        "topic_score": kwargs.get("topic_score", 5.0),
        "event_ids": kwargs.get("event_ids", []),
        "dominant_event_types": kwargs.get("dominant_event_types", []),
        "dominant_quad_classes": kwargs.get("dominant_quad_classes", []),
        "avg_severity_score": kwargs.get("avg_severity_score", None),
        "dominant_countries": kwargs.get("dominant_countries", []),
        "dominant_locations": kwargs.get("dominant_locations", []),
        "mention_count": kwargs.get("mention_count", 0),
        "distinct_mention_sources": kwargs.get("distinct_mention_sources", []),
        "first_mention_at": None,
        "last_mention_at": None,
        "persons": [],
        "organizations": [],
        "gkg_locations": [],
        "document_tone_avg": None,
        "computed_at": None,
    }


# ── ClusterMerger.merge ──────────────────────────────────────────────────────


def test_merge_clusters_sharing_mention_url():
    """Two clusters with one mention URL in common must be fused into one."""
    c1 = _make_cluster("c1", ["https://shared.com/article"], ["IRAN", "WAR"])
    c2 = _make_cluster("c2", ["https://shared.com/article", "https://other.com/x"], ["IRAN"])
    merger = ClusterMerger(mention_overlap_min=1, jaccard_threshold=0.3)
    result = merger.merge([c1, c2])
    assert len(result) == 1


def test_merge_clusters_via_jaccard_themes():
    """Two clusters with no shared mention URLs but Jaccard themes >= 0.3 must be fused."""
    themes_a = ["IRAN", "WAR", "OIL", "MILITARY"]
    themes_b = ["IRAN", "WAR", "SANCTIONS"]  # |inter|=2, |union|=5 → 0.4 >= 0.3
    c1 = _make_cluster("c1", ["https://url-a.com/1"], themes_a)
    c2 = _make_cluster("c2", ["https://url-b.com/2"], themes_b)
    merger = ClusterMerger(mention_overlap_min=1, jaccard_threshold=0.3)
    result = merger.merge([c1, c2])
    assert len(result) == 1


def test_merge_does_not_fuse_unrelated_clusters():
    """Clusters with no mention overlap and Jaccard < 0.3 must remain separate."""
    c1 = _make_cluster("c1", ["https://url-a.com/1"], ["IRAN", "WAR", "OIL"])
    c2 = _make_cluster("c2", ["https://url-b.com/2"], ["NIGERIA", "BANDITRY", "SECURITY"])
    merger = ClusterMerger(mention_overlap_min=1, jaccard_threshold=0.3)
    result = merger.merge([c1, c2])
    assert len(result) == 2


def test_merge_single_cluster_unchanged():
    c1 = _make_cluster("c1", ["https://url-a.com/1"], ["IRAN"])
    merger = ClusterMerger(mention_overlap_min=1, jaccard_threshold=0.3)
    result = merger.merge([c1])
    assert len(result) == 1


def test_merge_empty_list():
    merger = ClusterMerger(mention_overlap_min=1, jaccard_threshold=0.3)
    assert merger.merge([]) == []


def test_merged_cluster_aggregates_event_ids():
    """Fused cluster must contain event_ids from all source clusters."""
    c1 = _make_cluster("c1", ["https://shared.com/x"], ["IRAN"], event_ids=["1", "2"])
    c2 = _make_cluster("c2", ["https://shared.com/x"], ["IRAN"], event_ids=["3"])
    merger = ClusterMerger(mention_overlap_min=1, jaccard_threshold=0.3)
    result = merger.merge([c1, c2])
    assert set(result[0]["event_ids"]) == {"1", "2", "3"}


def test_merged_cluster_uses_highest_topic_score():
    """Fused cluster topic_score is the max among fused source clusters."""
    c1 = _make_cluster("c1", ["https://shared.com/x"], ["IRAN"], topic_score=5.5)
    c2 = _make_cluster("c2", ["https://shared.com/x"], ["IRAN"], topic_score=4.2)
    merger = ClusterMerger(mention_overlap_min=1, jaccard_threshold=0.3)
    result = merger.merge([c1, c2])
    assert result[0]["topic_score"] == 5.5


def test_merged_cluster_id_derived_from_highest_scoring_source():
    """Fused cluster_id must equal the cluster_id of the highest-scoring source cluster."""
    c1 = _make_cluster("c1", ["https://shared.com/x"], ["IRAN"], topic_score=5.5)
    c2 = _make_cluster("c2", ["https://shared.com/x"], ["IRAN"], topic_score=4.2)
    merger = ClusterMerger(mention_overlap_min=1, jaccard_threshold=0.3)
    result = merger.merge([c1, c2])
    assert result[0]["cluster_id"] == "c1"


def test_merged_cluster_unions_themes():
    """Fused cluster themes must be the sorted union of all source clusters."""
    c1 = _make_cluster("c1", ["https://shared.com/x"], ["IRAN", "WAR"])
    c2 = _make_cluster("c2", ["https://shared.com/x"], ["IRAN", "OIL"])
    merger = ClusterMerger(mention_overlap_min=1, jaccard_threshold=0.3)
    result = merger.merge([c1, c2])
    assert set(result[0]["themes"]) == {"IRAN", "WAR", "OIL"}


def test_merge_respects_mention_overlap_min():
    """Two clusters sharing exactly 1 URL must NOT be fused when min is 2."""
    c1 = _make_cluster("c1", ["https://shared.com/x"], ["IRAN"], topic_score=5.0)
    c2 = _make_cluster("c2", ["https://shared.com/x"], ["IRAN"], topic_score=4.0)
    merger = ClusterMerger(
        mention_overlap_min=2, jaccard_threshold=1.0
    )  # threshold=1.0 disables Jaccard
    result = merger.merge([c1, c2])
    assert len(result) == 2


def test_merge_single_cluster_sets_computed_at():
    """merge([c]) must return a cluster with computed_at set to a UTC datetime."""
    c1 = _make_cluster("c1", [], ["IRAN"])
    merger = ClusterMerger(mention_overlap_min=1, jaccard_threshold=0.3)
    result = merger.merge([c1])
    assert result[0]["computed_at"] is not None
    assert result[0]["computed_at"].tzinfo is not None  # timezone-aware
