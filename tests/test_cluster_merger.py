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


# ── max_themes_for_jaccard ────────────────────────────────────────────────────


def test_jaccard_cap_blocks_merge_caused_by_tail_overlap():
    """The theme cap must prevent merges driven purely by shared themes beyond position N.

    c1 = [UNIQUE_A_0..49, SHARED_0..49]  (100 themes, shared block is in positions 50-99)
    c2 = [UNIQUE_B_0..49, SHARED_0..49]  (100 themes, shared block is in positions 50-99)

    Without cap: Jaccard = 50 / (100+100-50) = 50/150 ≈ 0.33 > 0.30 → would merge.
    With cap=50: truncated sets are [UNIQUE_A_0..49] and [UNIQUE_B_0..49] → Jaccard = 0.0
    → must NOT merge.
    """
    unique_a = [f"UNIQUE_A_{i}" for i in range(50)]
    unique_b = [f"UNIQUE_B_{i}" for i in range(50)]
    shared = [f"SHARED_{i}" for i in range(50)]
    c1 = _make_cluster("c1", ["https://url-a.com/1"], unique_a + shared)
    c2 = _make_cluster("c2", ["https://url-b.com/2"], unique_b + shared)
    merger = ClusterMerger(
        mention_overlap_min=1,
        jaccard_threshold=0.3,
        max_themes_for_jaccard=50,
    )
    result = merger.merge([c1, c2])
    assert len(result) == 2


def test_jaccard_cap_does_not_block_merge_without_cap():
    """Without cap (max_themes_for_jaccard=None), tail-overlap clusters must still merge."""
    unique_a = [f"UNIQUE_A_{i}" for i in range(50)]
    unique_b = [f"UNIQUE_B_{i}" for i in range(50)]
    shared = [f"SHARED_{i}" for i in range(50)]
    # raw Jaccard = 50/150 ≈ 0.33 > 0.30 → must merge
    c1 = _make_cluster("c1", ["https://url-a.com/1"], unique_a + shared)
    c2 = _make_cluster("c2", ["https://url-b.com/2"], unique_b + shared)
    merger = ClusterMerger(
        mention_overlap_min=1,
        jaccard_threshold=0.3,
        max_themes_for_jaccard=None,
    )
    result = merger.merge([c1, c2])
    assert len(result) == 1


def test_jaccard_still_merges_small_theme_sets_under_cap():
    """Clusters with theme count below max_themes_for_jaccard must still merge when Jaccard qualifies."""
    themes_a = ["IRAN", "WAR", "OIL", "MILITARY"]
    themes_b = ["IRAN", "WAR", "SANCTIONS"]  # Jaccard = 2/5 = 0.4 > 0.3
    c1 = _make_cluster("c1", ["https://url-a.com/1"], themes_a)
    c2 = _make_cluster("c2", ["https://url-b.com/2"], themes_b)
    merger = ClusterMerger(
        mention_overlap_min=1,
        jaccard_threshold=0.3,
        max_themes_for_jaccard=50,
    )
    result = merger.merge([c1, c2])
    assert len(result) == 1


# ── max_cluster_size ──────────────────────────────────────────────────────────


def test_merge_blocked_when_result_exceeds_max_cluster_size():
    """A merge that would produce a cluster exceeding max_cluster_size must be skipped."""
    # c1 already has 1500 events; fusing with c2 (600) would give 2100 > 2000
    c1 = _make_cluster("c1", ["https://shared.com/x"], ["IRAN"], event_count=1500)
    c2 = _make_cluster("c2", ["https://shared.com/x"], ["IRAN"], event_count=600)
    merger = ClusterMerger(
        mention_overlap_min=1,
        jaccard_threshold=0.3,
        max_cluster_size=2000,
    )
    result = merger.merge([c1, c2])
    assert len(result) == 2


def test_merge_allowed_when_result_within_max_cluster_size():
    """A merge that stays within max_cluster_size must proceed normally."""
    c1 = _make_cluster("c1", ["https://shared.com/x"], ["IRAN"], event_count=800)
    c2 = _make_cluster("c2", ["https://shared.com/x"], ["IRAN"], event_count=600)
    merger = ClusterMerger(
        mention_overlap_min=1,
        jaccard_threshold=0.3,
        max_cluster_size=2000,
    )
    result = merger.merge([c1, c2])
    assert len(result) == 1
    assert result[0]["event_count"] == 1400


def test_max_cluster_size_none_disables_cap():
    """When max_cluster_size is None the size cap must not block any merge."""
    c1 = _make_cluster("c1", ["https://shared.com/x"], ["IRAN"], event_count=5000)
    c2 = _make_cluster("c2", ["https://shared.com/x"], ["IRAN"], event_count=5000)
    merger = ClusterMerger(
        mention_overlap_min=1,
        jaccard_threshold=0.3,
        max_cluster_size=None,
    )
    result = merger.merge([c1, c2])
    assert len(result) == 1
