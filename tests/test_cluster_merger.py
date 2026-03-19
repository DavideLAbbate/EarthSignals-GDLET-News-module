"""Unit tests for ClusterMerger — graph-based cluster fusion."""

from __future__ import annotations

import pytest

from app.integrations.event_enrichment_mapper import compute_topic_score
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


def test_merged_cluster_mention_count_is_deduplicated():
    """Fused mention_count equals the number of unique mention URLs, not the naive sum.

    When two clusters share a mention URL, naively summing mention_count would double-count
    that URL. The correct value is len(deduplicated mention_identifiers).
    """
    # c1 has 2 mention URLs; c2 has 2, one of which is shared with c1
    c1 = _make_cluster(
        "c1",
        ["https://shared.com/x", "https://unique-a.com/y"],
        ["IRAN"],
        mention_count=2,
    )
    c2 = _make_cluster(
        "c2",
        ["https://shared.com/x", "https://unique-b.com/z"],
        ["IRAN"],
        mention_count=2,
    )
    merger = ClusterMerger(mention_overlap_min=1, jaccard_threshold=0.3)
    result = merger.merge([c1, c2])
    # 3 unique URLs: shared + unique-a + unique-b
    assert result[0]["mention_count"] == 3


def test_merged_cluster_document_tone_weighted_by_gkg_doc_count():
    """document_tone_avg after merge is a gkg_doc_count-weighted mean, not a mean-of-means.

    A cluster with 9 GKG documents should outweigh one with 1 GKG document by 9:1.
    """
    c1 = _make_cluster("c1", ["https://shared.com/x"], ["IRAN"])
    c1["document_tone_avg"] = -2.0
    c1["gkg_doc_count"] = 9  # 9 documents

    c2 = _make_cluster("c2", ["https://shared.com/x"], ["IRAN"])
    c2["document_tone_avg"] = -10.0
    c2["gkg_doc_count"] = 1  # 1 document

    merger = ClusterMerger(mention_overlap_min=1, jaccard_threshold=0.3)
    result = merger.merge([c1, c2])
    # Weighted mean: (9 * -2.0 + 1 * -10.0) / 10 = -28/10 = -2.8
    assert result[0]["document_tone_avg"] == pytest.approx(-2.8)


def test_merged_cluster_document_tone_falls_back_to_unweighted_when_no_gkg_doc_count():
    """When gkg_doc_count is missing/zero, document_tone_avg falls back to unweighted mean."""
    c1 = _make_cluster("c1", ["https://shared.com/x"], ["IRAN"])
    c1["document_tone_avg"] = -4.0
    # no gkg_doc_count key

    c2 = _make_cluster("c2", ["https://shared.com/x"], ["IRAN"])
    c2["document_tone_avg"] = -8.0
    # no gkg_doc_count key

    merger = ClusterMerger(mention_overlap_min=1, jaccard_threshold=0.3)
    result = merger.merge([c1, c2])
    # Unweighted mean: (-4.0 + -8.0) / 2 = -6.0
    assert result[0]["document_tone_avg"] == pytest.approx(-6.0)


def test_merged_cluster_recalculates_topic_score():
    """Fused cluster topic_score is recalculated from merged aggregates, not the pre-merge max.

    The merged cluster should have a higher score than either individual cluster because
    the combined event_count / num_articles / num_mentions / num_sources signals are larger.
    """
    c1 = _make_cluster(
        "c1",
        ["https://shared.com/x"],
        ["IRAN"],
        topic_score=5.5,
        event_count=10,
        num_articles=100,
        num_mentions=200,
        num_sources=20,
    )
    c2 = _make_cluster(
        "c2",
        ["https://shared.com/x"],
        ["IRAN"],
        topic_score=4.2,
        event_count=8,
        num_articles=80,
        num_mentions=150,
        num_sources=15,
    )
    merger = ClusterMerger(mention_overlap_min=1, jaccard_threshold=0.3)
    result = merger.merge([c1, c2])
    expected_score = compute_topic_score(
        event_count=18,
        num_articles=180,
        num_mentions=350,
        num_sources=35,
    )
    assert result[0]["topic_score"] == pytest.approx(expected_score)


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


# ── max_theme_df ─────────────────────────────────────────────────────────────


def test_high_frequency_theme_does_not_prevent_merge_for_small_inputs():
    """With only 2 clusters, a shared theme must not be filtered out by the df cap.

    df_limit = max(2, int(2 * 0.2)) = 2, so a theme shared by exactly 2 clusters
    is at the boundary and must still form a candidate pair.
    """
    c1 = _make_cluster("c1", ["https://url-a.com/1"], ["IRAN", "WAR", "OIL"])
    c2 = _make_cluster("c2", ["https://url-b.com/2"], ["IRAN", "WAR", "SANCTIONS"])
    merger = ClusterMerger(mention_overlap_min=1, jaccard_threshold=0.3, max_theme_df=0.2)
    result = merger.merge([c1, c2])
    assert len(result) == 1


def test_high_frequency_theme_is_excluded_from_index_with_many_clusters():
    """A theme shared by more than max_theme_df of clusters must be excluded from the index.

    With 10 clusters and max_theme_df=0.2 → df_limit = max(2, int(10*0.2)) = 2.
    A theme shared by 3+ clusters is excluded; only themes shared by ≤2 clusters
    form candidate pairs. Two clusters that share ONLY the high-frequency theme
    must NOT be fused via Jaccard (they may still fuse via mention overlap).
    """
    # c1..c8: share "COMMON_THEME" among all 10, plus a unique theme each
    # c9 and c10: share only "COMMON_THEME" — should NOT fuse via Jaccard
    clusters = [
        _make_cluster(f"c{i}", [f"https://unique-{i}.com/"], ["COMMON_THEME", f"UNIQUE_{i}"])
        for i in range(8)
    ]
    c_a = _make_cluster("ca", ["https://ca.com/"], ["COMMON_THEME"])
    c_b = _make_cluster("cb", ["https://cb.com/"], ["COMMON_THEME"])
    merger = ClusterMerger(
        mention_overlap_min=2,  # disable mention overlap (each URL is unique)
        jaccard_threshold=0.3,
        max_theme_df=0.2,
    )
    result = merger.merge(clusters + [c_a, c_b])
    # ca and cb share only COMMON_THEME which is excluded → must remain separate
    result_ids = {c["cluster_id"] for c in result}
    assert "ca" in result_ids
    assert "cb" in result_ids


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
