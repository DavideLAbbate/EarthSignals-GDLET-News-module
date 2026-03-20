"""Graph-based cluster fusion using Union-Find over mention overlap and theme similarity."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import UTC, date, datetime
from statistics import mean
from typing import Any

from app.integrations.event_enrichment_mapper import compute_topic_score


# ── Union-Find ───────────────────────────────────────────────────────────────


class _UnionFind:
    """Union-Find (disjoint-set) with path compression."""

    def __init__(self, n: int) -> None:
        self._parent: list[int] = list(range(n))
        self._rank: list[int] = [0] * n

    def find(self, x: int) -> int:
        """Return root of x with path compression."""
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x])
        return self._parent[x]

    def union(self, x: int, y: int) -> None:
        """Merge the sets containing x and y by rank."""
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self._rank[rx] < self._rank[ry]:
            rx, ry = ry, rx
        self._parent[ry] = rx
        if self._rank[rx] == self._rank[ry]:
            self._rank[rx] += 1


# ── Public helper ─────────────────────────────────────────────────────────────


def _jaccard(a: set[str], b: set[str]) -> float:
    """Return Jaccard similarity between two sets; 0.0 for both empty."""
    union_size = len(a | b)
    if union_size == 0:
        return 0.0
    return len(a & b) / union_size


def _yyyymmdd_to_date(value: int) -> date:
    """Convert an 8-digit YYYYMMDD integer to a :class:`datetime.date`."""
    return datetime.strptime(str(value), "%Y%m%d").date()


# ── Merger ────────────────────────────────────────────────────────────────────

_LIST_FIELDS = (
    "mention_identifiers",
    "themes",
    "persons",
    "organizations",
    "gkg_locations",
    "event_ids",
    "distinct_mention_sources",
)

_SUM_INT_FIELDS = (
    "event_count",
    "num_articles",
    "num_mentions",
    "num_sources",
)

_DOMINANT_FIELDS = (
    "dominant_event_types",
    "dominant_quad_classes",
    "dominant_countries",
    "dominant_locations",
)


class ClusterMerger:
    """Fuse overlapping story clusters via mention-URL overlap and theme Jaccard similarity.

    Two gating conditions are evaluated before each union:

    * **Time-proximity gate** — the event date ranges (``event_date_ref_start`` /
      ``event_date_ref_end``) of the two components must be within
      ``max_merge_day_gap`` calendar days of each other.  Clusters reporting
      events from entirely different weeks cannot represent the same story.

    * **Shared action-type gate** — the two components must share at least one
      CAMEO root-code label in their ``dominant_event_types``.  A diplomacy
      cluster and a protest cluster are structurally different stories even if
      they share GKG themes.

    When either gate returns ``False`` the pair is skipped for that pass.
    If a cluster is missing the relevant field (``None`` / empty), the gate
    defaults to ``True`` (allow merge) so that legacy or sparse data is not
    inadvertently blocked.
    """

    def __init__(
        self,
        mention_overlap_min: int = 1,
        jaccard_threshold: float = 0.3,
        max_themes_for_jaccard: int | None = 50,
        max_merge_day_gap: int = 3,
        max_theme_df: float = 0.2,
    ) -> None:
        self._mention_overlap_min = mention_overlap_min
        self._jaccard_threshold = jaccard_threshold
        self._max_themes_for_jaccard = max_themes_for_jaccard
        self._max_merge_day_gap = max_merge_day_gap
        # max_theme_df: themes that appear in more than this fraction of all clusters
        # are excluded from the inverted index. A theme shared by 20%+ of clusters
        # (e.g. "UNITED_STATES", "ECONOMY") carries no discriminative signal and
        # would generate O(n²) candidate pairs on its own, defeating the index.
        self._max_theme_df = max_theme_df

    def merge(self, clusters: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Merge related clusters into fused representations.

        Returns a list of fused cluster dicts; each component is one entry.
        """
        if not clusters:
            return []

        uf = _UnionFind(len(clusters))

        # Per-component state — all three dicts are indexed by Union-Find root and
        # updated on every successful union so that gate checks remain O(α).
        self._comp_date_start: dict[int, int | None] = {
            i: c.get("event_date_ref_start") for i, c in enumerate(clusters)
        }
        self._comp_date_end: dict[int, int | None] = {
            i: c.get("event_date_ref_end") for i, c in enumerate(clusters)
        }
        self._comp_action_types: dict[int, set[str]] = {
            i: set(c.get("dominant_event_types") or []) for i, c in enumerate(clusters)
        }

        self._union_by_mention_overlap(clusters, uf)
        self._union_by_theme_jaccard(clusters, uf)

        components = self._collect_components(clusters, uf)
        return [self._fuse(group) for group in components.values()]

    # ── graph construction ───────────────────────────────────────────────────

    def _union_by_mention_overlap(
        self,
        clusters: list[dict[str, Any]],
        uf: _UnionFind,
    ) -> None:
        """Union pairs of clusters whose shared mention URL count >= mention_overlap_min.

        Skips the union if either gate (time-proximity, shared action-type) blocks it.
        """
        url_to_indices: dict[str, list[int]] = defaultdict(list)
        for i, cluster in enumerate(clusters):
            for mid in cluster.get("mention_identifiers") or []:
                url_to_indices[mid].append(i)

        pair_overlap: defaultdict[tuple[int, int], int] = defaultdict(int)
        for indices in url_to_indices.values():
            for a in range(len(indices)):
                for b in range(a + 1, len(indices)):
                    key = (min(indices[a], indices[b]), max(indices[a], indices[b]))
                    pair_overlap[key] += 1

        for (i, j), count in pair_overlap.items():
            if count >= self._mention_overlap_min:
                ri, rj = uf.find(i), uf.find(j)
                if ri == rj:
                    continue
                if not self._date_ranges_within_gap(ri, rj):
                    continue
                if not self._shares_action_type(ri, rj):
                    continue
                self._union_and_update_state(uf, i, j)

    def _union_by_theme_jaccard(
        self,
        clusters: list[dict[str, Any]],
        uf: _UnionFind,
    ) -> None:
        """Union pairs not yet connected whose theme Jaccard >= jaccard_threshold.

        Uses an inverted index on themes to build only the candidate pairs that share
        at least one theme, reducing the comparison set from O(n²) to O(k) where k is
        the number of co-occurring theme pairs — typically much smaller than n²/2 for
        real news data with sparse theme overlap.

        Each cluster's theme set is truncated to max_themes_for_jaccard items before
        computing similarity, preventing clusters with thousands of generic tags from
        generating spurious high-Jaccard matches.
        Skips the union if either gate (time-proximity, shared action-type) blocks it.
        """
        cap = self._max_themes_for_jaccard
        if cap is None:
            theme_sets = [set(c.get("themes") or []) for c in clusters]
        else:
            theme_sets = [set(list(c.get("themes") or [])[:cap]) for c in clusters]

        # Build an inverted index: theme → list of cluster indices that have it.
        # Themes that appear in more than max_theme_df of all clusters are excluded:
        # they carry no discriminative signal (like stopwords in IR) and would each
        # generate O(n²) candidate pairs, negating the benefit of the inverted index.
        n = len(clusters)
        # Minimum 2: a theme shared by only 1 cluster can never form a pair.
        # The percentage floor only kicks in when n is large enough that even
        # max_theme_df% of clusters produces a meaningful pair explosion.
        df_limit = max(2, int(n * self._max_theme_df))
        theme_to_indices: dict[str, list[int]] = defaultdict(list)
        for i, ts in enumerate(theme_sets):
            for theme in ts:
                theme_to_indices[theme].append(i)

        # Collect candidate pairs (i, j) that share at least one non-stopword theme.
        # Using a set avoids evaluating the same pair twice.
        candidate_pairs: set[tuple[int, int]] = set()
        for theme, indices in theme_to_indices.items():
            if len(indices) > df_limit:
                continue  # high-frequency theme — skip to avoid O(n²) explosion
            for a in range(len(indices)):
                for b in range(a + 1, len(indices)):
                    candidate_pairs.add((min(indices[a], indices[b]), max(indices[a], indices[b])))

        for i, j in candidate_pairs:
            ri, rj = uf.find(i), uf.find(j)
            if ri == rj:
                continue  # already merged — skip Jaccard check
            if _jaccard(theme_sets[i], theme_sets[j]) > self._jaccard_threshold:
                if not self._date_ranges_within_gap(ri, rj):
                    continue
                if not self._shares_action_type(ri, rj):
                    continue
                self._union_and_update_state(uf, i, j)

    # ── merge gates ──────────────────────────────────────────────────────────

    def _date_ranges_within_gap(self, ri: int, rj: int) -> bool:
        """Return True if the two components' event date ranges are within max_merge_day_gap days.

        Missing date data → allow merge (safe default).
        """
        s_i = self._comp_date_start.get(ri)
        e_i = self._comp_date_end.get(ri)
        s_j = self._comp_date_start.get(rj)
        e_j = self._comp_date_end.get(rj)
        if any(v is None for v in (s_i, e_i, s_j, e_j)):
            return True
        date_i_start = _yyyymmdd_to_date(s_i)  # type: ignore[arg-type]
        date_i_end = _yyyymmdd_to_date(e_i)  # type: ignore[arg-type]
        date_j_start = _yyyymmdd_to_date(s_j)  # type: ignore[arg-type]
        date_j_end = _yyyymmdd_to_date(e_j)  # type: ignore[arg-type]
        gap = max(0, (date_j_start - date_i_end).days, (date_i_start - date_j_end).days)
        return gap <= self._max_merge_day_gap

    def _shares_action_type(self, ri: int, rj: int) -> bool:
        """Return True if the two components share at least one dominant event type.

        Missing action-type data → allow merge (safe default).
        """
        types_i = self._comp_action_types.get(ri, set())
        types_j = self._comp_action_types.get(rj, set())
        if not types_i or not types_j:
            return True
        return bool(types_i & types_j)

    # ── state update ─────────────────────────────────────────────────────────

    def _union_and_update_state(self, uf: _UnionFind, i: int, j: int) -> None:
        """Merge components i and j and update per-component gate state.

        Must be called instead of uf.union directly so the date-range and
        action-type dicts stay consistent with the Union-Find structure.
        """
        ri, rj = uf.find(i), uf.find(j)
        if ri == rj:
            return
        uf.union(i, j)
        new_root = uf.find(i)

        # Merge date bounds — take the outer envelope.
        starts = [
            v
            for v in (self._comp_date_start.get(ri), self._comp_date_start.get(rj))
            if v is not None
        ]
        ends = [
            v for v in (self._comp_date_end.get(ri), self._comp_date_end.get(rj)) if v is not None
        ]
        self._comp_date_start[new_root] = min(starts) if starts else None
        self._comp_date_end[new_root] = max(ends) if ends else None

        # Merge action types — union of both sets.
        self._comp_action_types[new_root] = self._comp_action_types.get(
            ri, set()
        ) | self._comp_action_types.get(rj, set())

    # ── component extraction ─────────────────────────────────────────────────

    @staticmethod
    def _collect_components(
        clusters: list[dict[str, Any]], uf: _UnionFind
    ) -> dict[int, list[dict[str, Any]]]:
        """Group clusters by their Union-Find root."""
        components: dict[int, list[dict[str, Any]]] = {}
        for idx, cluster in enumerate(clusters):
            root = uf.find(idx)
            components.setdefault(root, []).append(cluster)
        return components

    # ── fusion ───────────────────────────────────────────────────────────────

    def _fuse(self, group: list[dict[str, Any]]) -> dict[str, Any]:
        """Fuse a group of related clusters into one representative dict."""
        if len(group) == 1:
            fused = dict(group[0])
            fused["computed_at"] = datetime.now(UTC)
            return fused

        anchor = max(group, key=lambda c: c["topic_score"])

        fused: dict[str, Any] = {
            "cluster_id": anchor["cluster_id"],
            "source_url": anchor["source_url"],
        }

        # List fields — sorted unique union
        for field in _LIST_FIELDS:
            fused[field] = _sorted_unique_union(group, field)

        # Summed integer fields (events don't overlap between source URLs)
        for field in _SUM_INT_FIELDS:
            fused[field] = sum(c.get(field) or 0 for c in group)

        # mention_count — derive from deduplicated mention_identifiers to avoid double-counting
        # shared mention URLs that triggered the merge
        fused["mention_count"] = len(fused["mention_identifiers"])

        # topic_score — recalculate from merged aggregates rather than taking max of pre-merge
        # scores, which ignores the compounding signal of the fused cluster
        fused["topic_score"] = compute_topic_score(
            event_count=fused["event_count"],
            num_articles=fused["num_articles"],
            num_mentions=fused["num_mentions"],
            num_sources=fused["num_sources"],
        )

        # avg_severity_score — unweighted mean (each source URL contributes one score)
        fused["avg_severity_score"] = _mean_of_non_none(group, "avg_severity_score")

        # document_tone_avg — weighted mean by gkg_doc_count to avoid mean-of-means bias
        # when sub-clusters have different numbers of GKG documents
        fused["document_tone_avg"] = _weighted_tone_avg(group)

        # Temporal boundaries (mention layer)
        first_times = [c["first_mention_at"] for c in group if c.get("first_mention_at")]
        last_times = [c["last_mention_at"] for c in group if c.get("last_mention_at")]
        fused["first_mention_at"] = min(first_times) if first_times else None
        fused["last_mention_at"] = max(last_times) if last_times else None

        # Event date range — outer envelope across all members
        starts = [
            m["event_date_ref_start"] for m in group if m.get("event_date_ref_start") is not None
        ]
        ends = [m["event_date_ref_end"] for m in group if m.get("event_date_ref_end") is not None]
        fused["event_date_ref_start"] = min(starts) if starts else None
        fused["event_date_ref_end"] = max(ends) if ends else None

        # Dominant fields — top-5 by frequency
        for field in _DOMINANT_FIELDS:
            all_values = [v for c in group for v in (c.get(field) or [])]
            fused[field] = _top_values(all_values)

        fused["computed_at"] = datetime.now(UTC)
        return fused


# ── module-level helpers ──────────────────────────────────────────────────────


def _sorted_unique_union(group: list[dict[str, Any]], field: str) -> list[str]:
    """Return sorted unique union of a list field across all clusters in a group."""
    return sorted({v for c in group for v in (c.get(field) or []) if v})


def _mean_of_non_none(group: list[dict[str, Any]], field: str) -> float | None:
    """Return mean of non-None float values for a field, or None if no values exist."""
    values = [c[field] for c in group if c.get(field) is not None]
    if not values:
        return None
    return round(mean(values), 2)


def _weighted_tone_avg(group: list[dict[str, Any]]) -> float | None:
    """Return a document-count-weighted mean of document_tone_avg across the group.

    Weights each sub-cluster's average by its ``gkg_doc_count`` so that larger
    sub-clusters (more GKG documents) contribute proportionally more to the fused
    tone, avoiding the mean-of-means statistical bias when groups differ in size.

    Falls back to an unweighted mean when no weight information is available.
    """
    pairs = [
        (c["document_tone_avg"], c.get("gkg_doc_count") or 0)
        for c in group
        if c.get("document_tone_avg") is not None
    ]
    if not pairs:
        return None
    total_weight = sum(w for _, w in pairs)
    if total_weight == 0:
        # No weight info — fall back to unweighted mean
        return round(mean(v for v, _ in pairs), 2)
    return round(sum(v * w for v, w in pairs) / total_weight, 2)


def _top_values(values: list[str], limit: int = 5) -> list[str]:
    """Return the top-N most common non-empty values in deterministic order."""
    counts = Counter(v for v in values if v and v != "Sconosciuto")
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [v for v, _ in ranked[:limit]]
