"""Graph-based cluster fusion using Union-Find over mention overlap and theme similarity."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import UTC, datetime
from statistics import mean
from typing import Any


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
    "mention_count",
)

_DOMINANT_FIELDS = (
    "dominant_event_types",
    "dominant_quad_classes",
    "dominant_countries",
    "dominant_locations",
)


class ClusterMerger:
    """Fuse overlapping story clusters via mention-URL overlap and theme Jaccard similarity."""

    def __init__(
        self,
        mention_overlap_min: int = 1,
        jaccard_threshold: float = 0.3,
        max_themes_for_jaccard: int | None = 50,
        max_cluster_size: int | None = 2000,
    ) -> None:
        self._mention_overlap_min = mention_overlap_min
        self._jaccard_threshold = jaccard_threshold
        self._max_themes_for_jaccard = max_themes_for_jaccard
        self._max_cluster_size = max_cluster_size

    def merge(self, clusters: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Merge related clusters into fused representations.

        Returns a list of fused cluster dicts; each component is one entry.
        """
        if not clusters:
            return []

        uf = _UnionFind(len(clusters))
        self._union_by_mention_overlap(clusters, uf)
        self._union_by_theme_jaccard(clusters, uf)

        components = self._collect_components(clusters, uf)
        return [self._fuse(group) for group in components.values()]

    # ── graph construction ───────────────────────────────────────────────────

    def _union_by_mention_overlap(self, clusters: list[dict[str, Any]], uf: _UnionFind) -> None:
        """Union pairs of clusters whose shared mention URL count >= mention_overlap_min.

        Skips the union if the resulting component would exceed max_cluster_size.
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
                if self._would_exceed_size_cap(clusters, uf, i, j):
                    continue
                uf.union(i, j)

    def _union_by_theme_jaccard(self, clusters: list[dict[str, Any]], uf: _UnionFind) -> None:
        """Union pairs not yet connected whose theme Jaccard >= jaccard_threshold.

        Each cluster's theme set is truncated to max_themes_for_jaccard items before
        computing similarity, preventing clusters with thousands of generic tags from
        generating spurious high-Jaccard matches.
        Skips the union if the resulting component would exceed max_cluster_size.
        """
        n = len(clusters)
        cap = self._max_themes_for_jaccard
        if cap is None:
            theme_sets = [set(c.get("themes") or []) for c in clusters]
        else:
            theme_sets = [set(list(c.get("themes") or [])[:cap]) for c in clusters]

        for i in range(n):
            for j in range(i + 1, n):
                if uf.find(i) == uf.find(j):
                    continue  # already merged — skip expensive Jaccard check
                if _jaccard(theme_sets[i], theme_sets[j]) > self._jaccard_threshold:
                    if self._would_exceed_size_cap(clusters, uf, i, j):
                        continue
                    uf.union(i, j)

    def _would_exceed_size_cap(
        self, clusters: list[dict[str, Any]], uf: _UnionFind, i: int, j: int
    ) -> bool:
        """Return True if merging the components of i and j would exceed max_cluster_size."""
        if self._max_cluster_size is None:
            return False
        ri, rj = uf.find(i), uf.find(j)
        if ri == rj:
            return False
        size_i = sum(
            c.get("event_count") or 0 for idx, c in enumerate(clusters) if uf.find(idx) == ri
        )
        size_j = sum(
            c.get("event_count") or 0 for idx, c in enumerate(clusters) if uf.find(idx) == rj
        )
        return (size_i + size_j) > self._max_cluster_size

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

        # Summed integer fields
        for field in _SUM_INT_FIELDS:
            fused[field] = sum(c.get(field) or 0 for c in group)

        # topic_score — max
        fused["topic_score"] = max(c["topic_score"] for c in group)

        # Averaged float fields — mean of non-None values
        fused["avg_severity_score"] = _mean_of_non_none(group, "avg_severity_score")
        fused["document_tone_avg"] = _mean_of_non_none(group, "document_tone_avg")

        # Temporal boundaries
        first_times = [c["first_mention_at"] for c in group if c.get("first_mention_at")]
        last_times = [c["last_mention_at"] for c in group if c.get("last_mention_at")]
        fused["first_mention_at"] = min(first_times) if first_times else None
        fused["last_mention_at"] = max(last_times) if last_times else None

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


def _top_values(values: list[str], limit: int = 5) -> list[str]:
    """Return the top-N most common non-empty values in deterministic order."""
    counts = Counter(v for v in values if v and v != "Sconosciuto")
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [v for v, _ in ranked[:limit]]
