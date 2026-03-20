# Cluster Merge Redesign — Design Document

**Date**: 2026-03-19  
**Status**: Approved

---

## Problem Statement

The current clustering pipeline caps merged cluster size at 2 000 `event_count` units
(`cluster_merger.py:87`). This cap was introduced to bound memory and query cost but
has the opposite semantic effect: genuinely large breaking-news stories (observed up to
25 000 events) are arbitrarily split into disconnected fragments. The cap says nothing
about whether two clusters are actually related.

Simultaneously, the merge signals (mention URL overlap, theme Jaccard) are time-blind
and action-type-blind. Two clusters from different days or covering different categories
of events (protests vs. diplomacy) can be merged purely on shared GKG themes.

---

## Goals

1. Remove the 2 000-event cap entirely.
2. Add a time-proximity gate: clusters whose event date ranges are more than N calendar
   days apart are ineligible to merge (default N = 3, configurable).
3. Add a shared-action-type gate: clusters that share no CAMEO root-code label in their
   `dominant_event_types` are ineligible to merge.
4. Track new columns `event_date_ref_start` and `event_date_ref_end` on `story_clusters`
   to record the calendar span of the underlying GDELT events (not the mention span).

---

## Non-Goals

- No change to scoring (`topic_score` formula).
- No change to GKG, mentions, or event ingestion pipelines.
- No UI / frontend changes.

---

## Approach: Gated Union-Find

Keep the existing Union-Find (path-compressed, union-by-rank) algorithm.
Add two new pre-merge gates evaluated before every `union()` call, in both
the mention-overlap pass and the theme-Jaccard pass.

---

## Schema Changes

### New columns on `story_clusters`

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `event_date_ref_start` | `Integer` | YES | `min(sql_date)` across all events in the cluster (YYYYMMDD) |
| `event_date_ref_end` | `Integer` | YES | `max(sql_date)` across all events in the cluster (YYYYMMDD) |

`sql_date` is the 8-digit YYYYMMDD integer already on `gdelt_events`, used by the
existing date-range search filters.

### Migration

New Alembic migration `011_add_event_date_ref_range_to_story_clusters.py`:

```sql
ALTER TABLE story_clusters ADD COLUMN event_date_ref_start INTEGER;
ALTER TABLE story_clusters ADD COLUMN event_date_ref_end INTEGER;
```

Both columns nullable to avoid breaking pre-existing rows.

---

## Cluster Building (`cluster_service.py` — `_build_cluster`)

Add to the per-candidate assembly step:

```python
cluster["event_date_ref_start"] = min(e.sql_date for e in events) if events else None
cluster["event_date_ref_end"]   = max(e.sql_date for e in events) if events else None
```

---

## Cluster Merger Changes (`cluster_merger.py`)

### Remove

- `max_cluster_size` constructor parameter (or mark deprecated / default `None`).
- `_would_exceed_size_cap()` method.
- `component_sizes` dict (was only used for the cap).

### New per-component state (initialized before any pass, updated on every union)

```
component_date_start: dict[int, int | None]   — min sql_date for component
component_date_end:   dict[int, int | None]   — max sql_date for component
component_action_types: dict[int, set[str]]   — union of dominant_event_types for component
```

All three are initialized from each cluster's pre-built fields.
After each `union(ri, rj)`, the merged root gets:

```python
component_date_start[new_root] = min(s for s in [...] if s is not None)
component_date_end[new_root]   = max(e for e in [...] if e is not None)
component_action_types[new_root] = component_action_types[ri] | component_action_types[rj]
```

### Gate 1 — Time proximity

```python
def _date_ranges_within_gap(self, ri: int, rj: int) -> bool:
    """Return True if the two components' event date ranges are within max_merge_day_gap days."""
    s_i = self._comp_date_start.get(ri)
    e_i = self._comp_date_end.get(ri)
    s_j = self._comp_date_start.get(rj)
    e_j = self._comp_date_end.get(rj)
    if any(v is None for v in (s_i, e_i, s_j, e_j)):
        return True   # missing data → allow merge (safe default)
    date_i_start = _yyyymmdd_to_date(s_i)
    date_i_end   = _yyyymmdd_to_date(e_i)
    date_j_start = _yyyymmdd_to_date(s_j)
    date_j_end   = _yyyymmdd_to_date(e_j)
    gap = max(0, (date_j_start - date_i_end).days, (date_i_start - date_j_end).days)
    return gap <= self._max_merge_day_gap
```

### Gate 2 — Shared action type

```python
def _shares_action_type(self, ri: int, rj: int) -> bool:
    """Return True if the two components share at least one dominant event type."""
    types_i = self._comp_action_types.get(ri, set())
    types_j = self._comp_action_types.get(rj, set())
    if not types_i or not types_j:
        return True   # missing data → allow merge (safe default)
    return bool(types_i & types_j)
```

Both gates are checked in the existing pre-union guard block that already
handles `_would_exceed_size_cap`. If either gate returns `False`, the pair is skipped.

### New constructor parameter

```python
max_merge_day_gap: int = 3
```

Passed from `cluster_service.py` via `ClusterMerger(mention_overlap_min=2,
jaccard_threshold=0.3, max_merge_day_gap=settings.cluster_max_merge_day_gap)`.

---

## Config Changes (`app/core/config.py`)

```python
cluster_max_merge_day_gap: int = Field(default=3, ge=0)
```

Env var: `CLUSTER_MAX_MERGE_DAY_GAP`.

---

## Fusion Changes (`cluster_merger.py` — `_fuse`)

Add to the fuse output:

```python
fused["event_date_ref_start"] = min(m["event_date_ref_start"] for m in members if m.get("event_date_ref_start") is not None)
fused["event_date_ref_end"]   = max(m["event_date_ref_end"]   for m in members if m.get("event_date_ref_end")   is not None)
```

---

## API Schema Changes (`app/schemas/clusters.py`)

```python
event_date_ref_start: int | None = None
event_date_ref_end:   int | None = None
```

Added to the `StoryClusterResponse` (or equivalent) schema. Both optional.

---

## Testing Plan

### `test_cluster_merger.py`

| Test | Assert |
|---|---|
| Clusters >3 days apart with shared themes | Not merged |
| Clusters >3 days apart with shared mentions | Not merged |
| Clusters within 3 days, no shared action type | Not merged |
| Clusters within 3 days, shared action type, shared theme Jaccard | Merged |
| Cluster growing past 2 000 events | Allowed (no cap) |
| Union updates date bounds and action types correctly | Verified via component state |

### `test_cluster_service.py`

| Test | Assert |
|---|---|
| `_build_cluster` with known events | `event_date_ref_start` = min `sql_date`, `event_date_ref_end` = max `sql_date` |
| Fused cluster | date range is outer envelope of all members |

### `test_story_cluster_model.py`

Verify `event_date_ref_start` and `event_date_ref_end` columns exist in ORM model.

### `test_cluster_repository.py`

Upsert a cluster with both new fields; read back; assert values match.

---

## Rollout Notes

- Migration is additive (two nullable columns). Safe to deploy before the service change.
- On first cluster run after deploy, all clusters will be rebuilt with new fields populated.
- No backfill needed: the 36-hour rolling window ensures clusters are rebuilt within one cycle.
- The removed `max_cluster_size` parameter is a breaking change to the `ClusterMerger`
  public interface. Any callers outside the test suite must be audited.
