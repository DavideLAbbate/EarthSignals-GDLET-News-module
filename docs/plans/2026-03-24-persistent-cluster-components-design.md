## Persistent Cluster Components Design

### Context

The current clustering pipeline builds component candidates from windowed events and mentions, enriches them with component-local GKG data, merges related clusters with an in-memory Union-Find, and materialises the result into `story_clusters` or `root_clusters`.

This works for a single run but leaves several architectural gaps across successive runs:

- merge relations are not persisted across runs;
- representative anchors can drift when scores change;
- the scheduler window is anchored to wall clock rather than the latest ingested data;
- clusters without source-local GKG become semantically blind without explicit policy;
- story/root reconciliation is not validated with a post-run consistency check.

The design below introduces a persistent cluster-component layer as the historical source of truth while keeping `story_clusters` and `root_clusters` as current materialised projections for API consumption.

### Goals

- Preserve merge continuity across runs with an immutable component identity.
- Make split and merge transitions explicit and auditabile.
- Stabilise the representative anchor independently from score drift.
- Anchor processing windows to ingested data rather than wall clock.
- Keep merge evidence lightweight but explainable.
- Preserve current story/root API shape while improving internal consistency.

### Non-Goals

- Reconstruct the full internal state of every historical run.
- Replace `story_clusters` and `root_clusters` with a single public table.
- Make `current_cluster_id` a hard foreign key to a materialised table.

## Data Model

### New Source-of-Truth Table: `cluster_components`

Each row represents one persistent logical story component.

Suggested fields:

- `component_id`: immutable identifier assigned exactly once at first observation.
- `status`: one of `active`, `stale`, `split`, `merged_into`.
- `anchor_source_url`: representative URL fixed at creation unless changed by explicit policy.
- `anchor_locked_at`: timestamp when the anchor was fixed.
- `seed_event_ids`: snapshot of the event IDs present at first observation, for audit only.
- `first_seen_at`: first run in which the component was created.
- `last_seen_at`: latest run in which the component was observed as current.
- `missing_run_count`: number of successive runs where the component was not observed.
- `merged_into_component_id`: target canonical component when status is `merged_into`.
- `current_cluster_id`: soft reference to the current materialised cluster row.
- `current_table`: soft reference target, `story_clusters` or `root_clusters`.
- `current_computed_at`: timestamp of the materialised row currently referenced.
- `has_gkg`: whether the current component had any component-local GKG rows.
- `merge_evidence`: lightweight JSON explaining the latest merge or recent merges.
- `created_at`, `updated_at`: bookkeeping timestamps.

### Membership Tables

Persisting only the component header is not enough to detect splits or continuity. The design therefore adds at least one bridge table.

#### `cluster_component_events`

- `component_id`
- `event_id`
- `first_seen_at`
- `last_seen_at`
- `is_active`

This table is required for:

- continuity matching across runs;
- split detection;
- audit of component growth over time.

#### Optional `cluster_component_sources`

- `component_id`
- `source_url`
- `first_seen_at`
- `last_seen_at`
- `is_active`

This table is useful for audit and anchor diagnostics, but event membership is the minimum required for correctness.

## Component Identity

### `component_id`

`component_id` must be immutable by definition.

It is assigned exactly once when a component is first observed and must never be recomputed from the component's current `event_ids` or `source_urls`.

Recommended implementation:

- generate a UUID or ULID at component creation;
- store `seed_event_ids` separately for first-observation audit.

If hashing is ever used internally, it must be documented strictly as a hash of the first-observation event set, never of the current event set.

## Status Semantics

### `active`

The component was observed in the latest run with sufficient structural connectivity and is part of the current materialised output.

### `stale`

The component was not observed in the last `N` runs, but there is no explicit structural evidence that it was invalidated or split.

### `split`

The component previously existed as a single historical unit, but in the current run its historical event membership now maps to two or more distinct current components.

This status must be set only through explicit split detection logic.

### `merged_into`

The component is no longer canonical because it has been absorbed into another persistent component. The target is recorded in `merged_into_component_id`.

## Merge Evidence

`merge_evidence` is intended for lightweight audit, not full run reconstruction.

Recommended JSON payload shape:

```json
{
  "mention_overlap": 3,
  "jaccard": 0.41,
  "date_gap_days": 1,
  "shared_action_type": true
}
```

This should answer "why were these components merged?" without storing the entire pre-merge state.

Recommended storage policy:

- keep the latest evidence object, or
- keep a capped list of recent evidence objects, for example the latest 5.

The capped-list variant is preferred because it preserves a short audit trail without making the row unbounded.

## Relationship to `story_clusters` and `root_clusters`

`story_clusters` and `root_clusters` remain current materialised projections for API use.

They are not the historical memory of the system anymore.

`cluster_components` becomes the continuity layer across runs.

### Current Materialisation Reference

The relationship from `cluster_components` to the materialised output must be a soft reference, not a hard foreign key.

Use:

- `current_cluster_id`
- `current_table`
- optionally `current_computed_at`

This avoids fragile integrity constraints during story/root reconciliation, where a cluster can move between tables as part of the same run.

## Run Flow

### 1. Build Current Components

The pipeline continues to:

- collect events within the processing window;
- collect mentions for those events;
- derive connected components;
- apply structural candidate gates;
- enrich admitted components with component-local GKG;
- merge semantically related components.

This yields the set of current run components.

### 2. Match Current Components to Persistent Components

Each current component is matched against persisted components using overlap on `event_ids`.

Recommended matching outcomes:

- one clear historical match -> update that persistent component;
- no sufficient historical match -> create a new persistent component;
- multiple relevant historical matches -> choose one canonical persistent component and mark the others `merged_into` it.

Recommended canonicalisation rule for multi-match merges:

- choose the historical component with the oldest `first_seen_at`.

This avoids coupling identity continuity to moving scores.

### 3. Update Persistent State

For each matched or created persistent component:

- set `status=active`;
- update `last_seen_at`;
- reset `missing_run_count`;
- upsert current membership into `cluster_component_events`;
- update `current_cluster_id`, `current_table`, and `current_computed_at` after materialisation.

For historical components not matched in the current run:

- increment `missing_run_count`;
- change to `stale` once the configured threshold `N` is exceeded.

### 4. Split Detection

Split detection must be explicit.

For each historical component previously considered unitary:

- compare its active historical `event_ids` with current-run components;
- if those event IDs now map with substantial overlap to two or more distinct current components, mark the historical component as `split`.

To avoid false positives, split detection must use a configurable overlap threshold, absolute or percentage-based.

In the first implementation, once a historical component is detected as split:

- the old component remains for audit with `status=split`;
- the newly separated current branches are treated as new persistent components.

This keeps semantics clear and avoids forcing continuity onto ambiguous branches.

### 5. Merge Detection Across Historical Components

When multiple historical components converge into one current component:

- choose the oldest historical component as canonical;
- update it as `active`;
- mark the others as `merged_into` with `merged_into_component_id` set to the canonical component.

### 6. Anchor Stability

The representative anchor must be fixed at first observation and must not drift automatically with `topic_score` changes.

Default policy:

- assign `anchor_source_url` when the persistent component is created;
- do not change it on later runs;
- only change it through explicit fallback logic.

Allowed fallback triggers can include:

- anchor URL absent for `M` successive runs;
- anchor URL invalid or unavailable;
- explicit administrative override.

No score-based automatic anchor rotation is allowed.

## Processing Window

The scheduled clustering window should be anchored to the latest ingested data, not to wall clock.

Recommended scheduler logic:

1. query `MAX(gdelt_events.date_added)` as `latest_ingested_date_added`;
2. derive `since_dt` from that value minus a configurable overlap window;
3. process the closed interval bounded by available data.

Benefits:

- resilient to ingestion delays larger than the nominal buffer;
- avoids processing against incomplete wall-clock windows;
- better aligned with the actual state of the database.

Fallbacks:

- if no events exist, do nothing or fall back to current no-op behavior;
- if the max timestamp cannot be determined, log and fail explicitly rather than silently using wall clock.

## Components Without GKG

Components without component-local GKG must still be allowed.

Policy:

- materialise them with `has_gkg=false`;
- allow merge by mention overlap;
- treat theme-Jaccard merge as unavailable for that component;
- log this condition explicitly for observability.

This makes the behavior intentional and auditable rather than an undocumented side effect.

## Story/Root Consistency

The current materialisation split remains post-merge:

- active persistent components materialise into `story_clusters` when below the root threshold;
- active persistent components materialise into `root_clusters` when above the root threshold.

After upsert and cross-table reconciliation, the pipeline must run a consistency check.

Required checks:

- no `cluster_id` exists in both `story_clusters` and `root_clusters`;
- every active persistent component's `current_cluster_id/current_table` points to an existing materialised row.

Failure policy:

- emit a structured high-severity log event;
- fail the run so the inconsistency is not silent.

## Verification Strategy

Add or update tests for:

- immutable `component_id` across runs while event membership grows;
- anchor stability despite `topic_score` changes;
- split detection when one historical component becomes two current components;
- `merged_into` when two historical components converge;
- `stale` transition after `N` missed runs;
- soft-link correctness for `current_cluster_id/current_table` across story/root flips;
- scheduler window anchored to `MAX(date_added)` instead of `now`;
- scheduler path performs `commit` after successful materialisation;
- post-run consistency check catches duplicates across `story_clusters` and `root_clusters`;
- components without GKG remain materialisable and merge only via available signals.

## Implementation Notes

- The in-memory `ClusterMerger` can remain as the within-run merge engine.
- The new persistent layer must sit after current-run component construction and before final materialised references are considered canonical.
- Existing docs that still describe source-URL candidate scoring as the active design should be updated after implementation to avoid drift from the codebase.
