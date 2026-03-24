## Cluster Terminal State Retention Design

### Context

The persistent cluster-component layer correctly limits reconciliation to `active` and `stale`
components, but terminal states still accumulate indefinitely.

Today, `merged` and `split` components remain in `cluster_components`, their historical rows in
`cluster_component_events` are never deleted, and terminal transitions do not immediately disable
membership rows. This leaves two operational issues:

- storage and query cost grow over time for rows with no operational value;
- `list_active_event_membership()` can still read memberships belonging to terminal components,
  because it filters only on `cluster_component_events.is_active`.

The paper already documents retention for terminal states as an architectural requirement, so the
missing piece is implementation.

### Goals

- Disable memberships immediately when a component becomes `merged` or `split`.
- Delete terminal components and their membership history after a configurable retention window.
- Add scheduler wiring so cleanup runs automatically without manual intervention.
- Harden soft-link lookups with a composite index on `current_table` and `current_cluster_id`.

### Non-Goals

- Introduce archive tables or long-term historical storage.
- Change story/root materialization semantics.
- Add hard foreign keys from `cluster_components` to materialized cluster tables.

## Decisions

### Retention policy

- Strategy: delete-after-retention.
- Default retention: 7 days.
- Configurability: allow overriding through settings, with 14 days as an acceptable operational
  buffer when needed.

Seven days matches the current operational window: once a component has stayed terminal for longer
than a week, it no longer helps the live newsroom clustering workflow.

### Terminal-state semantics

When a component transitions to `merged` or `split`:

- set the component status as today;
- clear its soft materialization reference (`current_cluster_id`, `current_table`,
  `current_computed_at`);
- deactivate all active rows in `cluster_component_events` for that component and timestamp the
  transition.

This preserves short-lived auditability during the retention window while preventing terminal
components from polluting active-membership reads.

### Active-membership reads

`list_active_event_membership()` should join against `cluster_components` and include only
components whose status is operationally active for reconciliation (`active`, `stale`).

Immediate deactivation on transition is the first defense; filtering by component status is the
second defense. Together they keep the read path correct even if historical data predates the fix.

### Garbage-collection job

Add a dedicated cleanup path for cluster terminal states, separate from ingestion retention.

The job should:

- compute a UTC cutoff from `cluster_terminal_state_retention_days`;
- select `merged` and `split` components whose `last_seen_at` is older than the cutoff;
- delete related `cluster_component_events` rows first;
- delete the terminal `cluster_components` rows;
- log counts for observability.

The scheduler should register this as its own recurring job so the cleanup lifecycle is explicit and
operationally visible.

### Index hardening

Add a composite index on `cluster_components(current_table, current_cluster_id)`.

This does not change correctness, but it supports current and future soft-link existence checks and
cleanup/query patterns more efficiently than separate scans on unindexed columns.

## Affected Areas

- `app/core/config.py` for retention settings.
- `app/db/models.py` and a new Alembic migration for index changes.
- `app/db/repositories/cluster_component_repository.py` for terminal transitions, active-membership
  filtering, and terminal deletion helpers.
- `app/services/cluster_service.py` only where terminal transitions already happen.
- `app/services/` and `app/scheduler/` for the new garbage-collection job.
- `tests/test_cluster_component_repository.py`, `tests/test_cluster_service.py`,
  `tests/test_scheduler_config.py`, and a new targeted cleanup test module.

## Testing Strategy

- Repository tests for immediate membership deactivation on `merged` and `split`.
- Repository or service tests proving `list_active_event_membership()` excludes terminal
  components.
- Service/job tests proving terminal components older than retention are deleted while newer ones
  are preserved.
- Scheduler configuration tests proving the dedicated cleanup job is registered.
- Migration/model coverage for the new composite index.
