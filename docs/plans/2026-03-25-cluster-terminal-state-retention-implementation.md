# Cluster Terminal State Retention Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add production-safe cleanup for terminal cluster components by deactivating memberships immediately, deleting old `merged`/`split` components after a 7-day configurable retention window, and hardening soft-link access with a composite index.

**Architecture:** Extend the existing cluster-component persistence layer instead of introducing archive storage. Keep short-term auditability inside the current tables, but treat `merged` and `split` rows as operationally dead: deactivate their memberships on transition, exclude them from active-membership reads, and remove them with a dedicated scheduled cleanup job after retention expires.

**Tech Stack:** Python 3.11, async SQLAlchemy, Alembic, APScheduler, pytest, Ruff.

---

### Task 1: Add failing repository tests for terminal transitions

**Files:**
- Modify: `tests/test_cluster_component_repository.py`
- Modify: `app/db/repositories/cluster_component_repository.py`

**Step 1: Write the failing test**

Add tests that prove:

- `mark_merged_into()` deactivates active memberships and clears soft-link fields;
- `mark_split()` deactivates active memberships and clears soft-link fields;
- `list_active_event_membership()` excludes memberships attached to `merged` or `split`
  components.

Example test shape:

```python
async def test_repository_mark_merged_into_deactivates_memberships(db_session) -> None:
    repo = ClusterComponentRepository(db_session)
    now = datetime(2026, 3, 25, tzinfo=UTC)
    # seed one active component with active memberships
    await repo.mark_merged_into("component-1", "component-2", now)
    await db_session.commit()

    memberships = (
        await db_session.execute(
            select(ClusterComponentEvent).where(ClusterComponentEvent.component_id == "component-1")
        )
    ).scalars().all()

    assert all(membership.is_active is False for membership in memberships)
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cluster_component_repository.py -v`
Expected: FAIL because terminal transitions currently leave memberships active and reads include them.

**Step 3: Write minimal implementation**

In `app/db/repositories/cluster_component_repository.py`:

- add a private helper that deactivates all active memberships for one component and stamps
  `last_seen_at`;
- call it from `mark_merged_into()` and `mark_split()`;
- clear `current_cluster_id`, `current_table`, and `current_computed_at` in both transitions;
- change `list_active_event_membership()` to join `cluster_component_events` with
  `cluster_components` and keep only reconcilable statuses.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cluster_component_repository.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_cluster_component_repository.py app/db/repositories/cluster_component_repository.py
git commit -m "fix: deactivate terminal cluster memberships"
```

### Task 2: Add retention setting and index coverage tests

**Files:**
- Modify: `app/core/config.py`
- Modify: `app/db/models.py`
- Create: `alembic/versions/015_add_cluster_terminal_retention_and_soft_link_index.py`
- Modify: `tests/test_cluster_component_model.py`

**Step 1: Write the failing test**

Add assertions that:

- settings expose `cluster_terminal_state_retention_days` with default `7`;
- the ORM table metadata defines an index on `current_table` and `current_cluster_id`.

Example test shape:

```python
def test_cluster_component_model_has_soft_link_index() -> None:
    indexes = {tuple(index.expressions.keys()) for index in ClusterComponent.__table__.indexes}
    assert ("current_table", "current_cluster_id") in indexes
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cluster_component_model.py -v`
Expected: FAIL because the setting and composite index do not exist yet.

**Step 3: Write minimal implementation**

- add `cluster_terminal_state_retention_days: int = Field(default=7, ge=1)` in
  `app/core/config.py`;
- add `Index("ix_cluster_components_current_table_cluster_id", "current_table", "current_cluster_id")`
  in `app/db/models.py`;
- create Alembic migration `015_add_cluster_terminal_retention_and_soft_link_index.py` that adds and
  drops the new index.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cluster_component_model.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add app/core/config.py app/db/models.py alembic/versions/015_add_cluster_terminal_retention_and_soft_link_index.py tests/test_cluster_component_model.py
git commit -m "chore: add cluster terminal retention config"
```

### Task 3: Add failing cleanup job tests

**Files:**
- Create: `tests/test_cluster_terminal_cleanup.py`
- Modify: `app/services/cluster_terminal_cleanup_service.py`
- Modify: `app/scheduler/scheduler.py`
- Modify: `tests/test_scheduler_config.py`

**Step 1: Write the failing test**

Create tests that prove:

- terminal components older than retention are deleted with their membership rows;
- fresh terminal components are preserved;
- `active` and `stale` components are preserved regardless of age;
- scheduler registers a dedicated terminal cleanup job.

Example test shape:

```python
async def test_run_cluster_terminal_cleanup_deletes_old_terminal_components(db_session, monkeypatch) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "cluster_terminal_state_retention_days", 7)
    result = await run_cluster_terminal_cleanup(db_session)
    assert result["deleted_components"] == 1
    assert result["deleted_memberships"] == 2
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cluster_terminal_cleanup.py tests/test_scheduler_config.py -v`
Expected: FAIL because the cleanup service and scheduler job do not exist yet.

**Step 3: Write minimal implementation**

- add a small service module, for example `app/services/cluster_terminal_cleanup_service.py`, that:
  - reads retention from `get_settings()`;
  - computes the UTC cutoff;
  - delegates deletes to repository methods;
  - logs start/end counts;
  - returns a small result dict.
- add a scheduler wrapper in `app/scheduler/scheduler.py` alongside existing retention jobs;
- register a dedicated APScheduler job id such as `gdelt_cluster_terminal_cleanup` on a daily
  interval.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cluster_terminal_cleanup.py tests/test_scheduler_config.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add app/services/cluster_terminal_cleanup_service.py app/scheduler/scheduler.py tests/test_cluster_terminal_cleanup.py tests/test_scheduler_config.py
git commit -m "feat: schedule cluster terminal cleanup"
```

### Task 4: Add repository deletion helpers under TDD

**Files:**
- Modify: `app/db/repositories/cluster_component_repository.py`
- Modify: `tests/test_cluster_component_repository.py`

**Step 1: Write the failing test**

Add tests for a repository helper that deletes only terminal components older than a cutoff and the
associated `cluster_component_events` rows.

Example test shape:

```python
async def test_repository_delete_terminal_components_before_cutoff(db_session) -> None:
    repo = ClusterComponentRepository(db_session)
    deleted = await repo.delete_terminal_components_before(cutoff)
    assert deleted == {"components": 1, "memberships": 2}
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cluster_component_repository.py -v`
Expected: FAIL because the deletion helper does not exist yet.

**Step 3: Write minimal implementation**

Implement focused repository methods such as:

- `list_terminal_component_ids_before(cutoff: datetime) -> list[str]`
- `delete_membership_for_component_ids(component_ids: list[str]) -> int`
- `delete_components_by_ids(component_ids: list[str]) -> int`
- or one compact `delete_terminal_components_before(cutoff: datetime) -> dict[str, int]`

Keep the implementation async, explicit, and SQLite/PostgreSQL compatible.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cluster_component_repository.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add app/db/repositories/cluster_component_repository.py tests/test_cluster_component_repository.py
git commit -m "feat: add cluster terminal cleanup repository support"
```

### Task 5: Run verification

**Files:**
- No code changes expected

**Step 1: Run focused tests**

Run:

```bash
pytest tests/test_cluster_component_repository.py tests/test_cluster_component_model.py tests/test_cluster_terminal_cleanup.py tests/test_scheduler_config.py tests/test_cluster_service.py -v
```

Expected: PASS

**Step 2: Run linter**

Run: `ruff check .`
Expected: PASS

**Step 3: Optional broader regression**

Run: `pytest -q`
Expected: PASS, if practical for current session.

**Step 4: Commit verification-only state if needed**

```bash
git status
```

Expected: clean working tree or only intentional uncommitted changes.
