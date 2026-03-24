# Persistent Cluster Components Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Persist cluster continuity across runs with immutable component identities, stable anchors, explicit split/merge states, data-anchored scheduler windows, and post-run consistency checks.

**Architecture:** Add a persistent `cluster_components` layer plus event-membership history tables, then teach `ClusterService` to reconcile current-run merged components against persisted history before updating `story_clusters` and `root_clusters`. Keep the existing within-run component discovery and `ClusterMerger`, but move cross-run identity and auditability into repositories and a dedicated reconciliation path.

**Tech Stack:** Python 3.11, FastAPI, async SQLAlchemy, Alembic, PostgreSQL/SQLite test support, pytest, Ruff.

---

### Task 1: Add persistent component models

**Files:**
- Modify: `app/db/models.py`
- Test: `tests/test_story_cluster_model.py`
- Test: `tests/test_root_cluster_model.py`
- Create: `tests/test_cluster_component_model.py`

**Step 1: Write the failing model test**

Add tests that instantiate:

- `ClusterComponent` with immutable-style fields like `component_id`, `status`, `anchor_source_url`, `seed_event_ids`, `current_cluster_id`, `current_table`, `has_gkg`
- `ClusterComponentEvent` with `component_id`, `event_id`, `first_seen_at`, `last_seen_at`, `is_active`

Example test shape:

```python
def test_cluster_component_model_fields() -> None:
    from datetime import UTC, datetime

    from app.db.models import ClusterComponent

    row = ClusterComponent(
        component_id="01ARZ3NDEKTSV4RRFFQ69G5FAV",
        status="active",
        anchor_source_url="https://example.com/story",
        seed_event_ids=["1001", "1002"],
        first_seen_at=datetime(2026, 3, 24, tzinfo=UTC),
        last_seen_at=datetime(2026, 3, 24, tzinfo=UTC),
        current_cluster_id="cluster-1",
        current_table="story_clusters",
        has_gkg=True,
    )

    assert row.component_id == "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    assert row.status == "active"
    assert row.current_table == "story_clusters"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cluster_component_model.py -v`
Expected: FAIL because the new models do not exist yet.

**Step 3: Write minimal implementation**

In `app/db/models.py`:

- add `ClusterComponent` ORM model
- add `ClusterComponentEvent` ORM model
- use module-level docstring / import order conventions already present
- keep field types explicit and JSON-backed where appropriate

Recommended field set for `ClusterComponent`:

```python
component_id: Mapped[str] = mapped_column(String(36), primary_key=True)
status: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
anchor_source_url: Mapped[str] = mapped_column(Text, nullable=False)
anchor_locked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
seed_event_ids: Mapped[list | None] = mapped_column(JSON, nullable=True)
first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
missing_run_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
merged_into_component_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
current_cluster_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
current_table: Mapped[str | None] = mapped_column(String(30), nullable=True)
current_computed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
has_gkg: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
merge_evidence: Mapped[list | dict | None] = mapped_column(JSON, nullable=True)
created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))
updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))
```

Recommended field set for `ClusterComponentEvent`:

```python
id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
component_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
event_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
```

Add indexes for:

- `cluster_components.status`
- `cluster_components.merged_into_component_id`
- uniqueness on `(component_id, event_id)` in `cluster_component_events`

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cluster_component_model.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add app/db/models.py tests/test_cluster_component_model.py
git commit -m "feat: add persistent cluster component models"
```

### Task 2: Create Alembic migration for persistent component tables

**Files:**
- Create: `alembic/versions/014_add_cluster_components.py`
- Modify: `app/db/models.py` (only if migration reveals naming mismatch)
- Test: `tests/test_cluster_component_model.py`

**Step 1: Write the failing schema test**

Extend `tests/test_cluster_component_model.py` with a DB-backed repository-free smoke test that inserts a component and one event membership through the ORM session and verifies they round-trip.

```python
async def test_cluster_component_tables_round_trip(db_session) -> None:
    from datetime import UTC, datetime
    from sqlalchemy import select

    from app.db.models import ClusterComponent, ClusterComponentEvent

    now = datetime(2026, 3, 24, tzinfo=UTC)
    component = ClusterComponent(
        component_id="comp-1",
        status="active",
        anchor_source_url="https://example.com/story",
        anchor_locked_at=now,
        seed_event_ids=["1001"],
        first_seen_at=now,
        last_seen_at=now,
        current_cluster_id="cluster-1",
        current_table="story_clusters",
        has_gkg=False,
    )
    db_session.add(component)
    db_session.add(
        ClusterComponentEvent(
            component_id="comp-1",
            event_id="1001",
            first_seen_at=now,
            last_seen_at=now,
            is_active=True,
        )
    )
    await db_session.commit()

    rows = (await db_session.execute(select(ClusterComponent))).scalars().all()
    assert len(rows) == 1
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cluster_component_model.py::test_cluster_component_tables_round_trip -v`
Expected: FAIL if the DB schema has not been created consistently.

**Step 3: Write minimal implementation**

Create `alembic/versions/014_add_cluster_components.py` modeled after prior migrations. In `upgrade()`:

- create `cluster_components`
- create `cluster_component_events`
- add indexes / unique constraint on `(component_id, event_id)`

In `downgrade()`:

- drop indexes
- drop `cluster_component_events`
- drop `cluster_components`

Keep names deterministic and consistent with existing migration style.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cluster_component_model.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add alembic/versions/014_add_cluster_components.py tests/test_cluster_component_model.py
git commit -m "feat: add cluster component schema"
```

### Task 3: Add repository support for persistent components

**Files:**
- Create: `app/db/repositories/cluster_component_repository.py`
- Test: `tests/test_cluster_component_repository.py`

**Step 1: Write the failing repository tests**

Create repository tests for these behaviors:

- create a new component with membership rows
- fetch active/stale candidates for reconciliation
- update `current_cluster_id/current_table/current_computed_at`
- mark a component `merged_into`
- increment missing-run count and transition to `stale`
- replace active event membership for an observed component

Example test shape:

```python
async def test_repository_creates_component_with_events(db_session) -> None:
    repo = ClusterComponentRepository(db_session)
    component_id = await repo.create_component(
        anchor_source_url="https://example.com/story",
        seed_event_ids=["1001", "1002"],
        event_ids=["1001", "1002"],
        observed_at=datetime(2026, 3, 24, tzinfo=UTC),
        has_gkg=True,
        merge_evidence={"mention_overlap": 2},
    )

    component = await repo.get_by_component_id(component_id)
    assert component is not None
    assert component.status == "active"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cluster_component_repository.py -v`
Expected: FAIL because the repository does not exist yet.

**Step 3: Write minimal implementation**

Implement `ClusterComponentRepository` with focused methods, such as:

- `create_component(...) -> str`
- `get_by_component_id(component_id: str) -> ClusterComponent | None`
- `list_reconcilable_components() -> list[ClusterComponent]`
- `replace_active_event_membership(component_id: str, event_ids: list[str], observed_at: datetime) -> None`
- `mark_active(...) -> None`
- `mark_stale(component_id: str, missing_run_count: int) -> None`
- `mark_merged_into(component_id: str, target_component_id: str, observed_at: datetime) -> None`
- `mark_split(component_id: str, observed_at: datetime) -> None`
- `update_current_materialization(...) -> None`

Prefer small explicit queries over generic abstractions.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cluster_component_repository.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add app/db/repositories/cluster_component_repository.py tests/test_cluster_component_repository.py
git commit -m "feat: add cluster component repository"
```

### Task 4: Teach `ClusterMerger` to return lightweight merge evidence

**Files:**
- Modify: `app/services/cluster_merger.py`
- Test: `tests/test_cluster_merger.py`

**Step 1: Write the failing merger tests**

Add tests ensuring a fused cluster exposes lightweight merge evidence only when a merge actually occurred.

```python
def test_merged_cluster_contains_lightweight_merge_evidence() -> None:
    merger = ClusterMerger(mention_overlap_min=1, jaccard_threshold=0.3)
    c1 = _make_cluster(
        source_url="https://a.example.com",
        mention_identifiers=["https://shared.example.com/1"],
        dominant_event_types=["Attacco"],
        event_date_ref_start=20260324,
        event_date_ref_end=20260324,
    )
    c2 = _make_cluster(
        source_url="https://b.example.com",
        mention_identifiers=["https://shared.example.com/1"],
        dominant_event_types=["Attacco"],
        event_date_ref_start=20260325,
        event_date_ref_end=20260325,
    )

    result = merger.merge([c1, c2])

    assert len(result) == 1
    assert result[0]["merge_evidence"] is not None
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cluster_merger.py -k merge_evidence -v`
Expected: FAIL because merged clusters do not expose audit evidence yet.

**Step 3: Write minimal implementation**

In `app/services/cluster_merger.py`:

- track lightweight evidence during successful unions
- store only compact fields such as `mention_overlap`, `jaccard`, `date_gap_days`, `shared_action_type`
- cap retained evidence list to a small bounded length, e.g. 5
- keep single-cluster unchanged cases free of synthetic merge evidence unless already present

Do not serialize full pre-merge component state.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cluster_merger.py -k merge_evidence -v`
Expected: PASS

**Step 5: Commit**

```bash
git add app/services/cluster_merger.py tests/test_cluster_merger.py
git commit -m "feat: record lightweight cluster merge evidence"
```

### Task 5: Add reconciliation helpers to `ClusterService`

**Files:**
- Modify: `app/services/cluster_service.py`
- Test: `tests/test_cluster_service.py`

**Step 1: Write the failing service-unit tests**

Add pure helper-level tests for:

- selecting the canonical historical component by oldest `first_seen_at`
- matching a current component to one historical component by event overlap
- identifying multi-match merge candidates
- identifying split candidates when one historical component overlaps 2+ current components

Example test shape:

```python
def test_choose_canonical_component_prefers_oldest_first_seen() -> None:
    service = object.__new__(ClusterService)
    candidates = [
        {"component_id": "newer", "first_seen_at": datetime(2026, 3, 24, tzinfo=UTC)},
        {"component_id": "older", "first_seen_at": datetime(2026, 3, 20, tzinfo=UTC)},
    ]

    canonical = service._choose_canonical_component(candidates)

    assert canonical["component_id"] == "older"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cluster_service.py -k "canonical_component or split_candidate or event_overlap" -v`
Expected: FAIL because the helpers do not exist.

**Step 3: Write minimal implementation**

In `app/services/cluster_service.py`, add private helpers for:

- converting current cluster rows into reconciliation payloads
- computing event overlap between current and persisted components
- selecting a canonical historical component
- deciding whether a historical component is split
- deciding whether a new persistent component must be created

Keep these helpers side-effect free where possible so they are easy to test.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cluster_service.py -k "canonical_component or split_candidate or event_overlap" -v`
Expected: PASS

**Step 5: Commit**

```bash
git add app/services/cluster_service.py tests/test_cluster_service.py
git commit -m "refactor: add cluster reconciliation helpers"
```

### Task 6: Reconcile current-run components against persistent components

**Files:**
- Modify: `app/services/cluster_service.py`
- Modify: `app/db/repositories/cluster_component_repository.py`
- Test: `tests/test_cluster_service.py`

**Step 1: Write the failing integration-style service tests**

Add tests covering these run-to-run behaviors:

- first observation creates a persistent component
- second run with additional event IDs keeps the same `component_id`
- historical multi-match merge marks losing components as `merged_into`
- anchor remains the original URL after score drift or membership growth

Example test shape:

```python
async def test_build_and_materialise_preserves_component_id_across_growth(db_session) -> None:
    service = ClusterService(db_session)

    # first run setup
    ...
    await service.build_and_materialise(20260324000000, 20260324010000)
    await db_session.commit()

    first_component_id = ...

    # second run adds connected events
    ...
    await service.build_and_materialise(20260324000000, 20260324120000)
    await db_session.commit()

    second_component_id = ...
    assert second_component_id == first_component_id
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cluster_service.py -k "preserves_component_id_across_growth or merged_into or anchor" -v`
Expected: FAIL because `ClusterService` does not yet reconcile with persistent components.

**Step 3: Write minimal implementation**

Modify `ClusterService.build_and_materialise()` so that after within-run merge and before final materialized-reference updates it:

- loads reconcilable persistent components
- matches current clusters by event overlap
- creates new persistent components when no match exists
- chooses oldest historical component as canonical on multi-match merge
- marks non-canonical matches as `merged_into`
- keeps the original anchor on matched persistent components
- updates active event membership and soft materialization reference fields

Important: do not replace `cluster_id` semantics for `story_clusters` / `root_clusters`; `component_id` is the new continuity key.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cluster_service.py -k "preserves_component_id_across_growth or merged_into or anchor" -v`
Expected: PASS

**Step 5: Commit**

```bash
git add app/services/cluster_service.py app/db/repositories/cluster_component_repository.py tests/test_cluster_service.py
git commit -m "feat: reconcile clusters against persistent components"
```

### Task 7: Detect splits and stale transitions explicitly

**Files:**
- Modify: `app/services/cluster_service.py`
- Modify: `app/db/repositories/cluster_component_repository.py`
- Test: `tests/test_cluster_service.py`

**Step 1: Write the failing tests**

Add tests for:

- a historical component becoming `split` when its prior event membership maps to two current components
- a component becoming `stale` after `N` missed runs

Example test shape:

```python
async def test_build_and_materialise_marks_component_split_when_history_branches(db_session) -> None:
    ...
    assert historical.status == "split"

async def test_build_and_materialise_marks_component_stale_after_missed_runs(db_session) -> None:
    ...
    assert component.status == "stale"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cluster_service.py -k "marks_component_split or marks_component_stale" -v`
Expected: FAIL because split/stale transitions are not implemented.

**Step 3: Write minimal implementation**

In `ClusterService`:

- compare historical active event membership with current-run components
- mark historical components `split` when configured overlap thresholds are met across 2+ current branches
- increment `missing_run_count` for unmatched historical components
- transition to `stale` after configured missed-run threshold

Add any needed configuration values in `app/core/config.py` with conservative defaults, for example:

- `cluster_component_stale_after_runs`
- `cluster_component_split_overlap_min`
- `cluster_component_split_overlap_ratio`

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cluster_service.py -k "marks_component_split or marks_component_stale" -v`
Expected: PASS

**Step 5: Commit**

```bash
git add app/services/cluster_service.py app/db/repositories/cluster_component_repository.py app/core/config.py tests/test_cluster_service.py
git commit -m "feat: detect split and stale cluster components"
```

### Task 8: Materialize soft references and validate story/root consistency

**Files:**
- Modify: `app/services/cluster_service.py`
- Modify: `app/db/repositories/cluster_repository.py`
- Modify: `app/db/repositories/root_cluster_repository.py`
- Test: `tests/test_cluster_service.py`

**Step 1: Write the failing tests**

Add tests for:

- `current_cluster_id/current_table` soft links updated after story materialization
- same fields updated after root materialization
- failure when a `cluster_id` exists in both `story_clusters` and `root_clusters`

Example test shape:

```python
async def test_build_and_materialise_updates_component_soft_reference(db_session) -> None:
    ...
    assert component.current_cluster_id == cluster.cluster_id
    assert component.current_table == "story_clusters"
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cluster_service.py -k "soft_reference or consistency" -v`
Expected: FAIL because soft references and post-run validation are missing.

**Step 3: Write minimal implementation**

In `ClusterService`:

- after upsert/reconciliation, update persistent components with `current_cluster_id`, `current_table`, `current_computed_at`
- add a post-run consistency validation step that:
  - checks no `cluster_id` exists in both materialized tables
  - checks every active component soft-link points to an existing row
- raise `ClusterBuildError` when consistency checks fail

If needed, add small repository helpers like:

- `list_cluster_ids()`
- `exists_by_cluster_id(cluster_id: str)`

Keep them tight and testable.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cluster_service.py -k "soft_reference or consistency" -v`
Expected: PASS

**Step 5: Commit**

```bash
git add app/services/cluster_service.py app/db/repositories/cluster_repository.py app/db/repositories/root_cluster_repository.py tests/test_cluster_service.py
git commit -m "feat: validate materialized cluster consistency"
```

### Task 9: Anchor scheduler window to latest ingested data and commit the job

**Files:**
- Modify: `app/scheduler/cluster_job.py`
- Modify: `app/services/cluster_service.py` (only if a helper for latest ingested timestamp is needed)
- Test: `tests/test_cluster_job.py`
- Possibly Test: `tests/test_cluster_service.py`

**Step 1: Write the failing scheduler tests**

Add tests verifying:

- the job queries/uses the latest ingested `date_added` instead of `datetime.now() - timedelta(hours=36)`
- the session commits after successful materialization

Example test shape:

```python
async def test_run_cluster_job_commits_after_success() -> None:
    ...
    session.commit.assert_awaited_once()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cluster_job.py -v`
Expected: FAIL because the job currently uses wall clock and does not commit.

**Step 3: Write minimal implementation**

In `app/scheduler/cluster_job.py`:

- query the latest ingested `date_added` from `gdelt_events`
- derive the overlap window from that value
- call `ClusterService.build_and_materialise()` with that window
- `await session.commit()` after successful completion
- preserve rollback-by-context-manager behavior on exception if needed by explicit `try/except`

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cluster_job.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add app/scheduler/cluster_job.py tests/test_cluster_job.py
git commit -m "fix: anchor cluster job to ingested data"
```

### Task 10: Make no-GKG behavior explicit and observable

**Files:**
- Modify: `app/services/cluster_service.py`
- Modify: `app/services/cluster_merger.py`
- Test: `tests/test_cluster_service.py`
- Test: `tests/test_cluster_merger.py`

**Step 1: Write the failing tests**

Add tests for:

- components with no source-local GKG still materialize
- such components set `has_gkg=False` in persistent state
- semantic Jaccard merge does not trigger for empty-theme components, while mention-overlap merge still can

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cluster_service.py -k no_gkg -v && pytest tests/test_cluster_merger.py -k empty -v`
Expected: FAIL because no-GKG state is not explicitly persisted or asserted.

**Step 3: Write minimal implementation**

Update service/reconciliation flow to:

- derive `has_gkg` from component-local GKG rows
- persist it into `cluster_components`
- log an explicit structured event for no-GKG active components

Do not add a fallback to foreign GKG documents outside the component boundary.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cluster_service.py -k no_gkg -v && pytest tests/test_cluster_merger.py -k empty -v`
Expected: PASS

**Step 5: Commit**

```bash
git add app/services/cluster_service.py app/services/cluster_merger.py tests/test_cluster_service.py tests/test_cluster_merger.py
git commit -m "feat: persist no-gkg cluster state"
```

### Task 11: Update documentation to reflect the new source of truth

**Files:**
- Modify: `docs/paper.md`
- Modify: `docs/clustering-pipeline.md`
- Possibly Modify: `AGENTS.md` (only if repository workflow guidance must change)

**Step 1: Write the failing doc checklist**

Create a short checklist in the task branch notes or commit message draft and verify the docs still describe outdated behavior such as:

- cluster identity derived only from current event sets
- merge continuity implicit in reruns
- wall-clock 36-hour scheduling as the only policy

**Step 2: Run doc grep to verify drift exists**

Run: `python -m pytest -q` is not needed here. Instead inspect the docs and confirm outdated statements are present.
Expected: outdated statements found.

**Step 3: Write minimal documentation updates**

Update docs so they explicitly state:

- `cluster_components` is the cross-run source of truth
- `component_id` is immutable and assigned at first observation
- `story_clusters` and `root_clusters` are materialized projections
- scheduler window is anchored to latest ingested data
- no-GKG behavior and post-run consistency checks are intentional

Keep the docs aligned with the implemented code, not with superseded design text.

**Step 4: Verify docs are internally consistent**

Run: `ruff check .`
Expected: PASS (including doc-adjacent Python files if touched)

**Step 5: Commit**

```bash
git add docs/paper.md docs/clustering-pipeline.md
git commit -m "docs: describe persistent cluster component flow"
```

### Task 12: Run verification suite and stabilize

**Files:**
- Modify: any touched files only if verification reveals issues

**Step 1: Run focused tests for new behavior**

Run:

```bash
pytest tests/test_cluster_component_model.py -v
pytest tests/test_cluster_component_repository.py -v
pytest tests/test_cluster_merger.py -v
pytest tests/test_cluster_service.py -v
pytest tests/test_cluster_job.py -v
```

Expected: PASS

**Step 2: Run repository and API regression tests**

Run:

```bash
pytest tests/test_cluster_repository.py tests/test_root_cluster_repository.py tests/test_api_clusters.py tests/test_api_root_clusters.py -v
```

Expected: PASS

**Step 3: Run linter**

Run: `ruff check .`
Expected: PASS

**Step 4: Run broader suite when practical**

Run: `pytest -v`
Expected: PASS

**Step 5: Commit final stabilization changes**

```bash
git add .
git commit -m "test: verify persistent cluster component pipeline"
```
