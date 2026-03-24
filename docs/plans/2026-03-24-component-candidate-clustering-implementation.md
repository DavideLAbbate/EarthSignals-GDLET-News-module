# Component Candidate Clustering Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the URL-based candidate phase in clustering with connected-component candidates built from the `event_id <-> mention_identifier` graph, using explicit multi-gate admission instead of a single scalar threshold.

**Architecture:** Add a component-discovery stage inside `ClusterService` that collects mentions and events for a `date_added` window, builds connected components, computes component metrics, applies explicit gates, and then materialises one cluster per admitted component. Keep the current cluster schema and merge path temporarily, but change cluster identity and candidate construction so the unit of work becomes the component rather than the source URL.

**Tech Stack:** Python 3.11+, FastAPI backend, async SQLAlchemy 2.0, PostgreSQL/SQLite, pytest, Ruff.

---

## Task 1: Introduce failing tests for component discovery

**Files:**
- Modify: `tests/test_cluster_service.py`
- Read: `app/services/cluster_service.py`
- Read: `app/db/models.py`

**Step 1: Write the failing test**

Add a test that creates:
- two events
- three mention rows
- one shared `mention_identifier` between the two events

Assert that a new helper such as `_build_candidate_components(...)` returns one connected component containing both event IDs and the shared mention URL.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cluster_service.py::test_build_candidate_components_groups_connected_event_and_mention_nodes -v`

Expected: FAIL because the helper does not exist yet.

**Step 3: Write minimal implementation**

Add internal helpers in `app/services/cluster_service.py` to:
- collect windowed events and mentions
- build adjacency from `event_id <-> mention_identifier`
- derive connected components

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cluster_service.py::test_build_candidate_components_groups_connected_event_and_mention_nodes -v`

Expected: PASS.

---

## Task 2: Add failing tests for component metrics

**Files:**
- Modify: `tests/test_cluster_service.py`
- Read: `app/services/cluster_service.py`

**Step 1: Write the failing test**

Add a test for a synthetic component asserting that metric computation returns:
- distinct `event_id_count`
- distinct `source_url_count`
- distinct `domain_count`
- non-zero `component_density`
- correct `event_time_span_hours`

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cluster_service.py::test_component_metrics_capture_size_density_and_time_span -v`

Expected: FAIL because metric helper does not exist yet.

**Step 3: Write minimal implementation**

Implement a helper in `app/services/cluster_service.py` that takes a component plus related event rows and returns a metric dict.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cluster_service.py::test_component_metrics_capture_size_density_and_time_span -v`

Expected: PASS.

---

## Task 3: Add failing tests for explicit candidate gates

**Files:**
- Modify: `tests/test_cluster_service.py`
- Modify: `app/core/config.py`
- Read: `app/services/cluster_service.py`

**Step 1: Write the failing tests**

Add tests covering:
- component rejected for too few events
- component rejected for too few source URLs
- component rejected for too few domains
- component rejected for too wide time span
- component accepted when all gates pass

Assert that the gate evaluator returns a boolean plus explicit failed gate names.

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_cluster_service.py -k "component_gate" -v`

Expected: FAIL because gate settings and evaluator do not exist yet.

**Step 3: Write minimal implementation**

In `app/core/config.py`, add settings such as:
- `cluster_candidate_min_event_ids`
- `cluster_candidate_min_source_urls`
- `cluster_candidate_min_domains`
- `cluster_candidate_max_event_span_hours`
- `cluster_candidate_min_density`

In `app/services/cluster_service.py`, implement a gate evaluator that returns both pass/fail and failed gate names.

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_cluster_service.py -k "component_gate" -v`

Expected: PASS.

---

## Task 4: Add failing tests for candidate rejection logging

**Files:**
- Modify: `tests/test_cluster_service.py`
- Read: `app/services/cluster_service.py`

**Step 1: Write the failing test**

Add a test that forces one component to fail a gate and asserts a structured log event is emitted with:
- component identifier
- component metrics
- failed gate names

Use the existing logging approach in the service and assert against captured logs or a mocked logger.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cluster_service.py::test_component_rejection_logs_failed_gates_and_metrics -v`

Expected: FAIL because rejection logging is not implemented yet.

**Step 3: Write minimal implementation**

Add structured rejection logging to the candidate admission phase in `app/services/cluster_service.py`.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cluster_service.py::test_component_rejection_logs_failed_gates_and_metrics -v`

Expected: PASS.

---

## Task 5: Add failing tests for component-based topic score inputs

**Files:**
- Modify: `tests/test_cluster_service.py`
- Modify: `tests/test_cluster_merger.py`
- Read: `app/integrations/event_enrichment_mapper.py`

**Step 1: Write the failing tests**

Add tests that assert the component-level topic score is computed from:
- `event_id_count`
- `source_url_count`
- `domain_count`

Do not use the old URL-local aggregate semantics in the new candidate phase.

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_cluster_service.py -k "component_topic_score" -v`

Expected: FAIL because scoring still assumes URL-level aggregate inputs.

**Step 3: Write minimal implementation**

Update `app/integrations/event_enrichment_mapper.py` to add a dedicated component-topic-score helper rather than overloading the current URL-centric helper in ambiguous ways.

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_cluster_service.py -k "component_topic_score" -v`

Expected: PASS.

---

## Task 6: Add failing tests for component-based cluster build

**Files:**
- Modify: `tests/test_cluster_service.py`
- Read: `app/services/cluster_service.py`
- Read: `app/db/models.py`

**Step 1: Write the failing test**

Add an integration-style service test where:
- two different `source_url` values
- multiple events
- shared mention connectivity

produce one materialised cluster because they belong to the same admitted component.

Assert the stored row aggregates both URLs' event IDs and component metadata.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cluster_service.py::test_build_and_materialise_uses_component_candidates_instead_of_single_source_url -v`

Expected: FAIL because current build path still creates one candidate per `source_url`.

**Step 3: Write minimal implementation**

Refactor `build_and_materialise()` in `app/services/cluster_service.py` to:
- discover components first
- compute metrics and gates
- build cluster rows from admitted components

Reuse existing aggregation helpers where possible, but switch the unit of work from URL to component.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cluster_service.py::test_build_and_materialise_uses_component_candidates_instead_of_single_source_url -v`

Expected: PASS.

---

## Task 7: Stabilise cluster identity for component candidates

**Files:**
- Modify: `tests/test_cluster_service.py`
- Modify: `tests/test_story_cluster_model.py`
- Modify: `app/services/cluster_service.py`
- Possibly modify: `app/db/models.py`

**Step 1: Write the failing tests**

Add tests asserting that component-based `cluster_id` is deterministic from stable component content, not from the currently strongest `source_url`.

Example target: hash of sorted event IDs, or sorted event IDs plus sorted source URLs.

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_cluster_service.py -k "component_cluster_id" -v`

Expected: FAIL because `cluster_id` is still derived from one `source_url`.

**Step 3: Write minimal implementation**

Change component cluster identity generation in `app/services/cluster_service.py` and update any schema/test assumptions that still say the cluster is identified by source URL.

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_cluster_service.py -k "component_cluster_id" -v`

Expected: PASS.

---

## Task 8: Re-evaluate merge semantics after component candidates

**Files:**
- Modify: `tests/test_cluster_merger.py`
- Modify: `tests/test_cluster_service.py`
- Modify: `app/services/cluster_service.py`
- Possibly modify: `app/services/cluster_merger.py`

**Step 1: Write the failing test**

Add a regression test that captures the desired behavior after introducing component candidates:
- either the merge stage is skipped because the component already is the story unit
- or the merge stage remains, but only for a narrower residual case

The test must make the intended post-redesign role of `ClusterMerger` explicit.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_cluster_service.py -k "post_component_merge" -v`

Expected: FAIL until the new role is enforced.

**Step 3: Write minimal implementation**

Choose one of:
- bypass `ClusterMerger` in the new pipeline
- keep it only for a constrained secondary fusion step

Do not leave the old merge path untouched without explicitly deciding its purpose.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_cluster_service.py -k "post_component_merge" -v`

Expected: PASS.

---

## Task 9: Update docs and repository guidance

**Files:**
- Modify: `docs/clustering-pipeline.md`
- Modify: `docs/paper.md`
- Modify: `AGENTS.md` only if cluster workflow guidance needs updating

**Step 1: Write the failing documentation checklist**

List the stale statements that must change:
- candidate unit is `source_url`
- single threshold-based admission
- old `cluster_id` semantics
- old interpretation of topic score

**Step 2: Update docs**

Revise the docs so they describe:
- `event_id <-> mention_identifier` connected components
- gate-based candidate admission
- component-level metrics
- new cluster identity semantics

**Step 3: Verify docs are consistent**

Run a manual grep for outdated phrases and update any remaining stale references.

Run: `pytest tests/test_cluster_service.py tests/test_cluster_merger.py -v`

Expected: tests still pass after doc updates.

---

## Task 10: Lint and focused verification

**Files:**
- Modify only files already touched above

**Step 1: Run Ruff**

Run: `ruff check .`

Expected: no lint errors.

**Step 2: Run focused clustering tests**

Run: `pytest tests/test_cluster_service.py tests/test_cluster_merger.py tests/test_cluster_repository.py tests/test_root_cluster_repository.py -v`

Expected: all targeted clustering tests pass.

**Step 3: If stable, run broader regression**

Run: `pytest -k "cluster or root_cluster" -v`

Expected: all cluster-related tests pass.
