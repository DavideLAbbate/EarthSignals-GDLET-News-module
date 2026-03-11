# Bootstrap Range Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a range-based bootstrap CLI that ingests all three GDELT CSV datasets into the local tables over an explicit time window.

**Architecture:** Extract the existing bootstrap ingestion loop into a shared range-based service function and keep `run_bootstrap()` as a retention-window wrapper. Add a dedicated CLI that normalizes `YYYYMMDD` and `YYYYMMDDHHMMSS` inputs, validates the range, and invokes the shared ingestion path. Reuse the existing mentions/GKG repository chunking and add explicit chunking for events so large CSV downloads remain safe.

**Tech Stack:** Python 3.11+, FastAPI backend services, SQLAlchemy 2 async, asyncpg, aiosqlite, httpx, structlog, pytest, Ruff.

---

## Architectural Decisions

| Decision | Rationale |
|---|---|
| New CLI `run_bootstrap_range.py` | Keeps manual backfill separate from startup bootstrap and scheduler behavior |
| Shared service core `run_bootstrap_range(...)` | Avoids duplicating ingestion logic between CLI and default bootstrap |
| Accept both 8-digit and 14-digit inputs | Matches existing operator workflow and GDELT timestamp format |
| Chronological processing by export timestamp | Preserves deterministic replay order and current ingestion semantics |
| Event insert chunking added explicitly | Prevents oversized statements when a single fetched export file is large |
| Mentions/GKG stay best-effort | Matches current non-fatal secondary dataset handling |
| Existing `run_bootstrap()` becomes wrapper | Preserves startup bootstrap compatibility |

---

## Phase 1 — Range Parsing + CLI

### Task 1.1 — Add failing tests for CLI timestamp normalization

**Files:**
- Create/Modify: `tests/test_bootstrap_range_cli.py`
- Test: `tests/test_bootstrap_range_cli.py`

**Step 1:** Write tests for:
- accepting `YYYYMMDD` start/end
- accepting `YYYYMMDDHHMMSS` start/end
- mixed 8/14 digit inputs
- rejecting malformed timestamps
- rejecting `start > end`

**Step 2:** Run:

```bash
pytest tests/test_bootstrap_range_cli.py -v
```

Expected: failing tests because the CLI and parsing helpers do not exist yet.

### Task 1.2 — Implement CLI parsing and validation

**Files:**
- Create: `run_bootstrap_range.py`
- Modify: `app/services/ingestion_service.py`

**Step 1:** Add a small helper in `app/services/ingestion_service.py` to normalize
an input timestamp string/int into inclusive `DATEADDED` integers.

**Step 2:** Create `run_bootstrap_range.py` that:
- loads `.env`
- normalizes async DB URL like the other scripts
- parses `<start> <end>`
- validates the range
- opens a session
- calls the new shared service function
- commits and prints inserted counts

**Step 3:** Run:

```bash
pytest tests/test_bootstrap_range_cli.py -v
```

Expected: CLI tests pass.

---

## Phase 2 — Shared Range Bootstrap Service

### Task 2.1 — Add failing service test for explicit range ingestion

**Files:**
- Modify: `tests/test_ingestion_service.py`

**Step 1:** Add a test that calls the new range-based bootstrap service with
explicit `start` and `end` values and verifies:
- only files in range are processed
- all three tables receive rows
- watermark equals normalized end timestamp

**Step 2:** Run the targeted test:

```bash
pytest tests/test_ingestion_service.py::test_run_bootstrap_range_ingests_three_datasets -v
```

Expected: FAIL because the new service function does not exist yet.

### Task 2.2 — Extract shared service implementation

**Files:**
- Modify: `app/services/ingestion_service.py`

**Step 1:** Add `run_bootstrap_range(session, since_ts, until_ts)`.

**Step 2:** Move the current bootstrap loop into that function while keeping:
- ingestion run creation/update
- chronological export processing
- best-effort mentions/GKG sub-batches

**Step 3:** Refactor `run_bootstrap(session)` to compute the retention window and
delegate to `run_bootstrap_range(session, since_ts, until_ts)`.

**Step 4:** Re-run:

```bash
pytest tests/test_ingestion_service.py::test_run_bootstrap_range_ingests_three_datasets -v
```

Expected: PASS.

---

## Phase 3 — Chunked Inserts

### Task 3.1 — Add failing regression test for chunked event insertion

**Files:**
- Modify: `tests/test_ingestion_service.py`
- Read: `app/db/repositories/event_repository.py`

**Step 1:** Add a test that feeds a large enough event batch to the range
bootstrap path and verifies the service delegates through chunk-safe bulk event
insert logic rather than building one oversized statement.

**Step 2:** Run:

```bash
pytest tests/test_ingestion_service.py::test_run_bootstrap_range_chunks_event_inserts -v
```

Expected: FAIL if current event insert path does not satisfy the new assertion.

### Task 3.2 — Implement event chunking in the repository/service path

**Files:**
- Modify: `app/db/repositories/event_repository.py`
- Possibly modify: `app/services/ingestion_service.py`

**Step 1:** Inspect the current event bulk insert implementation.

**Step 2:** Add or reuse dialect-aware chunk sizing similar to mentions/GKG.

**Step 3:** Ensure the range bootstrap path routes event rows through this
chunk-safe insert behavior.

**Step 4:** Re-run the targeted chunking test.

Expected: PASS.

---

## Phase 4 — Integration Verification

### Task 4.1 — Expand service coverage for mixed timestamp formats

**Files:**
- Modify: `tests/test_ingestion_service.py`

**Step 1:** Add tests covering mixed 8-digit and 14-digit input combinations.

**Step 2:** Run the focused ingestion service tests:

```bash
pytest tests/test_ingestion_service.py -k "bootstrap" -v
```

Expected: all bootstrap-related tests pass.

### Task 4.2 — Lint and regression verification

**Files:**
- Modify only files already touched above

**Step 1:** Run:

```bash
ruff check .
```

Expected: no lint errors.

**Step 2:** Run:

```bash
pytest tests/test_bootstrap_range_cli.py -v
pytest tests/test_ingestion_service.py -k "bootstrap" -v
```

Expected: all targeted tests pass.

**Step 3:** If stable, run the full suite:

```bash
pytest -v
```

Expected: full suite passes.
