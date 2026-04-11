# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Python 3.11+ FastAPI backend that ingests GDELT 2.0 HTTP export files into PostgreSQL, normalizes free-text search filters via Anthropic Claude, serves an authenticated REST API, and runs APScheduler background jobs for sync, ingestion, enrichment, and clustering.

## Commands

```bash
# Install
pip install -e ".[dev]"

# Dev server
uvicorn app.main:create_app --factory --reload --port 8000

# Docker (full stack)
docker compose up --build

# Local Postgres only (no container app)
docker compose -f docker-compose.local.yml up

# Migrations
alembic upgrade head
alembic revision --autogenerate -m "description"

# Lint / format
ruff check .
ruff check --fix .
ruff format .
ruff format --check .

# Tests
pytest
pytest -v
pytest --tb=short
pytest tests/test_api_events.py
pytest tests/test_api_events.py::test_search_events_success -v
pytest -k "bootstrap" -v
```

**Before finishing any change:** run `ruff check .`, then the focused test file, then the full suite.

## Architecture

### Layers

```
app/api/          routes, dependencies (auth, DB session, clients), error handlers
app/core/         Settings (pydantic-settings, lru_cache), exceptions, structlog setup
app/db/           SQLAlchemy models, async session factory, repositories
app/integrations/ Anthropic, GDELT HTTP, article fetcher/extractor, enrichment client
app/scheduler/    APScheduler wiring and startup job functions
app/schemas/      Pydantic v2 request/response models
app/services/     Orchestration and business logic (calls repositories)
alembic/versions/ Schema migrations
tests/            pytest suite with mocked external services
```

**Dependency direction:** routes → services → repositories. No SQL in routes or services; no business logic in repositories.

### Key runtime flows

**Search request:**
`POST /events/search` → `filter_service.normalize_filters()` (caches Claude output in `filter_mapping_cache`) → `query_service.search_events()` → `event_repository` → PostgreSQL

**Ingestion:**
GDELT HTTP exports → `ingestion_service.run_bootstrap()` / `run_incremental()` → `gdelt_events`, `gdelt_mentions`, `gdelt_gkg` tables

**Clustering pipeline:**
`cluster_job` → builds `StoryCluster` candidates from the three GDELT layers → `cluster_merger` merges overlapping components → large merged clusters (> `ROOT_CLUSTER_MIN_EVENT_COUNT`) go to `root_clusters` table; the rest go to `story_clusters` → `ClusterComponent` rows track persistent component identity across runs

**Enrichment:**
`event_enrichment_service` / `cluster_enrichment_service` call an external enrichment microservice (`EVENT_ENRICHMENT_SERVICE_BASE_URL`) and write article title, summary, topics, entities back to `gdelt_events` / `story_clusters` / `root_clusters`

### Scheduler jobs (registered in `app/scheduler/scheduler.py`)

| Job ID | Interval | Feature flag |
|---|---|---|
| `gdelt_sync` | `SYNC_INTERVAL_MINUTES` (15 min) | `ENABLE_METADATA_SYNC` |
| `gdelt_incremental_ingestion` | `INGESTION_INTERVAL_MINUTES` (60 min) | always on |
| `gdelt_event_enrichment` | `EVENT_ENRICHMENT_INTERVAL_MINUTES` (30 min) | `ENABLE_EVENT_ENRICHMENT` |
| `gdelt_cluster_materialisation` | `CLUSTER_INTERVAL_MINUTES` (1440 min) | `ENABLE_CLUSTER_MATERIALISATION` |
| `gdelt_cluster_enrichment` | `CLUSTER_ENRICHMENT_INTERVAL_MINUTES` (30 min) | `ENABLE_CLUSTER_ENRICHMENT` |
| `gdelt_retention_cleanup` | 24 h | always on |
| `gdelt_cluster_terminal_cleanup` | 24 h | always on |

The `AsyncIOScheduler` **must** be started inside the FastAPI lifespan to share uvicorn's event loop.

### Database models (`app/db/models.py`)

- `GdeltEvent` — cached GDELT 2.0 events; PK is GDELT's `GLOBALEVENTID`
- `GdeltMention` / `GdeltGkg` — EVENTMENTIONS and GKG layers keyed by source URL
- `StoryCluster` / `RootCluster` — materialised story clusters; `cluster_id` = `{YYYYMMDD}_{sha256(source_url)[:12]}`
- `ClusterComponent` — persistent component identity across materialisation runs; tracks `status`, `merged_into_component_id`, `current_table`/`current_cluster_id`
- `ClusterComponentEvent` — event membership history for components
- `SyncState` — last GDELT sync outcome and metadata snapshots
- `FilterMappingCache` — SHA256-keyed cache of Claude's filter normalization output
- `IngestionState` — per-run ingestion job tracking

### Settings (`app/core/config.py`)

All configuration is read via `get_settings()` (LRU-cached singleton). Tests clear this cache between cases. Key feature flags: `ENABLE_METADATA_SYNC`, `ENABLE_EVENT_ENRICHMENT`, `ENABLE_CLUSTER_MATERIALISATION`, `ENABLE_CLUSTER_ENRICHMENT`. Cluster merge parameters (`CLUSTER_MERGE_JACCARD_THRESHOLD`, `CLUSTER_MAX_MERGE_DAY_GAP`, etc.) tune the merge gates.

## Testing Conventions

- `asyncio_mode = "auto"` — no `@pytest.mark.asyncio` needed.
- In-memory SQLite (`sqlite+aiosqlite:///:memory:`); no real credentials required.
- `tests/conftest.py` seeds env vars **before** importing app modules — preserve that order.
- Shared fixtures to reuse: `async_client`, `api_headers`, `db_session`, `mock_anthropic_client`.
- External integrations (Anthropic, GDELT HTTP) must stay mocked in tests.
- When touching validation, run both the unit test and the relevant API route test.

## Code Conventions

- Every module starts with a docstring, then `from __future__ import annotations`.
- Import order: future → stdlib → third-party → `app.*`. Absolute imports only.
- Line length: 100 characters.
- Annotate every function signature including return type; use `str | None` not `Optional[str]`.
- Logging: `structlog` via `get_logger(__name__)`; first arg is a short event name; details go in kwargs.
- Domain exceptions inherit from `GDELTBackendError` (`app/core/exceptions.py`); map them in `app/api/error_handlers.py`. Services raise domain exceptions; routes raise `HTTPException` only for request-layer failures.
- Preserve section-divider comments like `# ── Section ─────`.
