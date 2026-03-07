# AGENTS.md — Coding Agent Reference

This file documents build/test commands, code style conventions, and architectural patterns
for the `gdelt-news-backend` project (Python 3.11+, FastAPI, async SQLAlchemy, BigQuery, Anthropic).

---

## Project Overview

A production FastAPI backend that queries GDELT 2.0 via Google BigQuery, uses Claude (Anthropic)
to interpret natural-language news filters, caches results in PostgreSQL, and runs a 15-minute
background sync job via APScheduler. Authentication is X-API-Key header-based.

Key layers: `app/api/` → `app/services/` → `app/integrations/` + `app/db/`.

---

## Build & Run Commands

```bash
# Install all dependencies (including dev extras)
pip install -e ".[dev]"

# Run the development server
uvicorn app.main:create_app --factory --reload --port 8000

# Run via Docker Compose (PostgreSQL + app)
docker-compose up --build

# Apply database migrations
alembic upgrade head

# Generate a new migration
alembic revision --autogenerate -m "description"
```

---

## Test Commands

```bash
# Run all tests
pytest

# Run all tests with verbose output
pytest -v

# Run a single test file
pytest tests/test_api_events.py

# Run a single test function  ← most common during development
pytest tests/test_api_events.py::test_search_events_success

# Run tests matching a keyword pattern
pytest -k "test_filter"

# Run tests with stdout shown (useful for debugging)
pytest -s -v

# Run with short traceback
pytest --tb=short
```

**Test setup requirements:** Before tests run, `conftest.py` sets environment variables
(`API_KEY`, `DATABASE_URL`, `ANTHROPIC_API_KEY`, `GCP_PROJECT_ID`, etc.) *before* importing
any `app.*` modules. Never import app modules at module level in test files — always inside
fixtures or test functions, or after env vars are guaranteed to be set.

---

## Linting & Formatting

```bash
# Lint with Ruff (configured in pyproject.toml)
ruff check .

# Auto-fix lint issues
ruff check --fix .

# Format with Ruff
ruff format .

# Check formatting without writing
ruff format --check .
```

Ruff is the sole linter/formatter. No Black, Flake8, or isort. Line length: **100 characters**.
Target: `py311`. Ruff must be installed separately (`pip install ruff`) — it is not in dev deps.

---

## Code Style Guidelines

### File Structure

Every Python source file must start with:
1. A module-level docstring explaining purpose, dependencies, and design decisions
2. `from __future__ import annotations` (always first import)
3. Standard import groups (see below)

```python
"""
Short one-liner description.

Longer explanation of responsibilities, external dependencies,
and any non-obvious design decisions.
"""
from __future__ import annotations

import asyncio                          # stdlib
from functools import lru_cache

import structlog                        # third-party
from fastapi import Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings  # internal (absolute, never relative)
from app.core.exceptions import QueryValidationError
```

### Import Conventions

- **Order:** `from __future__` → stdlib → third-party → `app.*` internal
- **Always absolute imports** — never use relative imports (no `from .module import ...`)
- **No wildcard imports** (`from module import *`)

### Type Annotations

- Full type annotations on all function signatures and class attributes
- Use `str | None` (union syntax) over `Optional[str]` — Python 3.10+ style
- Use `list[str]`, `dict[str, Any]` (lowercase generics) over `List[str]`, `Dict[str, Any]`
- Use `tuple[str, list]` for return types
- `Any` from `typing` is acceptable when the type is genuinely unknown

### Naming Conventions

| Kind | Convention | Example |
|---|---|---|
| Modules | `snake_case` | `gdelt_query_builder.py` |
| Classes | `PascalCase` | `BigQueryClientWrapper`, `SyncState` |
| Functions / methods | `snake_case` | `build_events_query`, `get_async_session` |
| Constants | `UPPER_SNAKE_CASE` | `CAMEO_COUNTRY_CODES`, `MAX_RETRIES` |
| Variables / args | `snake_case` | `fips_country_code`, `date_from_sqldate` |
| Private helpers | leading `_` | `_api_key_scheme`, `_get_session_factory` |
| Unused return values (FastAPI deps) | `_` | `_: str = Depends(verify_api_key)` |

### Pydantic Models (v2 API)

- Inherit `BaseModel`; use `Field(...)` with `description=` for required fields
- Use `model_validate(data)` — never `.parse_obj()`
- Use `.model_dump()` — never `.dict()`
- Use `@model_validator(mode="after")` for cross-field validation
- Use `@field_validator("field", mode="before")` for preprocessing
- Settings models use `SettingsConfigDict(env_file=".env", ...)`

### Function Signatures

Use keyword-only arguments (`*`) for factory and builder functions with multiple parameters
to avoid positional ordering mistakes:

```python
def build_events_query(
    *,
    date_from_sqldate: int,
    date_to_sqldate: int,
    fips_country_code: str | None = None,
    cameo_event_code: str | None = None,
) -> tuple[str, list]:
```

### Logging

Use `structlog` throughout. Never use `print()` or `logging.getLogger()`.

```python
logger = structlog.get_logger(__name__)   # module-level, not class-level

# Always use key=value pairs; event name in snake_case
logger.info("gdelt_query_start", date_from=date_from, country=fips_country_code)
logger.warning("filter_cache_corrupted", cache_key=cache_key[:8])
logger.error("bigquery_query_failed", error=str(exc))
```

### Error Handling

**Domain exceptions** all inherit `GDELTBackendError` (in `app/core/exceptions.py`).
Raise domain exceptions from service/integration layers; never raise `HTTPException` there.
`app/api/error_handlers.py` maps domain exceptions to HTTP status codes globally.

```python
# Guard clause + raise domain exception (preferred over nested if/else)
if not raw_filters.has_any_filter():
    raise FilterInterpretationError(
        "At least one filter field must be provided",
        detail="Supply country, event_type, or date_range",
    )

# Non-fatal errors: log + continue, do not re-raise
try:
    await upsert_cached_filter(session, cache_key, result)
except Exception as exc:
    logger.warning("filter_cache_write_failed", error=str(exc))
    # intentionally not re-raised; cache write failure is non-fatal
```

### Singleton / Dependency Injection Pattern

Use `@lru_cache(maxsize=1)` for singleton factories. Wire everything through FastAPI `Depends`.

```python
@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
```

### Async Patterns

- All I/O must be `async` — use `await` for database, Anthropic, and scheduler calls
- BigQuery SDK is synchronous — wrap calls in `asyncio.get_event_loop().run_in_executor(None, ...)`
  or a `ThreadPoolExecutor` (see `app/integrations/bigquery_client.py`)
- Database sessions via `async with factory() as session:` context managers
- `asyncio_mode = "auto"` in pytest config — all `async def test_*` are collected automatically

### Visual Code Organization

Use section dividers for logical grouping within longer files:

```python
# ── Public interface ──────────────────────────────────────────────────────
# ── Private helpers ───────────────────────────────────────────────────────
# ── Database models ───────────────────────────────────────────────────────
```

---

## Testing Conventions

- **Framework:** `pytest` + `pytest-asyncio` (`asyncio_mode = "auto"`)
- **Test DB:** SQLite in-memory (`sqlite+aiosqlite:///:memory:`) — never PostgreSQL in tests
- **HTTP client:** `httpx.AsyncClient` for async route tests; `fastapi.testclient.TestClient`
  for synchronous error-handler tests
- **External services:** Always mocked — BigQuery and Anthropic clients are replaced by
  `MagicMock` / `AsyncMock` via `conftest.py` fixtures and FastAPI dependency overrides

### Key Fixtures (from `tests/conftest.py`)

| Fixture | Scope | Purpose |
|---|---|---|
| `db_engine` | function | In-memory SQLite async engine, creates/drops tables |
| `db_session` | function | `AsyncSession` that rolls back after each test |
| `mock_bq_client` | function | `MagicMock` with `run_query = AsyncMock(return_value=[])` |
| `mock_anthropic_client` | function | `AsyncMock()` for Anthropic client |
| `app` | function | Full `FastAPI` app with all dependency overrides applied |
| `async_client` | function | `httpx.AsyncClient(app=app, base_url="http://test")` |
| `api_headers` | function | `{"X-API-Key": "test-api-key"}` |

### Anthropic Mock Pattern (used across test files)

```python
def _make_anthropic_mock(response_text: str):
    mock_message = MagicMock()
    mock_message.content = [MagicMock(text=response_text)]
    client = AsyncMock()
    client.messages.create = AsyncMock(return_value=mock_message)
    return client
```

### Exception Assertion

```python
with pytest.raises(FilterInterpretationError, match="At least one filter field"):
    await normalize_filters(raw, db_session, client)
```

---

## Architecture Notes

- `app/core/config.py` — single `Settings` class (pydantic-settings); use `get_settings()` everywhere
- `app/core/exceptions.py` — all domain exceptions; add new ones here, wire handler in `error_handlers.py`
- `app/api/dependencies.py` — FastAPI dependencies for auth, DB session, BQ client, Anthropic client
- `app/db/repositories/` — all raw DB access; services call repositories, routes call services
- `app/integrations/` — third-party SDK wrappers (BQ, Anthropic, GDELT query/result logic)
- `app/scheduler/sync_job.py` — 15-minute background GDELT sync; uses its own DB session factory
- Alembic migrations live in `alembic/versions/`; DB URL is injected at runtime from `settings`
