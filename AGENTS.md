# AGENTS.md — Repository Guidance For Coding Agents

Repository-specific guidance for agentic coding tools working in
`web-journal-news-module` (`gdelt-news-backend`).

This is a Python 3.11+ FastAPI backend that:
- ingests GDELT 2.0 events from public HTTP export files (no BigQuery at runtime)
- uses Anthropic (Claude) to normalize free-text search filters
- caches normalized filters in PostgreSQL
- exposes authenticated REST endpoints for searching locally cached events
- runs background APScheduler jobs for ingestion, metadata refresh, enrichment, and retention

## Rule Files
- No `.cursorrules` file found.
- No `.cursor/rules/` directory found.
- No `.github/copilot-instructions.md` file found.
- Follow this file and nearby code patterns first.

## Project Layout

```
app/api/          — routes, dependencies, auth checks, error handlers
app/services/     — orchestration and business logic
app/integrations/ — external clients (BigQuery, Anthropic, GDELT HTTP), query builders, mappers
app/db/           — ORM models, async session, repositories
app/core/         — settings, structlog config, typed domain exceptions
app/scheduler/    — APScheduler wiring, sync job, ingestion jobs
app/schemas/      — Pydantic v2 request/response models
tests/            — pytest suite; all external services are mocked
alembic/versions/ — database migrations
```

Key runtime data flow:
```
GDELT export ZIPs (HTTP) → ingestion_service → gdelt_events (Postgres)
gdelt_events → sync_job → sync_state (metadata snapshot)
POST /events/search → filter_service (Claude) → postgres_compiler → gdelt_events
```

## Install, Build, And Run

```bash
pip install -e ".[dev]"
uvicorn app.main:create_app --factory --reload --port 8000
docker-compose up --build
alembic upgrade head
alembic revision --autogenerate -m "description"
```

- `pyproject.toml` uses `hatchling` as the build backend.
- There is no frontend build pipeline.
- For verification prefer lint + targeted tests over packaging.

## Lint And Format

```bash
ruff check .          # check for lint errors
ruff check --fix .    # auto-fix lint errors
ruff format .         # format all files
ruff format --check . # check formatting without writing
```

- Ruff is the only configured formatter/linter; do not add a second one.
- Config lives in `pyproject.toml`: line length `100`, target `py311`.
- Always run `ruff check .` before considering any change complete.

## Test Commands

```bash
pytest                                                    # full suite
pytest -v                                                 # verbose
pytest tests/test_api_events.py                           # single file
pytest tests/test_api_events.py::test_search_events_success -v  # single test
pytest -k "filter" -v                                     # keyword match
pytest -s -v                                              # show stdout
pytest --tb=short                                         # shorter tracebacks
```

Single-test guidance:
- Prefer `pytest path/to/file.py::test_name -v` for the tightest feedback loop.
- When touching validation or error mapping, run the focused test **and** the related
  route test (`test_api_events.py`).
- External services (BigQuery, Anthropic, GDELT HTTP) must be mocked in all tests.

## Testing Conventions

- `asyncio_mode = "auto"` is set in `pyproject.toml`; no `@pytest.mark.asyncio` needed.
- `tests/conftest.py` sets required env vars **before** any `app.*` import occurs.
  Do not import app modules at module level in test files unless env vars are already set.
- Test database: in-memory SQLite via `sqlite+aiosqlite:///:memory:`.
- Standard fixtures (prefer reuse over duplication):
  - `async_client` — `httpx.AsyncClient` wired to the test app
  - `api_headers` — `{"X-API-Key": "test-api-key"}`
  - `db_session` — async SQLAlchemy session backed by in-memory SQLite
  - `mock_bq_client` — `MagicMock` with `run_query = AsyncMock(return_value=[])`
  - `mock_anthropic_client` — `AsyncMock`
- `clear_settings_cache` is an `autouse` fixture; `get_settings()` cache is cleared
  before and after every test automatically.

## Python Module Structure

- Start every non-trivial module with a module docstring (one-liner or short block).
- Follow the docstring with `from __future__ import annotations`.
- Import order (enforced by Ruff): future → stdlib → third-party → `app.*`.
- Use absolute imports only. No relative imports, no wildcard imports.

## Formatting And Style

- Line length: 100 characters.
- Match existing Ruff formatting exactly; do not reformat unrelated code.
- Prefer small, local edits over broad rewrites.
- Preserve existing section-divider comment style (e.g. `# ── Section ─────`).
- Add inline comments only when the code is not self-explanatory.
- Keep route handlers thin; push orchestration into `app/services/`.

## Types And Naming

- Annotate **all** function signatures (parameters and return type).
- Prefer builtin generics: `list[str]`, `dict[str, Any]`, `tuple[int, str]`.
- Prefer `str | None` over `Optional[str]`.
- Use precise return types for helpers, repositories, services, and integration wrappers.
- Reuse typed Pydantic schema models rather than passing raw `dict` across layer boundaries.

| Identifier kind | Convention |
|---|---|
| modules, functions, variables | `snake_case` |
| classes | `PascalCase` |
| constants (module-level) | `UPPER_SNAKE_CASE` |
| private helpers | `_leading_underscore` |
| log event names | short `snake_case` string literal |

## Pydantic And Settings

- Codebase uses **Pydantic v2**.
- Use `Field(...)` with constraints and descriptions for all API-facing schemas.
- `model_validate(...)` not `.parse_obj()`. `model_dump(...)` not `.dict()`.
- Cross-field validation: `@model_validator(mode="after")`.
- Raw-value preprocessing: `@field_validator(..., mode="before")`.
- Always obtain settings via `get_settings()` from `app/core/config.py`.
  Never read env vars directly with `os.environ` inside app code.

## FastAPI, Services, And Boundaries

- Inject dependencies via `Depends(...)` and `app.state`; never construct clients inline.
- HTTP concerns stay in `app/api/`; business logic stays in `app/services/`.
- SQL stays in `app/db/repositories/`; never embed raw queries in routes or services.
- External SDK calls stay in `app/integrations/`; never call SDKs directly from routes.
- `app/main.py` owns app factory, lifespan startup/shutdown, and scheduler wiring.
- Scheduler jobs are registered and wired in `app/scheduler/`; never schedule from routes.

## Error Handling

- All domain exceptions inherit from `GDELTBackendError` (`app/core/exceptions.py`).
- Add new exception classes in `app/core/exceptions.py`; map them in `app/api/error_handlers.py`.
- Services and integrations raise domain exceptions (`IngestionError`, `LocalQueryError`, etc.).
- Route handlers and dependency functions may raise `HTTPException` for request-layer errors.
- Prefer guard clauses over deep nesting.
- For non-fatal side effects, log the failure and continue (match existing patterns).

## Logging

- Use `structlog` via `app.core.logging.get_logger(__name__)`.
- No `print()` for runtime diagnostics.
- Use short, `snake_case` event strings as the first positional argument:
  `logger.info("event_ingested", url=url, count=inserted)`
- Attach context as keyword arguments, not in the message string.

## Async, Database, And Integrations

- All I/O paths are async; do not block the event loop.
- SQLAlchemy sessions are async; follow the existing `async with session_factory()` pattern.
- The BigQuery Python client is synchronous and runs inside a `ThreadPoolExecutor`
  (see `app/integrations/bigquery_client.py`). Do not call it directly from async code.
- The GDELT HTTP ingestion client (`app/integrations/gdelt_http_client.py`) is async
  and downloads `.export.CSV.zip` files from `data.gdeltproject.org`.
- Anthropic client calls stay inside `app/integrations/anthropic_client.py`.

## Agent Workflow Guidance

- Read nearby modules and their tests before making any change.
- Follow existing conventions; this repo values consistency over novelty.
- When adding behavior, add or update tests alongside the implementation.
- When touching validation logic, verify both schema behavior (unit) and HTTP response
  (route-level test in `test_api_events.py`).
- Do not introduce new tooling, formatters, or architectural layers unless explicitly asked.
- After any code change: run `ruff check .` then the focused test, then the full suite.
