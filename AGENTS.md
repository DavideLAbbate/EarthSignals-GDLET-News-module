# AGENTS.md — Repository Guidance For Coding Agents

Repository-specific guidance for agentic coding tools working in
`web-journal-news-module` (`gdelt-news-backend`).

This is a Python 3.11+ FastAPI backend that:
- ingests GDELT 2.0 public HTTP export files into PostgreSQL
- normalizes free-text search filters with Anthropic
- serves authenticated REST endpoints from the local event store
- runs APScheduler jobs for metadata sync, ingestion, enrichment, and clustering

## Rule Files

- No `.cursorrules` file found.
- No `.cursor/rules/` directory found.
- No `.github/copilot-instructions.md` file found.
- Follow this file, nearby modules, and nearby tests first.

## Project Layout

```text
app/api/          routes, dependencies, auth, error handlers
app/core/         settings, logging, domain exceptions
app/db/           SQLAlchemy models, sessions, repositories
app/integrations/ Anthropic, GDELT HTTP, article/enrichment clients
app/scheduler/    APScheduler wiring and startup jobs
app/schemas/      Pydantic v2 request/response models
app/services/     orchestration and business logic
alembic/versions/ schema migrations
tests/            pytest suite with mocked external services
```

Key runtime flow:

```text
GDELT HTTP exports -> ingestion_service -> gdelt_events / mentions / gkg tables
POST /events/search -> filter_service -> repositories -> PostgreSQL
startup + APScheduler -> sync, incremental ingestion, enrichment, clustering
```

## Install, Run, And Migrations

```bash
pip install -e ".[dev]"
uvicorn app.main:create_app --factory --reload --port 8000
docker compose up --build
alembic upgrade head
alembic revision --autogenerate -m "description"
```

- `pyproject.toml` uses `hatchling`; there is no separate build step beyond install.
- Prefer local verification with Ruff + pytest over packaging work.
- `docker-compose.local.yml` starts only Postgres for local non-container app development.

## Lint And Format

```bash
ruff check .
ruff check --fix .
ruff format .
ruff format --check .
```

- Ruff is the only configured linter/formatter; do not add another one.
- Repository style is whatever Ruff enforces plus local conventions in nearby files.
- Before finishing code changes, run at least `ruff check .`.

## Test Commands

```bash
pytest
pytest -v
pytest --tb=short
pytest -s -v
pytest tests/test_api_events.py
pytest tests/test_api_events.py::test_search_events_success -v
pytest tests/test_run_bootstrap_range.py -v
pytest -k "bootstrap" -v
```

Single-test guidance:

- Prefer `pytest path/to/file.py::test_name -v` for the fastest loop.
- When touching validation, run both the focused schema/unit test and the relevant API route test.
- When touching startup, scheduler, or ingestion behavior, run the nearest targeted test first.
- External integrations must stay mocked in tests; never require live Anthropic or GDELT access.

## Testing Conventions

- `pytest` is configured with `asyncio_mode = "auto"`; no `@pytest.mark.asyncio` needed.
- `tests/conftest.py` seeds env vars before importing app modules; preserve that order.
- Tests use in-memory SQLite via `sqlite+aiosqlite:///:memory:`.
- Reuse shared fixtures instead of duplicating setup:
  - `async_client`
  - `api_headers`
  - `db_session`
  - `mock_anthropic_client`
- `get_settings()` is cached; tests rely on clearing that cache between cases.

## Python File Structure

- Start every non-trivial module with a short module docstring.
- Follow the docstring with `from __future__ import annotations`.
- Import order: future -> stdlib -> third-party -> `app.*`.
- Use absolute imports only; no relative imports, no wildcard imports.

## Formatting And General Style

- Line length is 100 characters.
- Keep edits small and local; do not reformat unrelated code.
- Preserve existing section-divider comments such as `# ── Section ─────`.
- Add comments only when the code is not obvious from names and structure.
- Prefer guard clauses over deep nesting.
- Keep route handlers thin; push orchestration into `app/services/`.
- Keep repository code in `app/db/repositories/`; do not inline SQL in routes or services.

## Types And Naming

- Annotate every function signature, including return types.
- Prefer builtin generics: `list[str]`, `dict[str, Any]`, `tuple[int, str]`.
- Prefer `str | None` over `Optional[str]`.
- Use precise return types for helpers, services, repositories, and integrations.
- Pass typed schema/domain objects instead of loose dicts when a model already exists.

Naming conventions:

| Kind | Convention |
|---|---|
| modules, functions, variables | `snake_case` |
| classes | `PascalCase` |
| constants | `UPPER_SNAKE_CASE` |
| private helpers | `_leading_underscore` |
| log event names | short `snake_case` literals |

## Pydantic And Settings

- Codebase uses Pydantic v2 and `pydantic-settings`.
- Use `Field(...)` with constraints/descriptions for API-facing schemas.
- Use `model_validate(...)` and `model_dump(...)`, not v1 APIs.
- Use `@field_validator(..., mode="before")` for raw preprocessing.
- Use `@model_validator(mode="after")` for cross-field validation.
- In app code, read configuration only through `get_settings()` from `app/core/config.py`.

## FastAPI, Services, And Boundaries

- Inject dependencies with `Depends(...)` and `app.state`; do not instantiate clients inline.
- HTTP concerns stay in `app/api/`.
- Business logic stays in `app/services/`.
- External HTTP/SDK calls stay in `app/integrations/`.
- `app/main.py` owns app factory, lifespan startup/shutdown, and startup task scheduling.
- Scheduler registration belongs in `app/scheduler/`, not in route handlers.

## Error Handling

- Domain exceptions inherit from `GDELTBackendError` in `app/core/exceptions.py`.
- Add new exception classes there and map them in `app/api/error_handlers.py`.
- Services/integrations should raise domain exceptions, not FastAPI `HTTPException`.
- Routes/dependencies may raise `HTTPException` for request-layer failures only.
- For non-fatal side effects, log the failure and continue when existing code follows that pattern.

## Logging

- Use `structlog` via `app.core.logging.get_logger(__name__)`.
- Do not use `print()` for runtime diagnostics inside app code.
- First log argument should be a short event name, e.g. `logger.info("bootstrap_completed", ...)`.
- Put structured details in keyword arguments, not interpolated message strings.

## Async, Database, And Integration Notes

- Keep I/O paths async; do not block the event loop.
- Use async SQLAlchemy sessions with the existing session factory patterns.
- GDELT ingestion is HTTP-based and can be long-running on first startup.
- Startup bootstrap runs automatically when `gdelt_events` is empty.
- Default `RETENTION_DAYS=30` means roughly 2,880 event files, plus matching mentions and GKG files.
- If `docker compose up` appears idle during first bootstrap, inspect container logs before assuming a hang.

## Agent Workflow Guidance

- Read the implementation file and its nearest tests before editing.
- Follow existing conventions over clever rewrites.
- When adding behavior, add or update tests in the same change.
- After code changes, run `ruff check .`, then focused tests, then the full suite when practical.
- Do not introduce new tooling, architecture layers, or formatting systems unless explicitly requested.
