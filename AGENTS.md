# AGENTS.md - Repository Guidance For Coding Agents

Repository-specific guidance for agentic coding tools working in
`web-journal-news-module` (`gdelt-news-backend`).

This is a Python 3.11+ FastAPI backend that:
- queries GDELT 2.0 through Google BigQuery,
- uses Anthropic to normalize free-text filters,
- caches normalized filters in PostgreSQL,
- exposes authenticated REST endpoints,
- runs a periodic APScheduler sync job.

## Rule Files
- No `.cursorrules` file found.
- No `.cursor/rules/` directory found.
- No `.github/copilot-instructions.md` file found.
- Follow this file and nearby code patterns first.

## Project Layout
- `app/api/` - routes, dependencies, auth checks, error handlers
- `app/services/` - orchestration and business logic
- `app/integrations/` - BigQuery, Anthropic, query building, mapping helpers
- `app/db/` - models, session management, repositories
- `app/core/` - settings, logging, exceptions
- `app/scheduler/` - scheduler wiring and sync job
- `app/schemas/` - Pydantic request/response models
- `tests/` - pytest suite with mocked external services
- `alembic/versions/` - database migrations

## Install, Build, And Run
```bash
pip install -e ".[dev]"
uvicorn app.main:create_app --factory --reload --port 8000
docker-compose up --build
alembic upgrade head
alembic revision --autogenerate -m "description"
```

Notes:
- `pyproject.toml` uses `hatchling` as the build backend.
- There is no separate frontend build pipeline.
- For verification, lint + targeted tests are usually more useful than packaging.

## Lint And Format
```bash
ruff check .
ruff check --fix .
ruff format .
ruff format --check .
```

- Ruff is the only configured formatter/linter.
- Ruff config lives in `pyproject.toml`.
- Line length is `100`; target version is `py311`.

## Test Commands
```bash
pytest
pytest -v
pytest tests/test_api_events.py
pytest tests/test_api_events.py::test_search_events_success -v
pytest -k "filter" -v
pytest -s -v
pytest --tb=short
```

Single-test guidance:
- Prefer `pytest path/to/file.py::test_name -v` for the smallest useful loop.
- If you change validation or error mapping, run the focused test plus the related route test.
- Keep BigQuery and Anthropic fully mocked in tests.

## Testing Conventions
- `pytest` is configured with `asyncio_mode = "auto"`.
- `tests/conftest.py` sets required env vars before importing `app.*` modules.
- Test DB is in-memory SQLite: `sqlite+aiosqlite:///:memory:`.
- Prefer `httpx.AsyncClient` for async route tests.
- Reuse fixtures like `async_client`, `api_headers`, `db_session`, `mock_bq_client`,
  and `mock_anthropic_client`.
- Avoid module-level imports of app code in tests unless env vars are already prepared.

## Python Module Structure
- Start non-trivial Python modules with a module docstring.
- Follow the docstring with `from __future__ import annotations`.
- Group imports in this order: future, stdlib, third-party, `app.*`.
- Use absolute imports only; do not add relative imports or wildcard imports.

## Formatting And Style
- Match existing Ruff formatting; do not add a second formatter.
- Keep lines within `100` characters when practical.
- Prefer small, local edits over broad rewrites.
- Preserve existing file structure and section-divider comment style in longer modules.
- Add comments only when the code is not self-explanatory.
- Keep route handlers thin; move orchestration into services.

## Types And Naming
- Annotate all function signatures.
- Prefer builtin generics like `list[str]` and `dict[str, Any]`.
- Prefer `str | None` over `Optional[str]`.
- Use precise return types for helpers, repositories, services, and integrations.
- Reuse typed schema models instead of passing untyped dictionaries around.
- modules/functions/variables: `snake_case`
- classes: `PascalCase`
- constants: `UPPER_SNAKE_CASE`
- private helpers: leading underscore
- log event names: short `snake_case` strings

## Pydantic And Settings
- This codebase uses Pydantic v2.
- Use `Field(...)` constraints and descriptions for API-facing schemas.
- Prefer `model_validate(...)` over legacy parsing helpers.
- Prefer `model_dump(...)` over `.dict()`.
- Use `@model_validator(mode="after")` for cross-field validation.
- Use `@field_validator(..., mode="before")` when preprocessing raw values.
- Reuse `get_settings()` from `app/core/config.py` instead of reading env vars ad hoc.

## FastAPI, Services, And Boundaries
- Prefer dependency injection via `Depends(...)` and app state.
- Keep HTTP concerns in `app/api/` and business logic in `app/services/`.
- Reuse repository helpers in `app/db/repositories/` instead of embedding SQL in routes.
- Reuse integration wrappers; do not call external SDKs directly from route handlers.
- `app/main.py` owns app creation, lifespan startup, and shutdown wiring.

## Error Handling
- Business/domain exceptions should inherit from `GDELTBackendError`.
- Add new domain exceptions in `app/core/exceptions.py`.
- Map them centrally in `app/api/error_handlers.py`.
- API/dependency modules may raise `HTTPException` for request-layer concerns.
- Services and integrations should raise domain exceptions instead of FastAPI exceptions.
- Prefer guard clauses and explicit validation over deep nesting.
- For non-fatal side effects, log the failure and continue when that matches existing behavior.

## Logging
- Use `structlog` through `app.core.logging.get_logger(...)`.
- Do not add `print()` for runtime diagnostics.
- Log event-style messages with structured key/value fields.
- Keep logs concise and useful for debugging async flows.

## Async, Database, And Integrations
- Treat I/O paths as async.
- SQLAlchemy sessions are async; follow existing async session patterns.
- BigQuery's Python client is synchronous; keep calls behind the existing wrapper/executor.
- Anthropic calls should stay inside the integration layer.
- Scheduler behavior belongs in `app/scheduler/`, not API routes.

## Agent Workflow Guidance
- Inspect nearby modules and tests before changing behavior.
- Follow existing conventions first; this repo values consistency over novelty.
- When adding or changing behavior, update tests first or alongside the implementation.
- When touching validation, verify both schema behavior and route-level responses.
- Avoid adding new tooling, formatting systems, or architectural layers unless asked.
- If work touches external integrations, keep tests mocked and focused on repository logic.
