"""
FastAPI application factory and lifespan manager.

Startup sequence (in lifespan):
  1. Configure structlog
  2. Validate settings (fail fast if required vars are missing)
  3. Create integration clients used by the app
  4. Create APScheduler (shares uvicorn's event loop)
  5. Register scheduled jobs
  6. Start the scheduler
  7. Schedule one-off startup tasks (metadata refresh, bootstrap check)

Shutdown sequence:
  1. Drain or cancel tracked startup tasks
  2. Shut down the APScheduler
  3. Shut down long-lived integration clients
  4. Dispose the SQLAlchemy engine
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from app.api.error_handlers import register_error_handlers
from app.api.routes import events, filters, health, interpret, sync
from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger
from app.db.session import dispose_engine
from app.integrations.anthropic_client import create_anthropic_client
from app.integrations.bigquery_client import create_bigquery_client
from app.scheduler.scheduler import (
    add_sync_job,
    create_scheduler,
    trigger_startup_ingestion_if_needed,
    trigger_sync_now,
)

logger = get_logger(__name__)

STARTUP_TASK_SHUTDOWN_TIMEOUT_SECONDS = 1.0


async def _run_logged_startup_task(task_name: str, coroutine: Awaitable[object]) -> None:
    """Run a startup task and log failures so they are never left unobserved."""
    try:
        await coroutine
    except asyncio.CancelledError:
        logger.info("startup_task_cancelled", task_name=task_name)
        raise
    except Exception as exc:
        logger.error(
            "startup_task_failed",
            task_name=task_name,
            error=str(exc),
        )


def _schedule_startup_task(
    app: FastAPI, task_name: str, coroutine: Awaitable[object]
) -> asyncio.Task:
    """Create, name, and track a startup task for coordinated shutdown."""
    startup_tasks = app.state.startup_tasks
    task = asyncio.create_task(
        _run_logged_startup_task(task_name, coroutine),
        name=f"startup:{task_name}",
    )
    startup_tasks.append(task)
    return task


async def _shutdown_startup_tasks(
    startup_tasks: list[asyncio.Task],
    *,
    timeout_seconds: float = STARTUP_TASK_SHUTDOWN_TIMEOUT_SECONDS,
) -> None:
    """Wait briefly for startup tasks, then cancel any that are still running."""
    if not startup_tasks:
        return

    tracked_startup_tasks = list(startup_tasks)
    done, pending = await asyncio.wait(tracked_startup_tasks, timeout=timeout_seconds)
    if pending:
        logger.info(
            "startup_tasks_cancelling",
            pending_count=len(pending),
            timeout_seconds=timeout_seconds,
        )
        for task in pending:
            task.cancel()

        await asyncio.gather(*pending, return_exceptions=True)

    startup_tasks.clear()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan context manager.

    All startup code runs before `yield`; all shutdown code runs after.
    The APScheduler is started here to share uvicorn's running event loop.
    """
    settings = get_settings()
    configure_logging(log_level=settings.log_level, is_development=settings.is_development)

    logger.info("application_startup", env=settings.app_env)

    # ── BigQuery client ────────────────────────────────────────────────────
    bq_client = create_bigquery_client()
    app.state.bq_client = bq_client

    # ── Anthropic client ───────────────────────────────────────────────────
    anthropic_client = create_anthropic_client()
    app.state.anthropic_client = anthropic_client
    app.state.startup_tasks = []

    # ── Scheduler ──────────────────────────────────────────────────────────
    scheduler = create_scheduler()
    app.state.scheduler = scheduler
    add_sync_job(scheduler, bq_client)
    scheduler.start()
    logger.info("scheduler_started")

    # ── Initial sync on startup ────────────────────────────────────────────
    # Run async to not block startup — errors are handled inside sync_job
    if settings.enable_metadata_sync:
        _schedule_startup_task(app, "metadata_sync", trigger_sync_now(bq_client))
    _schedule_startup_task(app, "startup_ingestion", trigger_startup_ingestion_if_needed())

    logger.info("application_ready")
    yield

    # ── Shutdown ───────────────────────────────────────────────────────────
    logger.info("application_shutdown_start")

    await _shutdown_startup_tasks(app.state.startup_tasks)

    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("scheduler_stopped")

    bq_client.shutdown()

    await dispose_engine()
    logger.info("application_shutdown_complete")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()

    app = FastAPI(
        title="GDELT News Backend",
        description=(
            "Production REST API for searching locally cached GDELT 2.0 events. "
            "Filters are normalized by Claude (Anthropic), while metadata and ingestion "
            "are refreshed by background jobs."
        ),
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # ── Rate limiting ──────────────────────────────────────────────────────
    limiter = Limiter(key_func=get_remote_address)
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

    # ── CORS ───────────────────────────────────────────────────────────────
    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_credentials=True,
            allow_methods=["GET", "POST"],
            allow_headers=["Content-Type", "X-API-Key"],
        )

    # ── Error handlers ─────────────────────────────────────────────────────
    register_error_handlers(app)

    # ── Routes ─────────────────────────────────────────────────────────────
    app.include_router(health.router)
    app.include_router(events.router)
    app.include_router(filters.router)
    app.include_router(sync.router)
    app.include_router(interpret.router)

    return app


# Application instance (used by uvicorn: `uvicorn app.main:app`)
app = create_app()
