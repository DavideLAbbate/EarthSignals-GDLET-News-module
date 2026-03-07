"""
FastAPI application factory and lifespan manager.

Startup sequence (in lifespan):
  1. Configure structlog
  2. Validate settings (fail fast if required env vars missing)
  3. Create BigQuery client + executor thread pool
  4. Create Anthropic async client
  5. Create APScheduler (shares uvicorn's event loop)
  6. Register the 15-minute sync job
  7. Run an initial sync immediately
  8. Start the scheduler

Shutdown sequence:
  1. Shut down the APScheduler
  2. Shut down the BigQuery executor thread pool
  3. Dispose the SQLAlchemy engine
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import structlog
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
from app.scheduler.scheduler import add_sync_job, create_scheduler, trigger_sync_now

logger = get_logger(__name__)


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

    # ── Scheduler ──────────────────────────────────────────────────────────
    scheduler = create_scheduler()
    app.state.scheduler = scheduler
    add_sync_job(scheduler, bq_client)
    scheduler.start()
    logger.info("scheduler_started")

    # ── Initial sync on startup ────────────────────────────────────────────
    # Run async to not block startup — errors are handled inside sync_job
    asyncio.create_task(trigger_sync_now(bq_client))

    logger.info("application_ready")
    yield

    # ── Shutdown ───────────────────────────────────────────────────────────
    logger.info("application_shutdown_start")

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
            "Production REST API integrating GDELT 2.0 via Google BigQuery. "
            "Filters are normalized by Claude (Anthropic) and refreshed every 15 minutes."
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
