"""
FastAPI application factory for the Event Enrichment Service.

Startup sequence (lifespan):
  1. Configure structlog
  2. Create a shared httpx.AsyncClient stored in app.state.http_client

Shutdown sequence:
  1. Close the shared httpx.AsyncClient

Routes:
  GET  /health  → liveness probe
  POST /enrich  → call Ollama and return EnrichResponse
"""

from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager

import httpx
import structlog
from fastapi import APIRouter, FastAPI
from fastapi.responses import JSONResponse

from enrichment_service.config import get_settings
from enrichment_service.enricher import EnrichmentError, call_ollama_enrich
from enrichment_service.schemas import EnrichRequest, EnrichResponse

# ── Logging helpers ────────────────────────────────────────────────────────────


def _configure_logging(log_level: str = "INFO", is_development: bool = False) -> None:
    """Configure structlog (mirrors the main app's logging.py)."""
    log_level_int = getattr(logging, log_level.upper(), logging.INFO)

    shared_processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]

    if is_development:
        renderer = structlog.dev.ConsoleRenderer(colors=True)
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=shared_processors
        + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(log_level_int)

    for noisy_logger in ("uvicorn.access",):
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a named structlog logger."""
    return structlog.get_logger(name)


logger = get_logger(__name__)

# ── Routers ────────────────────────────────────────────────────────────────────

health_router = APIRouter()


@health_router.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}


# ── Lifespan ───────────────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    _configure_logging(
        log_level=settings.log_level,
        is_development=settings.is_development,
    )
    logger.info("enrichment_service_startup", env=settings.app_env, model=settings.ollama_model)

    app.state.http_client = httpx.AsyncClient()

    yield

    logger.info("enrichment_service_shutdown")
    await app.state.http_client.aclose()


# ── App factory ────────────────────────────────────────────────────────────────


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()

    application = FastAPI(
        title="Event Enrichment Service",
        description=(
            "Local LLM enrichment microservice.  Accepts a raw news article and "
            "returns structured semantic metadata produced by a local Ollama model."
        ),
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    application.include_router(health_router)

    # ── /enrich endpoint (defined inline to access app.state cleanly) ──────
    @application.post("/enrich", response_model=EnrichResponse)
    async def _enrich_endpoint(request: EnrichRequest) -> EnrichResponse | JSONResponse:
        http_client: httpx.AsyncClient = application.state.http_client

        try:
            result = await call_ollama_enrich(
                extracted_title=request.extracted_title,
                extracted_content=request.extracted_content,
                http_client=http_client,
                settings=settings,
            )
        except EnrichmentError as exc:
            logger.warning(
                "enrichment_failed",
                error=exc.message,
                cause=str(exc.cause) if exc.cause else None,
            )
            return JSONResponse(
                status_code=422,
                content={"detail": exc.message},
            )
        except Exception as exc:
            logger.error("enrichment_unexpected_error", error=str(exc), exc_info=True)
            return JSONResponse(
                status_code=500,
                content={"detail": "Internal enrichment error"},
            )

        return result

    return application


# ── Module-level app instance (used by uvicorn) ────────────────────────────────

app = create_app()
