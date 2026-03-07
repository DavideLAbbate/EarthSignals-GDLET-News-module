"""
FastAPI dependency providers.

verify_api_key — validates X-API-Key header on protected endpoints
get_db_session  — yields an async SQLAlchemy session
get_bq_client   — returns the BigQuery client from app state
get_anthropic_client — returns the Anthropic client from app state
"""

from __future__ import annotations

import anthropic
from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader

from app.core.config import get_settings
from app.integrations.bigquery_client import BigQueryClientWrapper

# ── API Key Auth ──────────────────────────────────────────────────────────

_api_key_scheme = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: str = Security(_api_key_scheme)) -> str:
    """
    Validate the X-API-Key header.

    Raises HTTP 401 if the key is missing.
    Raises HTTP 403 if the key is invalid.
    """
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "missing_api_key", "message": "X-API-Key header is required"},
        )
    settings = get_settings()
    if api_key != settings.api_key:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"error": "invalid_api_key", "message": "Invalid API key"},
        )
    return api_key


# ── Database Session ──────────────────────────────────────────────────────

from app.db.session import get_async_session  # noqa: E402


# ── App State Accessors ───────────────────────────────────────────────────


async def get_bq_client(request: Request) -> BigQueryClientWrapper:
    """Return the BigQuery client singleton from app state."""
    client = getattr(request.app.state, "bq_client", None)
    if client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "bq_unavailable", "message": "BigQuery client not initialized"},
        )
    return client


async def get_anthropic_client(request: Request) -> anthropic.AsyncAnthropic:
    """Return the Anthropic client singleton from app state."""
    client = getattr(request.app.state, "anthropic_client", None)
    if client is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "anthropic_unavailable", "message": "Anthropic client not initialized"},
        )
    return client


async def get_scheduler(request: Request):
    """Return the APScheduler instance from app state."""
    return getattr(request.app.state, "scheduler", None)
