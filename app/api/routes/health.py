"""
GET /health

Lightweight health check endpoint.
Performs a DB ping (SELECT 1) and returns component statuses.
No auth required (used by Docker Compose healthcheck).
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.repositories.sync_repository import db_ping
from app.db.session import get_async_session

router = APIRouter()


@router.get(
    "/health",
    summary="Health check",
    description="Returns the health status of all service components.",
    tags=["System"],
)
async def health_check(
    request: Request,
    session: AsyncSession = Depends(get_async_session),
) -> JSONResponse:
    db_ok = await db_ping(session)

    scheduler = getattr(request.app.state, "scheduler", None)
    scheduler_running = scheduler is not None and scheduler.running

    bq_client = getattr(request.app.state, "bq_client", None)
    bq_ok = bq_client is not None

    overall_status = "ok" if (db_ok and scheduler_running and bq_ok) else "degraded"
    http_status = 200 if overall_status == "ok" else 503

    return JSONResponse(
        status_code=http_status,
        content={
            "status": overall_status,
            "components": {
                "database": "ok" if db_ok else "error",
                "scheduler": "running" if scheduler_running else "stopped",
                "bigquery_client": "ok" if bq_ok else "error",
            },
        },
    )
