"""Repository for tracking ingestion state and progress."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import IngestionState


class IngestionType(str, Enum):
    BOOTSTRAP = "bootstrap"
    INCREMENTAL = "incremental"


class IngestionStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


async def create_ingestion_run(
    session: AsyncSession,
    ingestion_type: IngestionType,
) -> IngestionState:
    """Create a new ingestion run record."""
    state = IngestionState(
        ingestion_type=ingestion_type.value,
        status=IngestionStatus.RUNNING.value,
        started_at=datetime.now(timezone.utc),
    )
    session.add(state)
    await session.flush()
    return state


async def update_ingestion_run(
    session: AsyncSession,
    run_id: int,
    status: IngestionStatus,
    watermark_dateadded: int | None = None,
    events_ingested: int | None = None,
    error_message: str | None = None,
) -> None:
    """Update an ingestion run with completion status."""
    values: dict[str, Any] = {
        "status": status.value,
        "completed_at": datetime.now(timezone.utc),
    }

    if watermark_dateadded is not None:
        values["watermark_dateadded"] = watermark_dateadded
    if events_ingested is not None:
        values["events_ingested"] = events_ingested
    if error_message is not None:
        values["error_message"] = error_message

    stmt = update(IngestionState).where(IngestionState.id == run_id).values(**values)

    await session.execute(stmt)


async def get_latest_successful_ingestion(
    session: AsyncSession,
    ingestion_type: IngestionType | None = None,
) -> IngestionState | None:
    """Get the most recent successful ingestion run."""
    stmt = (
        select(IngestionState)
        .where(IngestionState.status == IngestionStatus.COMPLETED.value)
        .order_by(IngestionState.completed_at.desc())
        .limit(1)
    )

    if ingestion_type:
        stmt = stmt.where(IngestionState.ingestion_type == ingestion_type.value)

    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def is_bootstrap_complete(session: AsyncSession) -> bool:
    """Check if bootstrap ingestion has completed successfully."""
    bootstrap = await get_latest_successful_ingestion(
        session,
        IngestionType.BOOTSTRAP,
    )
    return bootstrap is not None


async def get_latest_ingestion_run(
    session: AsyncSession,
) -> IngestionState | None:
    """Get the most recent ingestion run (any status)."""
    stmt = select(IngestionState).order_by(IngestionState.started_at.desc()).limit(1)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()
