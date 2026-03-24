"""Cleanup service for terminal cluster components."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.logging import get_logger
from app.db.repositories.cluster_component_repository import ClusterComponentRepository

logger = get_logger(__name__)


def _utcnow() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(UTC)


async def run_cluster_terminal_cleanup(session: AsyncSession) -> dict[str, Any]:
    """Delete terminal cluster components older than the configured retention window."""
    settings = get_settings()
    cutoff = _utcnow() - timedelta(days=settings.cluster_terminal_state_retention_days)
    logger.info(
        "starting_cluster_terminal_cleanup",
        retention_days=settings.cluster_terminal_state_retention_days,
        cutoff_iso=cutoff.isoformat(),
    )

    repo = ClusterComponentRepository(session)
    deleted = await repo.delete_terminal_components_before(cutoff)
    await session.commit()

    logger.info(
        "cluster_terminal_cleanup_completed",
        deleted_components=deleted["components"],
        deleted_memberships=deleted["memberships"],
        cutoff_iso=cutoff.isoformat(),
    )
    return {
        "deleted_components": deleted["components"],
        "deleted_memberships": deleted["memberships"],
        "cutoff_iso": cutoff.isoformat(),
    }
