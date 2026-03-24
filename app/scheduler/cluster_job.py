"""Scheduled wrapper for periodic story-cluster materialisation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.exceptions import ClusterBuildError
from app.db.models import GdeltEvent
from app.services.cluster_service import ClusterService


async def run_cluster_job(
    session_factory: async_sessionmaker[AsyncSession],
) -> int:
    """Build story clusters for the last 36 hours using a fresh DB session.

    A 36-hour rolling window (vs the 24-hour job interval) provides a 12-hour
    overlap that absorbs GDELT ingestion latency without risking coverage gaps.
    Duplicate clusters are handled by the upsert in ClusterRepository.
    """
    async with session_factory() as session:
        latest_date_added = await session.scalar(select(func.max(GdeltEvent.date_added)))
        if latest_date_added is None:
            return 0

        until_dt = _parse_gdelt_date_added(latest_date_added)
        since_dt = until_dt - timedelta(hours=36)
        try:
            count = await ClusterService(session).build_and_materialise(since_dt, latest_date_added)
        except ClusterBuildError as exc:
            _raise_if_component_tables_missing(exc)
            raise
        await session.commit()
        return count


def _parse_gdelt_date_added(value: int) -> datetime:
    """Convert a GDELT DATEADDED integer to UTC datetime."""
    return datetime.strptime(str(value), "%Y%m%d%H%M%S").replace(tzinfo=UTC)


def _raise_if_component_tables_missing(exc: ClusterBuildError) -> None:
    """Raise an actionable migration hint when persistent component tables are missing."""
    detail = exc.detail or ""
    if 'relation "cluster_components" does not exist' not in detail:
        return
    raise RuntimeError(
        "cluster persistence tables are missing; run `alembic upgrade head` before materialising clusters"
    ) from exc
