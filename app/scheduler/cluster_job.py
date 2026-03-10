"""Scheduled wrapper for periodic story-cluster materialisation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.services.cluster_service import ClusterService


async def run_cluster_job(
    session_factory: async_sessionmaker[AsyncSession],
) -> int:
    """Build story clusters for the last 36 hours using a fresh DB session.

    A 36-hour rolling window (vs the 24-hour job interval) provides a 12-hour
    overlap that absorbs GDELT ingestion latency without risking coverage gaps.
    Duplicate clusters are handled by the upsert in ClusterRepository.
    """
    since_dt = datetime.now(UTC) - timedelta(hours=36)

    async with session_factory() as session:
        return await ClusterService(session).build_and_materialise(since_dt)
