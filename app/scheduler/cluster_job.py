"""Scheduled wrapper for periodic story-cluster materialisation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.services.cluster_service import ClusterService


async def run_cluster_job(
    session_factory: async_sessionmaker[AsyncSession],
) -> int:
    """Build story clusters for the last 30 days using a fresh DB session."""
    since_dt = datetime.now(UTC) - timedelta(days=30)
    since_sqldate = int(since_dt.strftime("%Y%m%d"))

    async with session_factory() as session:
        return await ClusterService(session).build_and_materialise(since_sqldate)
