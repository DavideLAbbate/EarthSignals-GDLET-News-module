"""Repository for story_clusters — upsert and query materialised clusters."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.db.models import StoryCluster

logger = get_logger(__name__)

_DEFAULT_LIMIT = 50
_MAX_LIMIT = 500


class ClusterRepository:
    """Data access layer for the story_clusters table."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # Keys that are used internally during cluster building/merging but are not DB columns
    _TRANSIENT_KEYS: frozenset[str] = frozenset({"gkg_doc_count"})

    async def upsert(self, cluster_dict: dict[str, Any]) -> None:
        """Insert or update a story cluster row keyed by cluster_id.

        Uses dialect-aware upsert:
        - PostgreSQL: INSERT … ON CONFLICT (cluster_id) DO UPDATE
        - SQLite: plain INSERT — if the cluster_id already exists the
          existing row is deleted first (delete-then-insert pattern).

        This ensures the cluster is always up-to-date after each
        materialisation run.
        """
        # Strip transient keys that are used during pipeline processing but are
        # not persisted as DB columns (e.g. gkg_doc_count for weighted tone avg)
        cluster_dict = {k: v for k, v in cluster_dict.items() if k not in self._TRANSIENT_KEYS}

        dialect_name = (
            self._session.bind.dialect.name if self._session.bind is not None else "postgresql"
        )
        if dialect_name == "sqlite":
            # SQLite: delete-then-insert for upsert semantics
            await self._session.execute(
                delete(StoryCluster).where(StoryCluster.cluster_id == cluster_dict["cluster_id"])
            )
            self._session.add(StoryCluster(**cluster_dict))
        else:
            from sqlalchemy.dialects.postgresql import insert as pg_insert

            update_cols = {k: v for k, v in cluster_dict.items() if k not in ("id", "cluster_id")}
            stmt = (
                pg_insert(StoryCluster)
                .values(cluster_dict)
                .on_conflict_do_update(
                    index_elements=["cluster_id"],
                    set_=update_cols,
                )
            )
            await self._session.execute(stmt)

    async def bulk_upsert(self, cluster_dicts: list[dict[str, Any]]) -> int:
        """Upsert multiple clusters. Returns the number of rows processed."""
        for c in cluster_dicts:
            await self.upsert(c)
        return len(cluster_dicts)

    async def search(
        self,
        *,
        min_score: float | None = None,
        country_code: str | None = None,
        limit: int = _DEFAULT_LIMIT,
        offset: int = 0,
    ) -> tuple[list[StoryCluster], int]:
        """Return (clusters, total_count) matching the filters, ordered by topic_score DESC."""
        q = select(StoryCluster)
        if min_score is not None:
            q = q.where(StoryCluster.topic_score >= min_score)

        if country_code is None:
            count_q = select(func.count()).select_from(
                q.with_only_columns(StoryCluster.id).subquery()
            )
            total_result = await self._session.execute(count_q)
            total = total_result.scalar_one()

            q = q.order_by(StoryCluster.topic_score.desc()).offset(offset).limit(limit)
            result = await self._session.execute(q)
            clusters = list(result.scalars().all())
            return clusters, total

        result = await self._session.execute(q.order_by(StoryCluster.topic_score.desc()))
        filtered_clusters = [
            cluster
            for cluster in result.scalars().all()
            if cluster.dominant_countries and country_code in cluster.dominant_countries
        ]
        total = len(filtered_clusters)
        return filtered_clusters[offset : offset + limit], total

    async def delete_computed_before(self, cutoff_ts: datetime) -> int:
        """Delete clusters computed before cutoff_ts. Returns deleted count."""
        result = await self._session.execute(
            delete(StoryCluster).where(StoryCluster.computed_at < cutoff_ts)
        )
        return result.rowcount or 0
