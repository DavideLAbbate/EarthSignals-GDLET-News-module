"""Repository for story_clusters — upsert and query materialised clusters."""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any

from sqlalchemy import cast, delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.db.models import StoryCluster

logger = get_logger(__name__)

_DEFAULT_LIMIT = 50
_MAX_LIMIT = 500

# asyncpg hard limit for bind parameters per statement.
_MAX_PG_ARGS = 32_767
# StoryCluster columns written per row (all columns except auto-increment `id`)
_CLUSTER_COLUMNS = len(StoryCluster.__table__.columns) - 1


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
        """Upsert multiple clusters in as few round-trips as possible.

        PostgreSQL path: strips transient keys then issues a single
        INSERT … ON CONFLICT (cluster_id) DO UPDATE … VALUES (row1),(row2),…
        chunked to stay below the asyncpg 32 767-parameter limit.

        SQLite path (tests): falls back to the one-by-one upsert because
        SQLite does not support multi-row VALUES with ON CONFLICT DO UPDATE
        through the same dialect interface.

        Returns the number of rows processed.
        """
        if not cluster_dicts:
            return 0

        dialect_name = (
            self._session.bind.dialect.name if self._session.bind is not None else "postgresql"
        )

        if dialect_name != "postgresql":
            # SQLite: use existing single-row upsert path (tests only)
            for c in cluster_dicts:
                await self.upsert(c)
            return len(cluster_dicts)

        from sqlalchemy.dialects.postgresql import insert as pg_insert

        # Strip transient pipeline keys that are not DB columns
        rows = [
            {k: v for k, v in c.items() if k not in self._TRANSIENT_KEYS} for c in cluster_dicts
        ]

        # Determine which columns are safe to update on conflict (exclude id and cluster_id)
        update_keys = [k for k in rows[0] if k not in ("id", "cluster_id")]

        chunk_size = max(1, math.floor(_MAX_PG_ARGS / _CLUSTER_COLUMNS))
        for start in range(0, len(rows), chunk_size):
            chunk = rows[start : start + chunk_size]
            stmt = (
                pg_insert(StoryCluster)
                .values(chunk)
                .on_conflict_do_update(
                    index_elements=["cluster_id"],
                    set_={k: getattr(pg_insert(StoryCluster).excluded, k) for k in update_keys},
                )
            )
            await self._session.execute(stmt)

        return len(cluster_dicts)

    async def search(
        self,
        *,
        min_score: float | None = None,
        country_code: str | None = None,
        limit: int = _DEFAULT_LIMIT,
        offset: int = 0,
    ) -> tuple[list[StoryCluster], int]:
        """Return (clusters, total_count) matching the filters, ordered by topic_score DESC.

        The country_code filter is pushed to SQL on both dialects:
        - PostgreSQL: JSONB containment operator ``@>`` — uses the GIN index on
          ``dominant_countries`` created by migration 010.
        - SQLite (tests): correlated EXISTS over ``json_each(dominant_countries)``.
        """
        q = select(StoryCluster)
        if min_score is not None:
            q = q.where(StoryCluster.topic_score >= min_score)

        if country_code is not None:
            dialect_name = (
                self._session.bind.dialect.name if self._session.bind is not None else "postgresql"
            )
            if dialect_name == "postgresql":
                from sqlalchemy.dialects.postgresql import JSONB

                q = q.where(
                    cast(StoryCluster.dominant_countries, JSONB).contains(
                        cast([country_code], JSONB)
                    )
                )
            else:
                # SQLite: EXISTS (SELECT 1 FROM json_each(dominant_countries) WHERE value = ?)
                q = q.where(
                    StoryCluster.dominant_countries.is_not(None),
                    text(
                        "EXISTS ("
                        "SELECT 1 FROM json_each(story_clusters.dominant_countries) "
                        "WHERE value = :cc"
                        ")"
                    ).bindparams(cc=country_code),
                )

        count_q = select(func.count()).select_from(q.with_only_columns(StoryCluster.id).subquery())
        total_result = await self._session.execute(count_q)
        total = total_result.scalar_one()

        q = q.order_by(StoryCluster.topic_score.desc()).offset(offset).limit(limit)
        result = await self._session.execute(q)
        clusters = list(result.scalars().all())
        return clusters, total

    async def delete_computed_before(self, cutoff_ts: datetime) -> int:
        """Delete clusters computed before cutoff_ts. Returns deleted count."""
        result = await self._session.execute(
            delete(StoryCluster).where(StoryCluster.computed_at < cutoff_ts)
        )
        return result.rowcount or 0
