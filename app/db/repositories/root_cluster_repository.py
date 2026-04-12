"""Repository for root_clusters - upsert and query materialised root clusters."""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any

from sqlalchemy import cast, delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import RootCluster

_DEFAULT_LIMIT = 50
_MAX_LIMIT = 500
_MAX_PG_ARGS = 32_767
_ROOT_CLUSTER_COLUMNS = len(RootCluster.__table__.columns) - 1


def _json_contains(q, column, value: str, table_name: str, col_name: str, bind_key: str, dialect: str):
    """Return query with a JSON-array-contains filter applied (dialect-aware)."""
    if dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import JSONB

        return q.where(cast(column, JSONB).contains(cast([value], JSONB)))
    # SQLite: correlated EXISTS over json_each
    return q.where(
        column.is_not(None),
        text(
            f"EXISTS (SELECT 1 FROM json_each({table_name}.{col_name}) WHERE value = :{bind_key})"
        ).bindparams(**{bind_key: value}),
    )


class RootClusterRepository:
    """Data access layer for the root_clusters table."""

    _TRANSIENT_KEYS: frozenset[str] = frozenset(
        {"gkg_doc_count", "component_source_urls", "component_domains", "merge_evidence"}
    )

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(self, cluster_dict: dict[str, Any]) -> None:
        """Insert or update a root cluster row keyed by cluster_id."""
        cluster_dict = {k: v for k, v in cluster_dict.items() if k not in self._TRANSIENT_KEYS}

        dialect_name = (
            self._session.bind.dialect.name if self._session.bind is not None else "postgresql"
        )
        if dialect_name == "sqlite":
            await self._session.execute(
                delete(RootCluster).where(RootCluster.cluster_id == cluster_dict["cluster_id"])
            )
            self._session.add(RootCluster(**cluster_dict))
        else:
            from sqlalchemy.dialects.postgresql import insert as pg_insert

            update_cols = {k: v for k, v in cluster_dict.items() if k not in ("id", "cluster_id")}
            stmt = (
                pg_insert(RootCluster)
                .values(cluster_dict)
                .on_conflict_do_update(index_elements=["cluster_id"], set_=update_cols)
            )
            await self._session.execute(stmt)

    async def bulk_upsert(self, cluster_dicts: list[dict[str, Any]]) -> int:
        """Upsert multiple root clusters in as few round-trips as possible."""
        if not cluster_dicts:
            return 0

        dialect_name = (
            self._session.bind.dialect.name if self._session.bind is not None else "postgresql"
        )
        if dialect_name != "postgresql":
            for cluster in cluster_dicts:
                await self.upsert(cluster)
            return len(cluster_dicts)

        from sqlalchemy.dialects.postgresql import insert as pg_insert

        rows = [
            {k: v for k, v in cluster.items() if k not in self._TRANSIENT_KEYS}
            for cluster in cluster_dicts
        ]
        update_keys = [k for k in rows[0] if k not in ("id", "cluster_id")]

        chunk_size = max(1, math.floor(_MAX_PG_ARGS / _ROOT_CLUSTER_COLUMNS))
        for start in range(0, len(rows), chunk_size):
            chunk = rows[start : start + chunk_size]
            stmt = (
                pg_insert(RootCluster)
                .values(chunk)
                .on_conflict_do_update(
                    index_elements=["cluster_id"],
                    set_={k: getattr(pg_insert(RootCluster).excluded, k) for k in update_keys},
                )
            )
            await self._session.execute(stmt)

        return len(cluster_dicts)

    async def search(
        self,
        *,
        min_score: float | None = None,
        min_event_count: int | None = None,
        min_mentions: int | None = None,
        country_code: str | None = None,
        date_from: int | None = None,
        date_to: int | None = None,
        mentioned_after: datetime | None = None,
        mentioned_before: datetime | None = None,
        enrichment_status: str | None = None,
        event_type: str | None = None,
        quad_class: str | None = None,
        theme: str | None = None,
        keyword: str | None = None,
        topic: str | None = None,
        limit: int = _DEFAULT_LIMIT,
        offset: int = 0,
    ) -> tuple[list[RootCluster], int]:
        """Return (clusters, total_count) matching the filters, ordered by topic_score DESC."""
        limit = max(1, min(limit, _MAX_LIMIT))

        dialect_name = (
            self._session.bind.dialect.name if self._session.bind is not None else "postgresql"
        )

        q = select(RootCluster)

        # ── Scalar filters ──────────────────────────────────────────────────
        if min_score is not None:
            q = q.where(RootCluster.topic_score >= min_score)
        if min_event_count is not None:
            q = q.where(RootCluster.event_count >= min_event_count)
        if min_mentions is not None:
            q = q.where(RootCluster.mention_count >= min_mentions)
        if date_from is not None:
            q = q.where(RootCluster.event_date_ref_start >= date_from)
        if date_to is not None:
            q = q.where(RootCluster.event_date_ref_start <= date_to)
        if mentioned_after is not None:
            q = q.where(RootCluster.first_mention_at >= mentioned_after)
        if mentioned_before is not None:
            q = q.where(RootCluster.last_mention_at <= mentioned_before)
        if enrichment_status is not None:
            q = q.where(RootCluster.enrichment_status == enrichment_status)

        # ── JSON-array containment filters ──────────────────────────────────
        _tbl = "root_clusters"
        if country_code is not None:
            q = _json_contains(
                q, RootCluster.dominant_countries, country_code,
                _tbl, "dominant_countries", "cc", dialect_name,
            )
        if event_type is not None:
            q = _json_contains(
                q, RootCluster.dominant_event_types, event_type,
                _tbl, "dominant_event_types", "et", dialect_name,
            )
        if quad_class is not None:
            q = _json_contains(
                q, RootCluster.dominant_quad_classes, quad_class,
                _tbl, "dominant_quad_classes", "qc", dialect_name,
            )
        if theme is not None:
            q = _json_contains(
                q, RootCluster.themes, theme,
                _tbl, "themes", "th", dialect_name,
            )
        if keyword is not None:
            q = _json_contains(
                q, RootCluster.keywords, keyword,
                _tbl, "keywords", "kw", dialect_name,
            )
        if topic is not None:
            q = _json_contains(
                q, RootCluster.main_topics, topic,
                _tbl, "main_topics", "tp", dialect_name,
            )

        count_q = select(func.count()).select_from(q.with_only_columns(RootCluster.id).subquery())
        total_result = await self._session.execute(count_q)
        total = total_result.scalar_one()

        q = q.order_by(RootCluster.topic_score.desc()).offset(offset).limit(limit)
        result = await self._session.execute(q)
        clusters = list(result.scalars().all())
        return clusters, total

    async def delete_computed_before(self, cutoff_ts: datetime) -> int:
        """Delete root clusters computed before cutoff_ts. Returns deleted count."""
        result = await self._session.execute(
            delete(RootCluster).where(RootCluster.computed_at < cutoff_ts)
        )
        return result.rowcount or 0

    async def delete_by_cluster_ids(self, cluster_ids: set[str]) -> int:
        """Delete root clusters whose cluster_id is in cluster_ids."""
        if not cluster_ids:
            return 0

        result = await self._session.execute(
            delete(RootCluster).where(RootCluster.cluster_id.in_(cluster_ids))
        )
        return result.rowcount or 0

    async def list_cluster_ids(self) -> set[str]:
        """Return all materialized root cluster IDs."""
        result = await self._session.execute(select(RootCluster.cluster_id))
        return set(result.scalars().all())

    async def exists_by_cluster_id(self, cluster_id: str) -> bool:
        """Return whether a root cluster exists for the given cluster_id."""
        result = await self._session.execute(
            select(RootCluster.id).where(RootCluster.cluster_id == cluster_id).limit(1)
        )
        return result.scalar_one_or_none() is not None
