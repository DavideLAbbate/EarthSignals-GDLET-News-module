"""
Repository for SyncState and FilterMappingCache database operations.

All methods accept an AsyncSession and are designed to be called
from within a managed session context (FastAPI dependency or scheduler).
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import desc, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import FilterMappingCache, SyncState


# ── SyncState operations ──────────────────────────────────────────────────


async def get_latest_sync_state(session: AsyncSession) -> SyncState | None:
    """Return the most recent SyncState row, or None if no syncs have run."""
    result = await session.execute(
        select(SyncState).order_by(desc(SyncState.synced_at)).limit(1)
    )
    return result.scalar_one_or_none()


async def upsert_sync_state(
    session: AsyncSession,
    *,
    latest_sqldate: int | None,
    latest_dateadded: int | None,
    top_countries: list[dict],
    top_event_root_codes: list[dict],
    mapping_version: str,
    sync_status: str = "success",
    error_message: str | None = None,
) -> SyncState:
    """
    Insert a new SyncState row representing the completed sync.

    Each sync produces a new row (append-only log). The latest row
    is always retrieved by ordering on synced_at DESC.
    """
    now = datetime.now(timezone.utc)
    sync_state = SyncState(
        latest_sqldate=latest_sqldate,
        latest_dateadded=latest_dateadded,
        top_countries=top_countries,
        top_event_root_codes=top_event_root_codes,
        mapping_version=mapping_version,
        synced_at=now,
        sync_status=sync_status,
        error_message=error_message,
    )
    session.add(sync_state)
    await session.flush()
    return sync_state


async def db_ping(session: AsyncSession) -> bool:
    """Lightweight DB connectivity check. Returns True if DB is reachable."""
    try:
        await session.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


# ── FilterMappingCache operations ─────────────────────────────────────────


def compute_cache_key(raw_filter_input: dict) -> str:
    """
    Compute a deterministic SHA256 cache key from a raw filter input dict.

    The dict is canonicalized: keys sorted, string values lowercased.
    """
    canonical = json.dumps(
        {k: (v.lower() if isinstance(v, str) else v) for k, v in sorted(raw_filter_input.items())},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


async def get_cached_filter(
    session: AsyncSession, cache_key: str
) -> FilterMappingCache | None:
    """
    Return a non-expired cache entry for the given key, or None.
    """
    now = datetime.now(timezone.utc)
    result = await session.execute(
        select(FilterMappingCache).where(
            FilterMappingCache.cache_key == cache_key,
            FilterMappingCache.expires_at > now,
        )
    )
    return result.scalar_one_or_none()


async def upsert_cached_filter(
    session: AsyncSession,
    *,
    cache_key: str,
    raw_input: dict,
    normalized_filters: dict,
    ttl_hours: int = 24,
) -> FilterMappingCache:
    """
    Insert or update a filter mapping cache entry.

    Uses PostgreSQL ON CONFLICT DO UPDATE to handle concurrent writes.
    """
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(hours=ttl_hours)

    stmt = (
        pg_insert(FilterMappingCache)
        .values(
            cache_key=cache_key,
            raw_input=raw_input,
            normalized_filters=normalized_filters,
            created_at=now,
            expires_at=expires_at,
        )
        .on_conflict_do_update(
            index_elements=["cache_key"],
            set_={
                "normalized_filters": normalized_filters,
                "raw_input": raw_input,
                "created_at": now,
                "expires_at": expires_at,
            },
        )
        .returning(FilterMappingCache)
    )
    result = await session.execute(stmt)
    await session.flush()
    return result.scalar_one()
