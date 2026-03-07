"""
SQLAlchemy ORM models.

SyncState     — persists the result of each 15-minute GDELT metadata sync.
FilterMappingCache — caches Claude's filter normalization output by input hash.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import JSON, BigInteger, DateTime, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class SyncState(Base):
    """
    Stores the outcome of the most recent (and historical) GDELT sync runs.

    One row is upserted per sync cycle. The latest row (ordered by synced_at)
    represents the current live state.
    """

    __tablename__ = "sync_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Timestamp of the most recent GDELT record seen (YYYYMMDDHHMMSS integer → UTC datetime)
    # latest_sqldate is YYYYMMDD (8 digits) → fits Integer
    # latest_dateadded is YYYYMMDDHHMMSS (14 digits) → requires BigInteger
    latest_sqldate: Mapped[int | None] = mapped_column(Integer, nullable=True)
    latest_dateadded: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Cached metadata from the last sync (JSON arrays)
    top_countries: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # [{"fips_code": "US", "event_count": 12345}, ...]

    top_event_root_codes: Mapped[list | None] = mapped_column(JSON, nullable=True)
    # [{"root_code": "14", "label": "PROTEST", "event_count": 4567}, ...]

    # ISO 8601 string identifying this mapping version
    mapping_version: Mapped[str] = mapped_column(String(50), nullable=False)

    # When this sync completed
    synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )

    # Whether the sync succeeded
    sync_status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="success"
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)


class FilterMappingCache(Base):
    """
    Caches Claude's normalized filter response to avoid redundant API calls.

    Cache key: SHA256 of the canonical (sorted, lowercased) RawFilterInput JSON.
    TTL: controlled by expires_at. Expired entries are ignored and overwritten.
    """

    __tablename__ = "filter_mapping_cache"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )

    # SHA256 of canonical filter input JSON
    cache_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)

    # Original raw filter input stored for debugging
    raw_input: Mapped[dict] = mapped_column(JSON, nullable=False)

    # Claude's normalized output (NormalizedFilters schema as JSON)
    normalized_filters: Mapped[dict] = mapped_column(JSON, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
