"""
SQLAlchemy ORM models.

SyncState     — persists the result of each 15-minute GDELT metadata sync.
FilterMappingCache — caches Claude's filter normalization output by input hash.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, BigInteger, DateTime, Float, Index, Integer, String, Text
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
    sync_status: Mapped[str] = mapped_column(String(20), nullable=False, default="success")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)


class FilterMappingCache(Base):
    """
    Caches Claude's normalized filter response to avoid redundant API calls.

    Cache key: SHA256 of the canonical (sorted, lowercased) RawFilterInput JSON.
    TTL: controlled by expires_at. Expired entries are ignored and overwritten.
    """

    __tablename__ = "filter_mapping_cache"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))

    # SHA256 of canonical filter input JSON
    cache_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)

    # Original raw filter input stored for debugging
    raw_input: Mapped[dict] = mapped_column(JSON, nullable=False)

    # Claude's normalized output (NormalizedFilters schema as JSON)
    normalized_filters: Mapped[dict] = mapped_column(JSON, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class GdeltEvent(Base):
    """
    Stores individual GDELT event records for local querying and caching.

    Each row represents a single event from the GDELT 2.0 dataset.
    The primary key is GDELT's GLOBALEVENTID (not auto-incremented).
    """

    __tablename__ = "gdelt_events"
    __table_args__ = (
        Index("ix_gdelt_events_geo_date", "action_geo_country_code", "sql_date"),
        Index("ix_gdelt_events_event_date", "event_root_code", "sql_date"),
    )

    global_event_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    sql_date: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    date_added: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    actor1_country_code: Mapped[str | None] = mapped_column(String(3), nullable=True)
    actor2_country_code: Mapped[str | None] = mapped_column(String(3), nullable=True)
    event_code: Mapped[str | None] = mapped_column(String(4), nullable=True)
    event_base_code: Mapped[str | None] = mapped_column(String(3), nullable=True)
    event_root_code: Mapped[str | None] = mapped_column(String(2), nullable=True, index=True)
    quad_class: Mapped[int | None] = mapped_column(Integer, nullable=True)
    goldstein_scale: Mapped[float | None] = mapped_column(Float, nullable=True)
    avg_tone: Mapped[float | None] = mapped_column(Float, nullable=True)
    num_mentions: Mapped[int | None] = mapped_column(Integer, nullable=True)
    num_sources: Mapped[int | None] = mapped_column(Integer, nullable=True)
    num_articles: Mapped[int | None] = mapped_column(Integer, nullable=True)
    action_geo_full_name: Mapped[str | None] = mapped_column(String(500), nullable=True)
    action_geo_country_code: Mapped[str | None] = mapped_column(
        String(2), nullable=True, index=True
    )
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    article_title: Mapped[str | None] = mapped_column(Text, nullable=True)
    article_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    cited_sources: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    main_topics: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    keywords: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    entities: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    enrichment_status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    enriched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    enrichment_error: Mapped[str | None] = mapped_column(Text, nullable=True)


class IngestionState(Base):
    """
    Tracks the state of GDELT event ingestion runs.

    Each row represents a single ingestion job (bootstrap or incremental).
    Used to track progress, watermarks, and success/failure status.
    """

    __tablename__ = "ingestion_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ingestion_type: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="running")
    watermark_dateadded: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    events_ingested: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
