"""
SQLAlchemy ORM models.

SyncState          — persists metadata snapshots derived from the local GDELT event store.
FilterMappingCache — caches Claude's filter normalization output by input hash.
GdeltEvent         — locally cached GDELT 2.0 event records.
IngestionState     — tracks ingestion job runs (bootstrap / incremental).
GdeltMention       — GDELT EVENTMENTIONS rows (documents mentioning an event).
GdeltGkg           — GDELT GKG rows (semantic metadata per document URL).
StoryCluster       — materialised story clusters built from the three layers above.
"""

from __future__ import annotations

import uuid  # used in FilterMappingCache.id default
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Boolean, JSON, BigInteger, DateTime, Float, Index, Integer, String, Text
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


class GdeltMention(Base):
    """One GDELT EVENTMENTIONS row — a document that mentions a specific event.

    Surrogate integer PK because EVENTMENTIONS has no single-column natural key.
    Unique index on (global_event_id, mention_identifier) drives ON CONFLICT DO NOTHING.
    """

    __tablename__ = "gdelt_mentions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    global_event_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    event_time_date: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    mention_time_date: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    mention_type: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mention_source_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    mention_identifier: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    sent_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mention_doc_len: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mention_doc_tone: Mapped[float | None] = mapped_column(Float, nullable=True)

    __table_args__ = (
        Index("ix_gdelt_mentions_event_mention", "global_event_id", "mention_identifier"),
    )


class GdeltGkg(Base):
    """One GDELT GKG row — semantic metadata for a document URL.

    The GKG layer adds themes, persons, organisations, locations and tone
    for each document URL, independently of the events that reference it.
    """

    __tablename__ = "gdelt_gkg"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    gkg_record_id: Mapped[str | None] = mapped_column(String(50), nullable=True, index=True)
    date: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    source_common_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    document_identifier: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    themes: Mapped[list | None] = mapped_column(JSON, nullable=True)
    persons: Mapped[list | None] = mapped_column(JSON, nullable=True)
    organizations: Mapped[list | None] = mapped_column(JSON, nullable=True)
    locations: Mapped[list | None] = mapped_column(JSON, nullable=True)
    document_tone: Mapped[float | None] = mapped_column(Float, nullable=True)


class StoryCluster(Base):
    """Materialised story cluster — one row per source_url window, enriched with mentions and GKG.

    cluster_id is deterministic: "{YYYYMMDD}_{sha256(source_url)[:12]}".
    Unique constraint on cluster_id allows ON CONFLICT DO UPDATE (upsert on re-materialisation).
    """

    __tablename__ = "story_clusters"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cluster_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)

    # ── Scoring ────────────────────────────────────────────────────────────
    event_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    num_articles: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    num_mentions: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    num_sources: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    topic_score: Mapped[float | None] = mapped_column(Float, nullable=True)

    # ── Event layer enrichment ─────────────────────────────────────────────
    event_ids: Mapped[list | None] = mapped_column(JSON, nullable=True)
    dominant_event_types: Mapped[list | None] = mapped_column(JSON, nullable=True)
    dominant_quad_classes: Mapped[list | None] = mapped_column(JSON, nullable=True)
    avg_severity_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    dominant_countries: Mapped[list | None] = mapped_column(JSON, nullable=True)
    dominant_locations: Mapped[list | None] = mapped_column(JSON, nullable=True)

    # ── Mentions layer enrichment ──────────────────────────────────────────
    mention_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    distinct_mention_sources: Mapped[list | None] = mapped_column(JSON, nullable=True)
    mention_identifiers: Mapped[list | None] = mapped_column(JSON, nullable=True)
    first_mention_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_mention_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # ── GKG layer enrichment ───────────────────────────────────────────────
    themes: Mapped[list | None] = mapped_column(JSON, nullable=True)
    persons: Mapped[list | None] = mapped_column(JSON, nullable=True)
    organizations: Mapped[list | None] = mapped_column(JSON, nullable=True)
    gkg_locations: Mapped[list | None] = mapped_column(JSON, nullable=True)
    document_tone_avg: Mapped[float | None] = mapped_column(Float, nullable=True)

    # ── Event date range ───────────────────────────────────────────────────
    # Calendar span of the underlying GDELT events (sql_date YYYYMMDD integer).
    # Populated during cluster build; used by the time-proximity merge gate.
    event_date_ref_start: Mapped[int | None] = mapped_column(Integer, nullable=True)
    event_date_ref_end: Mapped[int | None] = mapped_column(Integer, nullable=True)

    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    # ── LLM enrichment ─────────────────────────────────────────────────────
    article_title: Mapped[str | None] = mapped_column(Text, nullable=True)
    article_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    cited_sources: Mapped[list | None] = mapped_column(JSON, nullable=True)
    main_topics: Mapped[list | None] = mapped_column(JSON, nullable=True)
    keywords: Mapped[list | None] = mapped_column(JSON, nullable=True)
    entities: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    enrichment_status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending"
    )
    enriched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    enrichment_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("ix_story_clusters_topic_score", "topic_score"),
        Index("ix_story_clusters_enrichment_status", "enrichment_status"),
    )


class RootCluster(Base):
    """Materialised root cluster for very large merged stories.

    Mirrors StoryCluster so the service and API can reuse the same payload shape,
    while keeping large clusters isolated in a dedicated table.
    """

    __tablename__ = "root_clusters"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cluster_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)

    # ── Scoring ────────────────────────────────────────────────────────────
    event_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    num_articles: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    num_mentions: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    num_sources: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    topic_score: Mapped[float | None] = mapped_column(Float, nullable=True)

    # ── Event layer enrichment ─────────────────────────────────────────────
    event_ids: Mapped[list | None] = mapped_column(JSON, nullable=True)
    dominant_event_types: Mapped[list | None] = mapped_column(JSON, nullable=True)
    dominant_quad_classes: Mapped[list | None] = mapped_column(JSON, nullable=True)
    avg_severity_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    dominant_countries: Mapped[list | None] = mapped_column(JSON, nullable=True)
    dominant_locations: Mapped[list | None] = mapped_column(JSON, nullable=True)

    # ── Mentions layer enrichment ──────────────────────────────────────────
    mention_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    distinct_mention_sources: Mapped[list | None] = mapped_column(JSON, nullable=True)
    mention_identifiers: Mapped[list | None] = mapped_column(JSON, nullable=True)
    first_mention_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_mention_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # ── GKG layer enrichment ───────────────────────────────────────────────
    themes: Mapped[list | None] = mapped_column(JSON, nullable=True)
    persons: Mapped[list | None] = mapped_column(JSON, nullable=True)
    organizations: Mapped[list | None] = mapped_column(JSON, nullable=True)
    gkg_locations: Mapped[list | None] = mapped_column(JSON, nullable=True)
    document_tone_avg: Mapped[float | None] = mapped_column(Float, nullable=True)

    # ── Event date range ───────────────────────────────────────────────────
    event_date_ref_start: Mapped[int | None] = mapped_column(Integer, nullable=True)
    event_date_ref_end: Mapped[int | None] = mapped_column(Integer, nullable=True)

    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    # ── LLM enrichment ─────────────────────────────────────────────────────
    article_title: Mapped[str | None] = mapped_column(Text, nullable=True)
    article_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    cited_sources: Mapped[list | None] = mapped_column(JSON, nullable=True)
    main_topics: Mapped[list | None] = mapped_column(JSON, nullable=True)
    keywords: Mapped[list | None] = mapped_column(JSON, nullable=True)
    entities: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    enrichment_status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending"
    )
    enriched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    enrichment_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("ix_root_clusters_topic_score", "topic_score"),
        Index("ix_root_clusters_enrichment_status", "enrichment_status"),
    )


class ClusterComponent(Base):
    """Persistent cluster component identity across cluster materialisation runs."""

    __tablename__ = "cluster_components"
    __table_args__ = (
        Index("ix_cluster_components_status", "status"),
        Index("ix_cluster_components_merged_into_component_id", "merged_into_component_id"),
        Index(
            "ix_cluster_components_current_table_cluster_id",
            "current_table",
            "current_cluster_id",
        ),
    )

    component_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    anchor_source_url: Mapped[str] = mapped_column(Text, nullable=False)
    component_source_urls: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    anchor_locked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    seed_event_ids: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    missing_run_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    merged_into_component_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    current_cluster_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    current_table: Mapped[str | None] = mapped_column(String(30), nullable=True)
    current_computed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    has_gkg: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    merge_evidence: Mapped[list[Any] | dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )


class ClusterComponentEvent(Base):
    """Event membership history for persistent cluster components."""

    __tablename__ = "cluster_component_events"
    __table_args__ = (
        Index("ix_cluster_component_events_component_id", "component_id"),
        Index("ix_cluster_component_events_event_id", "event_id"),
        Index(
            "uq_cluster_component_events_component_event",
            "component_id",
            "event_id",
            unique=True,
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    component_id: Mapped[str] = mapped_column(String(36), nullable=False)
    event_id: Mapped[str] = mapped_column(String(32), nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
