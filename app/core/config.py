"""
Application configuration via pydantic-settings.

Reads all settings from environment variables (or a .env file).
Fails fast at startup if any required variable is missing.
"""

from __future__ import annotations

import json
from functools import lru_cache

from pydantic import AnyHttpUrl, Field, TypeAdapter, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Anthropic ─────────────────────────────────────────────────────────
    anthropic_api_key: str = Field(..., description="Anthropic API key")
    anthropic_model: str = Field(default="claude-opus-4-5")
    anthropic_timeout_seconds: float = Field(default=30.0, ge=5.0, le=120.0)
    anthropic_max_retries: int = Field(default=3, ge=0, le=10)

    # ── PostgreSQL ────────────────────────────────────────────────────────
    database_url: str = Field(..., description="Async SQLAlchemy database URL")

    # ── App / Security ────────────────────────────────────────────────────
    api_key: str = Field(..., description="X-API-Key for endpoint authentication")
    cors_origins: list[str] = Field(default_factory=list)
    log_level: str = Field(default="INFO")
    app_env: str = Field(default="production")

    # ── Scheduler ────────────────────────────────────────────────────────
    enable_metadata_sync: bool = Field(default=True)
    sync_interval_minutes: int = Field(default=15, ge=1, le=60)

    # ── Event Store / Ingestion ──────────────────────────────────────────
    retention_days: int = Field(default=30, ge=1, le=365)
    ingestion_interval_minutes: int = Field(default=60, ge=5, le=1440)
    ingestion_batch_size: int = Field(default=10_000, ge=100, le=100_000)
    enable_event_enrichment: bool = Field(default=False)
    event_enrichment_interval_minutes: int = Field(default=30, ge=1, le=1440)
    event_enrichment_batch_size: int = Field(default=100, ge=1, le=10_000)
    enable_cluster_materialisation: bool = Field(default=True)
    cluster_interval_minutes: int = Field(default=1440, ge=1, le=1440)
    event_enrichment_service_base_url: AnyHttpUrl = Field(
        default_factory=lambda: TypeAdapter(AnyHttpUrl).validate_python("http://localhost:8001"),
    )
    event_enrichment_service_timeout_seconds: float = Field(default=10.0, ge=1.0, le=120.0)

    # ── Rate Limiting ─────────────────────────────────────────────────────
    rate_limit_per_minute: int = Field(default=10, ge=1, le=1000)

    # ── Cluster pipeline ──────────────────────────────────────────────────
    # Domains whose source URLs are excluded from cluster candidate scoring.
    # These are pure aggregators / content farms that produce no original
    # journalism: they copy feeds from wire services, inflating topic_score
    # and contaminating dominant_countries with unrelated geographies.
    # Override via CLUSTER_SOURCE_DOMAIN_BLOCKLIST env var as a JSON array:
    #   CLUSTER_SOURCE_DOMAIN_BLOCKLIST='["www.yahoo.com","www.aol.com"]'
    cluster_source_domain_blocklist: frozenset[str] = Field(
        default=frozenset(
            {
                "www.yahoo.com",
                "www.aol.com",
                "www.aol.co.uk",
                "www.dailymail.co.uk",
                "www.mirror.co.uk",
                "www.express.co.uk",
                "www.winnipegfreepress.com",
                "www.bignewsnetwork.com",
                "www.miragenews.com",
                "countercurrents.org",
                "www.globalsecurity.org",
            }
        ),
        description="Source URL domains excluded from cluster candidate scoring.",
    )

    @field_validator("cluster_source_domain_blocklist", mode="before")
    @classmethod
    def parse_domain_blocklist(cls, v: str | list | set | frozenset) -> frozenset[str]:
        if isinstance(v, (frozenset, set)):
            return frozenset(v)
        if isinstance(v, list):
            return frozenset(v)
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return frozenset(parsed)
            except (json.JSONDecodeError, ValueError):
                pass
            return frozenset(d.strip() for d in v.split(",") if d.strip())
        return frozenset()

    @field_validator("cors_origins", mode="before")
    @classmethod
    def parse_cors_origins(cls, v: str | list) -> list[str]:
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return parsed
            except (json.JSONDecodeError, ValueError):
                pass
            # Fallback: treat as comma-separated
            return [origin.strip() for origin in v.split(",") if origin.strip()]
        return []

    @property
    def is_development(self) -> bool:
        return self.app_env.lower() == "development"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached settings singleton. Fails fast if required vars are missing."""
    return Settings()
