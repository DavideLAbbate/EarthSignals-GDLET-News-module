"""
Application configuration via pydantic-settings.

Reads all settings from environment variables (or a .env file).
Fails fast at startup if any required variable is missing.
"""

from __future__ import annotations

import json
from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Google Cloud / BigQuery ───────────────────────────────────────────
    google_application_credentials: str = Field(
        ...,
        description="Path to GCP service account JSON key file",
    )
    gcp_project_id: str = Field(
        ...,
        description="GCP project ID for billing attribution",
    )
    gcp_service_account_json: str | None = Field(
        default=None,
        description="Optional inline GCP service account JSON for hosted deployments",
    )
    bq_max_results: int = Field(default=500, ge=1, le=10_000)
    max_bq_scan_days: int = Field(default=3650, ge=1, le=36500)
    bq_executor_max_workers: int = Field(default=4, ge=1, le=32)

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
    sync_interval_minutes: int = Field(default=15, ge=1, le=60)

    # ── Event Store / Ingestion ──────────────────────────────────────────
    retention_days: int = Field(default=30, ge=1, le=365)
    ingestion_interval_minutes: int = Field(default=60, ge=5, le=1440)
    ingestion_batch_size: int = Field(default=10_000, ge=100, le=100_000)

    # ── Rate Limiting ─────────────────────────────────────────────────────
    rate_limit_per_minute: int = Field(default=10, ge=1, le=1000)

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
