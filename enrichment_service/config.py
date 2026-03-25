"""
Configuration for the Event Enrichment Service.

All settings can be overridden via environment variables (pydantic-settings).
Call get_settings() to obtain a cached singleton.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Runtime configuration for the enrichment service."""

    # ── Ollama ─────────────────────────────────────────────────────────────
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "qwen2.5:14b"
    # LLMs can be slow; allow a generous timeout before giving up.
    ollama_timeout_seconds: float = 120.0
    ollama_max_retries: int = 2

    # ── Observability ──────────────────────────────────────────────────────
    log_level: str = "INFO"

    # ── Environment ────────────────────────────────────────────────────────
    app_env: str = "production"

    @property
    def is_development(self) -> bool:
        """True when running in a local development environment."""
        return self.app_env == "development"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached Settings singleton."""
    return Settings()
