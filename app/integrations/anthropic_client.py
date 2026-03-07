"""
Async Anthropic client wrapper.

Singleton stored in app.state. Configures timeout from settings.
All callers use the async interface (AsyncAnthropic).
"""

from __future__ import annotations

import anthropic

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)


def create_anthropic_client() -> anthropic.AsyncAnthropic:
    """
    Instantiate the async Anthropic client singleton.

    Reads ANTHROPIC_API_KEY and ANTHROPIC_TIMEOUT_SECONDS from settings.
    Fails fast at startup if the API key is missing.
    """
    settings = get_settings()

    client = anthropic.AsyncAnthropic(
        api_key=settings.anthropic_api_key,
        timeout=settings.anthropic_timeout_seconds,
        max_retries=0,  # We implement our own retry logic in filter_interpreter.py
    )
    logger.info(
        "anthropic_client_created",
        model=settings.anthropic_model,
        timeout=settings.anthropic_timeout_seconds,
    )
    return client
