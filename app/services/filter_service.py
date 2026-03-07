"""
Filter normalization service.

Orchestrates the full filter pipeline:
  1. Guard clause: fail fast if no filters provided
  2. Check FilterMappingCache in PostgreSQL
  3. On cache miss: call Claude via filter_interpreter
  4. Validate Claude's response with Pydantic
  5. Write result to FilterMappingCache
  6. Return NormalizedFilters
"""

from __future__ import annotations

import anthropic
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import FilterInterpretationError
from app.core.logging import get_logger
from app.db.repositories.sync_repository import (
    compute_cache_key,
    get_cached_filter,
    upsert_cached_filter,
)
from app.integrations.filter_interpreter import interpret_filters
from app.schemas.filters import ClaudeFilterResponse, NormalizedFilters, RawFilterInput

logger = get_logger(__name__)


async def normalize_filters(
    raw_filters: RawFilterInput,
    session: AsyncSession,
    anthropic_client: anthropic.AsyncAnthropic,
) -> NormalizedFilters:
    """
    Normalize raw frontend filters into GDELT-compatible query parameters.

    Guard clause: raises FilterInterpretationError if no filter fields are set.
    Reads from cache before calling Claude; writes to cache after.
    """
    if not raw_filters.has_any_filter():
        raise FilterInterpretationError(
            "At least one filter field must be provided: "
            "country, event_type, macro_topic, or date_range",
        )

    canonical_dict = raw_filters.to_canonical_dict()
    cache_key = compute_cache_key(canonical_dict)

    # ── Cache read ─────────────────────────────────────────────────────────
    cached = await get_cached_filter(session, cache_key)
    if cached is not None:
        logger.info("filter_cache_hit", cache_key=cache_key[:8])
        try:
            claude_response = ClaudeFilterResponse.model_validate(cached.normalized_filters)
            return NormalizedFilters.from_claude_response(claude_response)
        except Exception:
            # Corrupted cache entry — fall through to Claude
            logger.warning("filter_cache_corrupted", cache_key=cache_key[:8])

    # ── Claude interpretation ──────────────────────────────────────────────
    logger.info("filter_cache_miss_calling_claude", cache_key=cache_key[:8])
    claude_response = await interpret_filters(raw_filters, anthropic_client)

    normalized = NormalizedFilters.from_claude_response(claude_response)

    # ── Cache write ────────────────────────────────────────────────────────
    try:
        await upsert_cached_filter(
            session,
            cache_key=cache_key,
            raw_input=canonical_dict,
            normalized_filters=claude_response.model_dump(),
            ttl_hours=24,
        )
    except Exception as exc:
        # Cache write failure is non-fatal — log and continue
        logger.warning("filter_cache_write_failed", error=str(exc))

    return normalized
