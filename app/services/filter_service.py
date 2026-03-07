"""
Filter normalization service.

Orchestrates the full filter pipeline:
  1. Guard clause: fail fast if no filters provided
  2. Normalize structured filters directly (sentiment, impact, actors, source)
  3. Check FilterMappingCache in PostgreSQL for free-text filters
  4. On cache miss: call Claude via filter_interpreter
  5. Merge Claude output with structured filters
  6. Write Claude result to FilterMappingCache
  7. Return fully normalized filters
"""

from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import urlparse

import anthropic
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import FilterInterpretationError
from app.core.logging import get_logger
from app.db.repositories.sync_repository import (
    compute_cache_key,
    get_cached_filter,
    upsert_cached_filter,
)
from app.integrations.country_codes import CAMEO_COUNTRY_CODES, FIPS_COUNTRY_CODES
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

    Free-text filters use Claude for semantic mapping. Structured filters are
    normalized locally and merged into the final NormalizedFilters object.
    """
    if not raw_filters.has_any_filter():
        raise FilterInterpretationError(
            "At least one filter field must be provided: country, countries, event_type, "
            "macro_topic, date_range, sentiment, impact, actors, source, event_codes, "
            "or quad_classes",
        )

    if raw_filters.has_free_text_filters():
        normalized = await _normalize_with_claude(raw_filters, session, anthropic_client)
    else:
        normalized = _build_structured_only_filters(raw_filters)

    return _merge_structured_filters(normalized, raw_filters)


async def _normalize_with_claude(
    raw_filters: RawFilterInput,
    session: AsyncSession,
    anthropic_client: anthropic.AsyncAnthropic,
) -> NormalizedFilters:
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
        logger.warning("filter_cache_write_failed", error=str(exc))

    return normalized


def _build_structured_only_filters(raw_filters: RawFilterInput) -> NormalizedFilters:
    """Build a normalized filter set without calling Claude."""
    date_from_sqldate, date_to_sqldate = _resolve_date_range(raw_filters)
    notes = "Structured filters applied without free-text normalization"
    return NormalizedFilters(
        date_from_sqldate=date_from_sqldate,
        date_to_sqldate=date_to_sqldate,
        normalization_notes=notes,
    )


def _merge_structured_filters(
    normalized: NormalizedFilters,
    raw_filters: RawFilterInput,
) -> NormalizedFilters:
    """Merge direct UI filters into the normalized Claude output."""
    date_from_sqldate, date_to_sqldate = _resolve_date_range(raw_filters, normalized)

    geo_country_codes = _merge_unique(
        [normalized.fips_country_code] if normalized.fips_country_code else [],
        [_normalize_geo_country_code(country) for country in raw_filters.countries],
    )

    actor1_country_code = _normalize_actor_country_code(raw_filters.actors.actor1_country) if raw_filters.actors else None
    actor2_country_code = _normalize_actor_country_code(raw_filters.actors.actor2_country) if raw_filters.actors else None

    return normalized.model_copy(
        update={
            "date_from_sqldate": date_from_sqldate,
            "date_to_sqldate": date_to_sqldate,
            "geo_country_codes": geo_country_codes,
            "actor1_country_code": actor1_country_code,
            "actor2_country_code": actor2_country_code,
            "event_root_codes": _merge_unique(
                normalized.event_root_codes,
                raw_filters.event_codes.root_codes if raw_filters.event_codes else [],
            ),
            "event_base_codes": _merge_unique(
                normalized.event_base_codes,
                raw_filters.event_codes.base_codes if raw_filters.event_codes else [],
            ),
            "event_codes": _normalize_codes(
                raw_filters.event_codes.full_codes if raw_filters.event_codes else []
            ),
            "quad_classes": sorted(set(raw_filters.quad_classes)),
            "source_domains": _normalize_domains(raw_filters.source.domains if raw_filters.source else []),
            "tone_min": raw_filters.sentiment.tone_min if raw_filters.sentiment else None,
            "tone_max": raw_filters.sentiment.tone_max if raw_filters.sentiment else None,
            "goldstein_min": raw_filters.sentiment.goldstein_min if raw_filters.sentiment else None,
            "goldstein_max": raw_filters.sentiment.goldstein_max if raw_filters.sentiment else None,
            "min_mentions": raw_filters.impact.min_mentions if raw_filters.impact else None,
            "min_sources": raw_filters.impact.min_sources if raw_filters.impact else None,
            "min_articles": raw_filters.impact.min_articles if raw_filters.impact else None,
        }
    )


def _resolve_date_range(
    raw_filters: RawFilterInput,
    existing: NormalizedFilters | None = None,
) -> tuple[int, int]:
    """Resolve YYYYMMDD range from raw filters or existing normalized values."""
    if raw_filters.date_range is not None:
        return (
            int(f"{raw_filters.date_range.from_year}0101"),
            int(f"{raw_filters.date_range.to_year}1231"),
        )

    if existing is not None:
        return existing.date_from_sqldate, existing.date_to_sqldate

    return 20150101, int(datetime.now(timezone.utc).strftime("%Y%m%d"))


def _normalize_geo_country_code(country: str) -> str | None:
    if not country:
        return None
    cleaned = country.strip().lower()
    if not cleaned:
        return None

    if len(cleaned) == 2 and cleaned.upper() in FIPS_COUNTRY_CODES.values():
        return cleaned.upper()
    return FIPS_COUNTRY_CODES.get(cleaned)


def _normalize_actor_country_code(country: str | None) -> str | None:
    if not country:
        return None
    cleaned = country.strip().lower()
    if not cleaned:
        return None

    if len(cleaned) == 3 and cleaned.upper() in CAMEO_COUNTRY_CODES.values():
        return cleaned.upper()
    return CAMEO_COUNTRY_CODES.get(cleaned)


def _normalize_domains(domains: list[str]) -> list[str]:
    normalized_domains: list[str] = []
    for domain in domains:
        cleaned = domain.strip().lower()
        if not cleaned:
            continue
        if "://" in cleaned:
            parsed = urlparse(cleaned)
            cleaned = parsed.netloc or cleaned
        normalized_domains.append(cleaned.removeprefix("www."))
    return sorted(set(normalized_domains))


def _normalize_codes(codes: list[str]) -> list[str]:
    return sorted({code.strip() for code in codes if code and code.strip()})


def _merge_unique(base: list[str], extra: list[str | None]) -> list[str]:
    merged = [value for value in base if value]
    merged.extend(value for value in extra if value)
    return sorted(set(merged))
