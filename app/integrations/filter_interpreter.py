"""
Claude-based filter interpreter.

Builds a structured system prompt embedding the full CAMEO/FIPS reference
data, sends the user's raw filters to Claude, and validates the response
into a ClaudeFilterResponse Pydantic model.

Retry strategy: exponential backoff up to ANTHROPIC_MAX_RETRIES attempts
on transient errors (rate limit, timeout, server errors).
"""

from __future__ import annotations

import asyncio
import json

import anthropic
from pydantic import ValidationError

from app.core.config import get_settings
from app.core.exceptions import AnthropicUnavailableError, FilterInterpretationError
from app.core.logging import get_logger
from app.integrations.country_codes import (
    CAMEO_COUNTRY_CODES,
    CAMEO_ROOT_CODE_LABELS,
    FIPS_COUNTRY_CODES,
)
from app.schemas.filters import ClaudeFilterResponse, RawFilterInput

logger = get_logger(__name__)

# ── System prompt ─────────────────────────────────────────────────────────

_CAMEO_CODES_REFERENCE = "\n".join(
    f"  {code}: {label}" for code, label in CAMEO_ROOT_CODE_LABELS.items()
)

_CAMEO_COUNTRY_SAMPLE = "\n".join(
    f"  {name.title()}: {code}" for name, code in list(CAMEO_COUNTRY_CODES.items())[:30]
)

_FIPS_COUNTRY_SAMPLE = "\n".join(
    f"  {name.title()}: {code}" for name, code in list(FIPS_COUNTRY_CODES.items())[:30]
)

SYSTEM_PROMPT = f"""You are a GDELT 2.0 dataset filter normalization engine.

Your task: Convert user-provided filter values into precise GDELT query parameters.

## GDELT CAMEO Event Root Codes (EventRootCode field)
{_CAMEO_CODES_REFERENCE}

## CAMEO Country Codes (used for Actor1CountryCode / Actor2CountryCode)
These are 3-letter codes (similar to but NOT identical to ISO alpha-3):
{_CAMEO_COUNTRY_SAMPLE}
(More countries follow the same pattern — use your knowledge of CAMEO codes)

## FIPS 10-4 Country Codes (used for ActionGeo_CountryCode — geographic location)
These are 2-letter codes (NOT the same as ISO alpha-2):
{_FIPS_COUNTRY_SAMPLE}
Key differences from ISO alpha-2: China=CH (not CN), Germany=GM (not DE),
Russia=RS (not RU), Iraq=IZ (not IQ), United Kingdom=UK (not GB)

## Your Output Rules
1. Respond ONLY with a valid JSON object. No markdown fences, no explanation text.
2. The JSON must match this exact schema:
{{
  "cameo_country_code": "<CAMEO 3-letter code or null>",
  "fips_country_code": "<FIPS 2-letter code or null>",
  "event_root_codes": ["<2-digit string>", ...],
  "event_base_codes": ["<3-digit string>", ...],
  "date_from_sqldate": <YYYYMMDD integer>,
  "date_to_sqldate": <YYYYMMDD integer>,
  "normalization_notes": "<brief explanation of your mapping choices>"
}}
3. For event_root_codes: map the user's event type to ALL relevant root codes.
   - "protest" → ["14"]
   - "war" or "conflict" or "fighting" → ["18", "19", "20"]
   - "diplomacy" → ["04", "05", "06"]
   - "sanctions" or "pressure" → ["16", "17"]
   - "cooperation" → ["05", "06", "07"]
   - "threat" → ["13"]
   - Include adjacent codes when the user term is broad.
4. For event_base_codes: add 3-digit refinements only when the user is specific.
   Leave empty [] if the root code filter is sufficient.
5. For macro_topic: use your knowledge to select the most relevant event codes.
   - "energy" events → protests about energy policy: root ["14"], notes explain
   - Map topics to the CAMEO event types most likely to generate news about that topic.
6. date_from_sqldate: convert year to YYYYMMDD by appending 0101 (Jan 1)
7. date_to_sqldate: convert year to YYYYMMDD by appending 1231 (Dec 31)
8. If no date range is specified, use 20150101 to today's date.
9. If the country cannot be mapped, set both country codes to null.
10. normalization_notes must explain your choices in 1-3 sentences.
"""

USER_PROMPT_TEMPLATE = """Normalize these GDELT filters:

country: {country}
event_type: {event_type}
macro_topic: {macro_topic}
date_range_from_year: {date_from}
date_range_to_year: {date_to}

Return the JSON normalization result."""


# ── Main interpreter function ─────────────────────────────────────────────


async def interpret_filters(
    raw_filters: RawFilterInput,
    client: anthropic.AsyncAnthropic,
) -> ClaudeFilterResponse:
    """
    Send raw filters to Claude and return a validated ClaudeFilterResponse.

    Implements exponential backoff retry on transient errors.
    Raises FilterInterpretationError on invalid Claude response.
    Raises AnthropicUnavailableError after max retries.
    """
    settings = get_settings()
    user_prompt = _build_user_prompt(raw_filters)
    max_retries = settings.anthropic_max_retries

    last_error: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            logger.info(
                "anthropic_request_start",
                attempt=attempt + 1,
                max_attempts=max_retries + 1,
            )
            response = await client.messages.create(
                model=settings.anthropic_model,
                max_tokens=512,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_prompt}],
            )
            raw_text = response.content[0].text.strip()
            logger.info("anthropic_response_received", response_length=len(raw_text))
            return _parse_and_validate_response(raw_text, raw_filters)

        except (
            anthropic.RateLimitError,
            anthropic.APIStatusError,
            anthropic.APIConnectionError,
        ) as exc:
            last_error = exc
            if attempt < max_retries:
                wait_seconds = 2**attempt  # exponential backoff: 1s, 2s, 4s
                logger.warning(
                    "anthropic_transient_error_retry",
                    attempt=attempt + 1,
                    wait_seconds=wait_seconds,
                    error=str(exc),
                )
                await asyncio.sleep(wait_seconds)
            continue

        except anthropic.APITimeoutError as exc:
            last_error = exc
            if attempt < max_retries:
                wait_seconds = 2**attempt
                logger.warning(
                    "anthropic_timeout_retry",
                    attempt=attempt + 1,
                    wait_seconds=wait_seconds,
                )
                await asyncio.sleep(wait_seconds)
            continue

        except FilterInterpretationError:
            raise

        except Exception as exc:
            raise AnthropicUnavailableError(
                f"Unexpected error calling Anthropic API: {exc}",
                detail=str(exc),
            ) from exc

    raise AnthropicUnavailableError(
        f"Anthropic API unavailable after {max_retries + 1} attempts",
        detail=str(last_error),
    )


# ── Private helpers ───────────────────────────────────────────────────────


def _build_user_prompt(raw_filters: RawFilterInput) -> str:
    """Format the user prompt with the raw filter values."""
    date_from = date_to = "not specified"
    if raw_filters.date_range:
        date_from = str(raw_filters.date_range.from_year)
        date_to = str(raw_filters.date_range.to_year)

    return USER_PROMPT_TEMPLATE.format(
        country=raw_filters.country or "not specified",
        event_type=raw_filters.event_type or "not specified",
        macro_topic=raw_filters.macro_topic or "not specified",
        date_from=date_from,
        date_to=date_to,
    )


def _parse_and_validate_response(
    raw_text: str, raw_filters: RawFilterInput
) -> ClaudeFilterResponse:
    """
    Parse Claude's text response as JSON and validate against ClaudeFilterResponse.

    Raises FilterInterpretationError on any parse or validation failure.
    """
    # Strip any accidental markdown fences
    clean_text = raw_text.strip()
    if clean_text.startswith("```"):
        lines = clean_text.split("\n")
        clean_text = "\n".join(line for line in lines if not line.startswith("```")).strip()

    try:
        data = json.loads(clean_text)
    except json.JSONDecodeError as exc:
        raise FilterInterpretationError(
            f"Claude returned invalid JSON: {exc}",
            detail=f"Raw response: {raw_text[:500]}",
        ) from exc

    try:
        return ClaudeFilterResponse.model_validate(data)
    except ValidationError as exc:
        raise FilterInterpretationError(
            f"Claude response failed schema validation: {exc}",
            detail=f"Parsed data: {data}",
        ) from exc
