"""
Ollama LLM enrichment logic.

Provides call_ollama_enrich(), which sends a news article to the local Ollama
instance and returns a validated EnrichResponse.  Simple exponential-backoff
retry is applied on transient connection / timeout errors.
"""

from __future__ import annotations

import asyncio
import json
import re

import httpx

from enrichment_service.config import Settings
from enrichment_service.schemas import EnrichResponse

# ── Custom exception ───────────────────────────────────────────────────────────


class EnrichmentError(Exception):
    """
    Raised when the enrichment call cannot produce a validated EnrichResponse.

    Wraps parse failures, schema validation errors, and exhausted retries.
    """

    def __init__(self, message: str, cause: BaseException | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.cause = cause


# ── Prompts ────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are a precise semantic metadata extractor for news journalism.

## Task
You will receive one or more news articles about the same news event, separated
by "---".  The TITLE field belongs to the primary (most widely-reported) article.
Extract structured semantic metadata that synthesizes the full picture across all
provided articles and return it as a single, well-formed JSON object.  Your output
must be the JSON object only — no markdown code fences, no prose, no leading or
trailing whitespace outside the object.

## Output schema
Return exactly the following JSON structure (all keys required):

{
  "article_title": <string or null>,
  "article_summary": <string or null>,
  "cited_sources": [<string>, ...],
  "main_topics": [<string>, ...],
  "keywords": [<string>, ...],
  "entities": {
    "persons_cited": [<string>, ...],
    "organizations_cited": [<string>, ...],
    "locations": [<string>, ...],
    "ethnicities_cited": [<string>, ...],
    "religions_cited": [<string>, ...],
    "occupations_cited": [<string>, ...],
    "political_affiliations_cited": [<string>, ...],
    "industries_cited": [<string>, ...],
    "products_cited": [<string>, ...],
    "brands_cited": [<string>, ...]
  }
}

## Field definitions

### article_title
A canonical, cleaned headline that accurately represents the shared news event.
Prefer the supplied TITLE when it is accurate and clear; synthesize a better
one only if the supplied title is misleading, too narrow, or garbled.  Correct
minor OCR artefacts or encoding issues if necessary.  Return null if the content
is too short, garbled, or ambiguous to produce a meaningful title.

### article_summary
A neutral, factual synthesis of the main point(s) of the news event written in
2–4 complete sentences.  When multiple articles are provided, combine their
perspectives into a single coherent summary — highlight consensus facts and note
significant differences in framing only if they are editorially meaningful.  Do
not editorialize, draw conclusions not stated in any article, or copy large
verbatim passages.  Return null when the combined body text is too short
(fewer than ~50 words) or too incoherent to summarize meaningfully.

### cited_sources
A deduplicated list of news outlets, wire agencies, publications, broadcasters,
or named external reports that are explicitly referenced inside any of the
article bodies as the origin of a claim, quote, or piece of data.  Examples:
"Reuters", "BBC News", "The Wall Street Journal", "UN report".  Exclude outlets
that published the articles themselves unless cited as secondary sources.
Return [] if none.

### main_topics
Between 3 and 8 high-level, human-readable subject categories that describe
what the news event is fundamentally about, derived from all provided articles.
Use broad journalistic labels such as "international relations", "military
conflict", "economic policy", "public health", "climate change", "technology
regulation", "human rights", "electoral politics", "corporate finance",
"crime and justice".  Do not use proper nouns here.  Return [] if topics
cannot be determined.

### keywords
Between 5 and 15 specific, significant terms or short phrases that are central
to the news event, drawn from all provided articles.  These should be concrete
and distinctive: proper nouns, technical terms, named legislation, named
operations, product names, treaty names, etc.  Avoid generic words like
"government", "said", or "report".  Return [] if the content is insufficient.

### entities.persons_cited
Full names (first + last where available) of individual people mentioned in any
of the articles, including those quoted, referenced, or described.  Deduplicate
across articles.  Return [] if none.

### entities.organizations_cited
Names of companies, NGOs, international bodies, government agencies,
inter-governmental organizations, political parties, armed groups, or other
formal institutions mentioned in any article.  Deduplicate.  Return [] if none.

### entities.locations
Cities, metropolitan areas, regions, countries, continents, bodies of water,
geographic features, or named zones mentioned in any article.  Normalize to the
most common English form (e.g. "Ukraine" not "THE UKRAINE").  Deduplicate.
Return [] if none.

### entities.ethnicities_cited
Ethnic, racial, or demographic group labels mentioned in any article (e.g.
"Uyghurs", "Rohingya", "Hispanic Americans").  Only include groups explicitly
named in the text.  Deduplicate.  Return [] if none.

### entities.religions_cited
Religions, religious denominations, sects, or religious communities mentioned
in any article (e.g. "Islam", "Catholic Church", "Evangelical Christianity",
"Shia Muslims").  Deduplicate.  Return [] if none.

### entities.occupations_cited
Job titles, roles, or professional designations mentioned in any article (e.g.
"prime minister", "central bank governor", "whistleblower", "surgeon general",
"journalist").  Normalise to lowercase.  Deduplicate.  Return [] if none.

### entities.political_affiliations_cited
Political parties, coalitions, movements, or named ideological currents
mentioned in any article (e.g. "Republican Party", "Labour Party", "MAGA
movement", "far-right", "Green New Deal coalition").  Deduplicate.
Return [] if none.

### entities.industries_cited
Economic sectors or industries referenced in any article (e.g. "energy",
"banking", "semiconductor", "defense", "pharmaceutical", "agriculture").
Deduplicate.  Return [] if none.

### entities.products_cited
Specific products, commodities, weapons systems, software packages, or
technologies mentioned by name in any article (e.g. "F-35", "ChatGPT",
"Nord Stream pipeline", "mRNA vaccine").  Deduplicate.  Return [] if none.

### entities.brands_cited
Brand names or registered trademarks mentioned in any article (e.g. "Boeing",
"Google", "Pfizer", "OPEC+").  Do not duplicate entries already in
organizations_cited unless the brand context is distinct.  Return [] if none.

## Rules
- Return ONLY the JSON object.  No markdown, no explanation, no extra keys.
- All values must be derived exclusively from the supplied article text(s).
  Do not invent, infer, or hallucinate content not present in any article.
- If a list field has no relevant items, return an empty list [].
- String values must not be empty strings; omit the item rather than include "".
- Deduplication: do not repeat the same string within a single list.
- When articles contradict each other, reflect the most corroborated claim or
  note both perspectives briefly in article_summary only.
"""

USER_PROMPT_TEMPLATE = """\
TITLE: {title}

ARTICLES:
{content}
"""

# ── Helpers ────────────────────────────────────────────────────────────────────

_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.MULTILINE)


def _strip_markdown_fences(text: str) -> str:
    """Remove leading/trailing markdown code fences that some models emit."""
    return _FENCE_RE.sub("", text).strip()


def _build_messages(extracted_title: str | None, extracted_content: str) -> list[dict]:
    title_line = extracted_title if extracted_title else "(no title provided)"
    user_content = USER_PROMPT_TEMPLATE.format(
        title=title_line,
        content=extracted_content,
    )
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]


# ── Main entry-point ───────────────────────────────────────────────────────────


async def call_ollama_enrich(
    extracted_title: str | None,
    extracted_content: str,
    *,
    http_client: httpx.AsyncClient,
    settings: Settings,
) -> EnrichResponse:
    """
    Call the local Ollama instance and return a validated EnrichResponse.

    Retries up to settings.ollama_max_retries times on transient network errors
    (connection refused, timeout) using simple exponential backoff.

    Raises:
        EnrichmentError: on exhausted retries, JSON parse failure, or schema
                         validation failure.
    """
    url = settings.ollama_base_url.rstrip("/") + "/api/chat"
    payload = {
        "model": settings.ollama_model,
        "messages": _build_messages(extracted_title, extracted_content),
        "format": "json",
        "stream": False,
    }
    timeout = settings.ollama_timeout_seconds
    max_retries = settings.ollama_max_retries

    last_error: BaseException | None = None
    for attempt in range(max_retries + 1):
        if attempt > 0:
            backoff = 2.0 ** (attempt - 1)  # 1s, 2s, 4s …
            await asyncio.sleep(backoff)

        try:
            response = await http_client.post(url, json=payload, timeout=timeout)
            response.raise_for_status()
        except (httpx.ConnectError, httpx.TimeoutException) as exc:
            last_error = exc
            continue  # retry
        except httpx.HTTPStatusError as exc:
            raise EnrichmentError(
                f"Ollama returned HTTP {exc.response.status_code}: {exc.response.text[:200]}",
                cause=exc,
            ) from exc
        except httpx.RequestError as exc:
            raise EnrichmentError(
                f"Network error contacting Ollama: {exc}",
                cause=exc,
            ) from exc

        # ── Parse Ollama envelope ──────────────────────────────────────────
        try:
            envelope = response.json()
        except ValueError as exc:
            raise EnrichmentError(
                "Failed to parse Ollama HTTP response as JSON",
                cause=exc,
            ) from exc

        # Ollama /api/chat returns {"message": {"role": ..., "content": ...}, ...}
        try:
            raw_content: str = envelope["message"]["content"]
        except (KeyError, TypeError) as exc:
            raise EnrichmentError(
                f"Unexpected Ollama response structure: {str(envelope)[:300]}",
                cause=exc,
            ) from exc

        # ── Strip fences and parse model JSON ─────────────────────────────
        cleaned = _strip_markdown_fences(raw_content)
        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            raise EnrichmentError(
                f"Model output is not valid JSON: {exc}  (raw: {cleaned[:300]})",
                cause=exc,
            ) from exc

        # ── Validate against EnrichResponse schema ────────────────────────
        try:
            return EnrichResponse.model_validate(parsed)
        except Exception as exc:  # pydantic ValidationError
            raise EnrichmentError(
                f"Model JSON does not match EnrichResponse schema: {exc}",
                cause=exc,
            ) from exc

    # All retry attempts exhausted
    raise EnrichmentError(
        f"Ollama unreachable after {max_retries + 1} attempt(s): {last_error}",
        cause=last_error,
    )
