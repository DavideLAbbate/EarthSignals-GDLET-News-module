"""Orchestrates event enrichment state transitions."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import TypedDict

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.db.repositories import event_repository
from app.db.repositories.mentions_repository import MentionsRepository
from app.integrations.article_extractor import extract_article_content as extract_article_payload
from app.integrations.article_fetcher import fetch_article_html
from app.integrations.event_enrichment_client import enrich_article_content

logger = get_logger(__name__)


class ExtractedArticleContent(TypedDict):
    """Deterministic article extraction payload for Phase 3."""

    title: str | None
    content: str


class EnrichedArticleContent(TypedDict):
    """Semantic enrichment payload returned by the internal service client."""

    article_title: str | None
    article_summary: str | None
    cited_sources: list[str]
    main_topics: list[str]
    keywords: list[str]
    entities: dict[str, list[str]]


class SuccessUpdatePayload(TypedDict):
    """Typed repository kwargs for a successful enrichment update."""

    article_title: str | None
    article_summary: str | None
    cited_sources: list[str]
    main_topics: list[str]
    keywords: list[str]
    entities: dict[str, list[str]]
    enriched_at: datetime


def _build_success_payload(
    enriched_article: EnrichedArticleContent,
) -> SuccessUpdatePayload:
    """Return repository kwargs for a successful enrichment update."""
    return {
        "article_title": enriched_article["article_title"],
        "article_summary": enriched_article["article_summary"],
        "cited_sources": enriched_article["cited_sources"],
        "main_topics": enriched_article["main_topics"],
        "keywords": enriched_article["keywords"],
        "entities": enriched_article["entities"],
        "enriched_at": _now_utc(),
    }


async def run_event_enrichment_batch(
    session: AsyncSession,
    *,
    batch_size: int,
) -> dict[str, int]:
    """Process a deterministic batch of pending events independently.

    Per-event URL resolution follows the paper's architecture:
      1. mention_identifiers from gdelt_mentions are the primary document unit;
      2. source_url from gdelt_events is appended as a fallback only when not
         already present in the mention list.

    Within a batch, results are cached by URL so that multiple events sharing
    the same mention_identifier trigger only one fetch + LLM call.
    """
    candidates = await event_repository.get_pending_enrichment_candidates(session, limit=batch_size)

    # Extract all needed data from ORM objects immediately, before any further
    # DB operations expire the session state and trigger sync lazy-loads.
    candidate_rows = [
        {
            "global_event_id": event.global_event_id,
            "source_url": event.source_url,
        }
        for event in candidates
    ]

    # Batch-load mentions for all candidate events in one round-trip.
    event_ids = [row["global_event_id"] for row in candidate_rows]
    mentions_repo = MentionsRepository(session)
    all_mentions = await mentions_repo.get_by_event_ids(event_ids)

    # Build event_id → ordered list of mention_identifier URLs.
    event_mention_urls: dict[int, list[str]] = defaultdict(list)
    for mention in all_mentions:
        if mention.mention_identifier and mention.mention_identifier.strip():
            event_mention_urls[mention.global_event_id].append(mention.mention_identifier)

    summary = {"selected": len(candidate_rows), "enriched": 0, "failed": 0, "skipped": 0}

    logger.info(
        "event_enrichment_batch_started",
        selected=len(candidate_rows),
        unique_events_with_mentions=len(event_mention_urls),
        total_mention_urls=sum(len(v) for v in event_mention_urls.values()),
    )

    # mention_identifier (or source_url) → EnrichedArticleContent | Exception
    _url_cache: dict[str, EnrichedArticleContent | Exception] = {}

    for i, candidate in enumerate(candidate_rows):
        global_event_id = candidate["global_event_id"]
        source_url = candidate["source_url"]

        # mention_identifiers are the primary unit; source_url is a fallback.
        urls_to_try: list[str] = list(event_mention_urls.get(global_event_id, []))
        if _has_source_url(source_url) and source_url not in urls_to_try:
            urls_to_try.append(source_url)  # type: ignore[arg-type]

        logger.info(
            "event_enrichment_candidate",
            progress=f"{i + 1}/{len(candidate_rows)}",
            global_event_id=global_event_id,
            urls_to_try=len(urls_to_try),
            cache_size=len(_url_cache),
        )

        try:
            if not urls_to_try:
                logger.warning("event_enrichment_no_urls", global_event_id=global_event_id)
                persisted_failed_status = await _persist_failed_status(
                    session,
                    global_event_id,
                    error_message="no fetchable URL (no mention_identifiers, no source_url)",
                )
                summary[_failure_summary_bucket(persisted_failed_status)] += 1
                continue

            claimed = await event_repository.mark_event_enrichment_processing(
                session,
                global_event_id,
            )
            if not claimed:
                await session.rollback()
                logger.warning("event_enrichment_claim_failed", global_event_id=global_event_id)
                summary["skipped"] += 1
                continue

            enriched_article = await _enrich_from_url_list(urls_to_try, _url_cache)

            updated = await _persist_successful_enrichment(
                session,
                global_event_id,
                enriched_article,
            )
            if not updated:
                raise RuntimeError("success update returned no rows")

            await _commit_transaction(session)
            summary["enriched"] += 1
            logger.info(
                "event_enrichment_succeeded",
                global_event_id=global_event_id,
                article_title=enriched_article.get("article_title"),
            )

        except Exception as exc:
            logger.warning(
                "event_enrichment_row_failed",
                global_event_id=global_event_id,
                error=_stringify_error(exc),
            )
            persisted_failed_status = await _record_row_failure(
                session,
                global_event_id,
                error_message=_stringify_error(exc),
            )
            summary[_failure_summary_bucket(persisted_failed_status)] += 1
            continue

    logger.info("event_enrichment_batch_completed", **summary)
    return summary


async def _enrich_from_url_list(
    urls: list[str],
    cache: dict[str, EnrichedArticleContent | Exception],
) -> EnrichedArticleContent:
    """Try each URL in order, returning the first successful enrichment.

    Results (both success and failure) are stored in cache so subsequent events
    sharing the same mention_identifier avoid redundant fetch + LLM calls.
    Raises the last encountered exception when all URLs are exhausted.
    """
    last_exc: Exception | None = None

    for url in urls:
        cached = cache.get(url)

        if isinstance(cached, Exception):
            last_exc = cached
            continue  # already known to fail — skip without retrying

        if cached is not None:
            logger.info("event_enrichment_url_cache_hit", url=url)
            return cached

        try:
            logger.info("event_enrichment_fetching", url=url)
            extracted = await _extract_article_content(url)
            logger.info(
                "event_enrichment_fetch_ok",
                url=url,
                content_length=len(extracted.get("content", "")),
            )
            logger.info("event_enrichment_calling_llm", url=url)
            enriched = await _enrich_article_content(extracted)
            logger.info("event_enrichment_llm_ok", url=url)
            cache[url] = enriched
            return enriched
        except Exception as exc:
            logger.warning("event_enrichment_url_failed", url=url, error=_stringify_error(exc))
            cache[url] = exc
            last_exc = exc
            continue

    raise last_exc or RuntimeError("all candidate URLs failed without a recorded error")


async def _commit_transaction(session: AsyncSession) -> None:
    """Commit the current transaction."""
    await session.commit()


async def _record_row_failure(
    session: AsyncSession,
    global_event_id: int,
    *,
    error_message: str,
) -> bool:
    """Rollback failed work and best-effort persist a failed status without leaving rows stuck."""
    await session.rollback()

    return await _persist_failed_status(
        session,
        global_event_id,
        error_message=error_message,
    )


async def _persist_successful_enrichment(
    session: AsyncSession,
    global_event_id: int,
    enriched_article: EnrichedArticleContent,
) -> bool:
    """Persist a successful enrichment result for a claimed row."""
    success_payload = _build_success_payload(enriched_article)
    return await event_repository.mark_event_enrichment_succeeded(
        session,
        global_event_id,
        article_title=success_payload["article_title"],
        article_summary=success_payload["article_summary"],
        cited_sources=success_payload["cited_sources"],
        main_topics=success_payload["main_topics"],
        keywords=success_payload["keywords"],
        entities=success_payload["entities"],
        enriched_at=success_payload["enriched_at"],
    )


async def _persist_failed_status(
    session: AsyncSession,
    global_event_id: int,
    *,
    error_message: str,
) -> bool:
    """Best-effort persist a failed status and log when the update is a no-op."""

    try:
        updated = await event_repository.mark_event_enrichment_failed(
            session,
            global_event_id,
            error_message=error_message,
        )
        if not updated:
            raise RuntimeError("failure update returned no rows")
        await _commit_transaction(session)
        return True
    except Exception as exc:
        await session.rollback()
        logger.error(
            "event_enrichment_failure_persistence_failed",
            global_event_id=global_event_id,
            error_message=error_message,
            persistence_error=_stringify_error(exc),
        )
        return False


def _failure_summary_bucket(persisted_failed_status: bool) -> str:
    """Return the summary bucket for a row-level failure outcome."""
    if persisted_failed_status:
        return "failed"

    return "skipped"


def _has_source_url(source_url: str | None) -> bool:
    """Return True when a source URL is present and non-empty."""
    return bool(source_url and source_url.strip())


def _now_utc() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc)


def _stringify_error(exc: Exception) -> str:
    """Normalize an exception into a concise persisted error message."""
    message = str(exc).strip()
    return message or exc.__class__.__name__


async def _extract_article_content(source_url: str) -> ExtractedArticleContent:
    """Fetch article HTML and extract deterministic Phase 3 content."""
    fetched_article = await fetch_article_html(source_url)
    return extract_article_payload(fetched_article["html"])


async def _enrich_article_content(
    article: ExtractedArticleContent,
) -> EnrichedArticleContent:
    """Return semantic enrichment fields from the internal enrichment service."""
    enriched_article = await enrich_article_content(article)
    return {
        "article_title": enriched_article.article_title,
        "article_summary": enriched_article.article_summary,
        "cited_sources": enriched_article.cited_sources,
        "main_topics": enriched_article.main_topics,
        "keywords": enriched_article.keywords,
        "entities": enriched_article.entities.model_dump(),
    }
