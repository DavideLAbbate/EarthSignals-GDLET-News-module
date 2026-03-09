"""Orchestrates event enrichment state transitions."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TypedDict

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.db.repositories import event_repository
from app.integrations.article_extractor import extract_article_content as extract_article_payload
from app.integrations.event_enrichment_client import enrich_article_content
from app.integrations.article_fetcher import fetch_article_html

logger = get_logger(__name__)


class ExtractedArticleContent(TypedDict):
    """Deterministic article extraction payload for Phase 3."""

    title: str | None
    content: str


class EnrichedArticleContent(TypedDict):
    """Semantic enrichment payload returned by the internal service client."""

    article_title: str | None
    article_summary: str | None
    sources: list[str]


async def run_event_enrichment_batch(
    session: AsyncSession,
    *,
    batch_size: int,
) -> dict[str, int]:
    """Process a deterministic batch of pending events independently."""
    candidates = await event_repository.get_pending_enrichment_candidates(session, limit=batch_size)
    candidate_rows = [
        {
            "global_event_id": event.global_event_id,
            "source_url": event.source_url,
        }
        for event in candidates
    ]
    summary = {"selected": len(candidate_rows), "enriched": 0, "failed": 0, "skipped": 0}

    for candidate in candidate_rows:
        global_event_id = candidate["global_event_id"]
        source_url = candidate["source_url"]

        try:
            if not _has_source_url(source_url):
                await event_repository.mark_event_enrichment_failed(
                    session,
                    global_event_id,
                    error_message="missing source_url",
                )
                await _commit_transaction(session)
                summary["failed"] += 1
                continue

            claimed = await event_repository.mark_event_enrichment_processing(
                session,
                global_event_id,
            )
            if not claimed:
                await session.rollback()
                summary["skipped"] += 1
                continue

            if source_url is None:
                raise ValueError("missing source_url")

            extracted_article = await _extract_article_content(source_url)
            enriched_article = await _enrich_article_content(extracted_article)
            updated = await event_repository.mark_event_enrichment_succeeded(
                session,
                global_event_id,
                article_title=enriched_article["article_title"],
                article_summary=enriched_article["article_summary"],
                sources=enriched_article["sources"],
                enriched_at=_now_utc(),
            )
            if not updated:
                raise RuntimeError("success update returned no rows")

            await _commit_transaction(session)
            summary["enriched"] += 1
        except Exception as exc:
            await _record_row_failure(
                session,
                global_event_id,
                error_message=_stringify_error(exc),
            )
            summary["failed"] += 1
            continue

    return summary


async def _commit_transaction(session: AsyncSession) -> None:
    """Commit the current transaction."""
    await session.commit()


async def _record_row_failure(
    session: AsyncSession,
    global_event_id: int,
    *,
    error_message: str,
) -> None:
    """Rollback failed work and best-effort persist a failed status without leaving rows stuck."""
    await session.rollback()

    try:
        await event_repository.mark_event_enrichment_failed(
            session,
            global_event_id,
            error_message=error_message,
        )
        await _commit_transaction(session)
    except Exception as exc:
        await session.rollback()
        logger.error(
            "event_enrichment_failure_persistence_failed",
            global_event_id=global_event_id,
            error_message=error_message,
            persistence_error=_stringify_error(exc),
        )


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
        "sources": enriched_article.sources,
    }
