"""Orchestrates LLM enrichment for materialised story and root clusters.

Architecture (per paper.md):
  - The cluster is the primary editorial unit, not the individual event.
  - Each cluster already carries mention_identifiers (aggregated during
    materialisation) — these are the candidate URLs to fetch and enrich.
  - source_url on the cluster is used as a fallback when mention_identifiers
    is empty or all attempts fail.
  - Within a batch, results are cached by URL to avoid redundant fetch + LLM
    calls when multiple clusters reference the same document.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.logging import get_logger
from app.db.models import RootCluster, StoryCluster
from app.integrations.article_extractor import extract_article_content as extract_article_payload
from app.integrations.article_fetcher import fetch_article_html
from app.integrations.event_enrichment_client import enrich_article_content

logger = get_logger(__name__)

# ── Type aliases ───────────────────────────────────────────────────────────────

EnrichedPayload = dict[str, Any]
UrlCache = dict[str, Any]

_URL_MAX_RETRIES = 2
_STALE_PROCESSING_MINUTES = 15


# ── Public entry-point ────────────────────────────────────────────────────────


async def run_cluster_enrichment_batch(
    session: AsyncSession,
    *,
    batch_size: int,
    table: str = "story",
    date_from: int | None = None,
    date_to: int | None = None,
) -> dict[str, int]:
    """Enrich a batch of pending clusters using their mention_identifiers.

    Args:
        session:    Active async DB session.
        batch_size: Maximum number of clusters to process per call.
        table:      "story" targets story_clusters; "root" targets root_clusters.
        date_from:  Optional YYYYMMDD filter on event_date_ref_start (inclusive).
        date_to:    Optional YYYYMMDD filter on event_date_ref_start (inclusive).
    """
    model = StoryCluster if table == "story" else RootCluster

    recovered = await _reset_stale_processing(session, model)
    if recovered:
        logger.info("cluster_enrichment_stale_reset", table=table, recovered=recovered)

    candidates = await _get_pending_candidates(
        session, model, limit=batch_size, date_from=date_from, date_to=date_to
    )
    candidate_rows = [
        {
            "cluster_id": c.cluster_id,
            "mention_identifiers": c.mention_identifiers or [],
        }
        for c in candidates
    ]

    summary = {"selected": len(candidate_rows), "enriched": 0, "failed": 0, "skipped": 0}

    logger.info(
        "cluster_enrichment_batch_started",
        table=table,
        selected=len(candidate_rows),
    )

    url_cache: UrlCache = {}

    for i, row in enumerate(candidate_rows):
        cluster_id: str = row["cluster_id"]
        urls_to_try: list[str] = list(row["mention_identifiers"])

        logger.info(
            "cluster_enrichment_candidate",
            progress=f"{i + 1}/{len(candidate_rows)}",
            cluster_id=cluster_id,
            urls_to_try=len(urls_to_try),
            cache_size=len(url_cache),
        )

        try:
            if not urls_to_try:
                logger.warning("cluster_enrichment_no_urls", cluster_id=cluster_id)
                await _mark_failed(
                    session, model, cluster_id,
                    error="no fetchable URL (no mention_identifiers, no source_url)",
                )
                summary["failed"] += 1
                continue

            claimed = await _mark_processing(session, model, cluster_id)
            if not claimed:
                await session.rollback()
                logger.warning("cluster_enrichment_claim_failed", cluster_id=cluster_id)
                summary["skipped"] += 1
                continue

            payload = await _enrich_from_url_list(
                urls_to_try, url_cache, max_articles=get_settings().cluster_enrichment_max_articles
            )

            await _mark_succeeded(session, model, cluster_id, payload)
            summary["enriched"] += 1
            logger.info(
                "cluster_enrichment_succeeded",
                cluster_id=cluster_id,
                article_title=payload.get("article_title"),
            )

        except Exception as exc:
            error_msg = _stringify(exc)
            logger.warning(
                "cluster_enrichment_row_failed",
                cluster_id=cluster_id,
                error=error_msg,
            )
            await session.rollback()
            await _mark_failed(session, model, cluster_id, error=error_msg)
            summary["failed"] += 1
            continue

    logger.info("cluster_enrichment_batch_completed", table=table, **summary)
    return summary


# ── DB helpers ────────────────────────────────────────────────────────────────


async def _reset_stale_processing(
    session: AsyncSession,
    model: type[StoryCluster | RootCluster],
) -> int:
    """Reset clusters stuck in 'processing' back to 'pending'.

    A cluster is considered stale when it has been in 'processing' for longer
    than _STALE_PROCESSING_MINUTES — meaning the previous job was interrupted
    before it could complete or record a failure.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=_STALE_PROCESSING_MINUTES)
    result = await session.execute(
        update(model)
        .where(
            model.enrichment_status == "processing",
            model.enriched_at == None,  # noqa: E711 — SQLAlchemy requires == None
            model.computed_at < cutoff,
        )
        .values(enrichment_status="pending", enrichment_error=None)
        .returning(model.cluster_id)
    )
    await session.commit()
    rows = result.fetchall()
    return len(rows)


async def _get_pending_candidates(
    session: AsyncSession,
    model: type[StoryCluster | RootCluster],
    *,
    limit: int,
    date_from: int | None = None,
    date_to: int | None = None,
) -> list[StoryCluster | RootCluster]:
    q = select(model).where(model.enrichment_status == "pending")
    if date_from is not None:
        q = q.where(model.event_date_ref_start >= date_from)
    if date_to is not None:
        q = q.where(model.event_date_ref_start <= date_to)
    q = q.order_by(model.topic_score.desc().nulls_last()).limit(limit)
    result = await session.execute(q)
    return list(result.scalars().all())


async def _mark_processing(
    session: AsyncSession,
    model: type[StoryCluster | RootCluster],
    cluster_id: str,
) -> bool:
    result = await session.execute(
        update(model)
        .where(model.cluster_id == cluster_id, model.enrichment_status == "pending")
        .values(enrichment_status="processing", enrichment_error=None)
        .returning(model.cluster_id)
    )
    await session.flush()
    return result.first() is not None


async def _mark_succeeded(
    session: AsyncSession,
    model: type[StoryCluster | RootCluster],
    cluster_id: str,
    payload: EnrichedPayload,
) -> None:
    await session.execute(
        update(model)
        .where(model.cluster_id == cluster_id)
        .values(
            enrichment_status="success",
            enrichment_error=None,
            enriched_at=datetime.now(timezone.utc),
            article_title=payload.get("article_title"),
            article_summary=payload.get("article_summary"),
            cited_sources=payload.get("cited_sources"),
            main_topics=payload.get("main_topics"),
            keywords=payload.get("keywords"),
            entities=payload.get("entities"),
        )
    )
    await session.commit()


async def _mark_failed(
    session: AsyncSession,
    model: type[StoryCluster | RootCluster],
    cluster_id: str,
    *,
    error: str,
) -> None:
    try:
        await session.execute(
            update(model)
            .where(model.cluster_id == cluster_id)
            .values(enrichment_status="failed", enrichment_error=error)
        )
        await session.commit()
    except Exception as exc:
        await session.rollback()
        logger.error(
            "cluster_enrichment_failure_persistence_failed",
            cluster_id=cluster_id,
            persistence_error=_stringify(exc),
        )


# ── Enrichment logic ──────────────────────────────────────────────────────────


async def _enrich_from_url_list(
    urls: list[str], cache: UrlCache, max_articles: int = 3
) -> EnrichedPayload:
    """Fetch up to max_articles diverse articles and enrich their combined content.

    URLs are expected to be pre-ranked by corroboration frequency (most-mentioned
    first). At most one article per unique domain is collected to ensure source
    diversity. All successfully fetched extracted payloads are cached per-URL to
    avoid redundant HTTP requests within the same batch.
    """
    collected: list[dict[str, str]] = []
    seen_domains: set[str] = set()
    last_exc: Exception | None = None

    for url in urls:
        if len(collected) >= max_articles:
            break

        domain = urlparse(url).netloc
        if domain in seen_domains:
            continue

        cached = cache.get(url)
        if isinstance(cached, Exception):
            last_exc = cached
            continue
        if isinstance(cached, dict) and "content" in cached:
            logger.info("cluster_enrichment_url_cache_hit", url=url)
            collected.append(cached)
            seen_domains.add(domain)
            continue

        for attempt in range(1, _URL_MAX_RETRIES + 1):
            try:
                logger.info("cluster_enrichment_fetching", url=url, attempt=attempt)
                fetched = await fetch_article_html(url)
                extracted = extract_article_payload(fetched["html"])
                logger.info(
                    "cluster_enrichment_fetch_ok",
                    url=url,
                    content_length=len(extracted.get("content", "")),
                )
                cache[url] = extracted
                collected.append(extracted)
                seen_domains.add(domain)
                break
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "cluster_enrichment_url_attempt_failed",
                    url=url,
                    attempt=attempt,
                    max_retries=_URL_MAX_RETRIES,
                    error=_stringify(exc),
                )
        else:
            cache[url] = last_exc  # type: ignore[assignment]
            logger.warning("cluster_enrichment_url_failed", url=url, error=_stringify(last_exc))

    if not collected:
        raise last_exc or RuntimeError("all candidate URLs exhausted without a recorded error")

    combined_title = collected[0].get("title") or ""
    combined_content = "\n\n---\n\n".join(a["content"] for a in collected if a.get("content"))

    logger.info(
        "cluster_enrichment_calling_llm",
        articles_combined=len(collected),
        domains=sorted(seen_domains),
    )
    enriched = await enrich_article_content({"title": combined_title, "content": combined_content})
    logger.info("cluster_enrichment_llm_ok", articles_combined=len(collected))

    return {
        "article_title": enriched.article_title,
        "article_summary": enriched.article_summary,
        "cited_sources": enriched.cited_sources,
        "main_topics": enriched.main_topics,
        "keywords": enriched.keywords,
        "entities": enriched.entities.model_dump(),
    }


def _stringify(exc: Exception) -> str:
    msg = str(exc).strip()
    return msg or exc.__class__.__name__
