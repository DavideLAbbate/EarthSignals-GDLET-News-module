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

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.db.models import RootCluster, StoryCluster
from app.integrations.article_extractor import extract_article_content as extract_article_payload
from app.integrations.article_fetcher import fetch_article_html
from app.integrations.event_enrichment_client import enrich_article_content

logger = get_logger(__name__)

# ── Type aliases ───────────────────────────────────────────────────────────────

EnrichedPayload = dict[str, Any]
UrlCache = dict[str, Any]


# ── Public entry-point ────────────────────────────────────────────────────────


async def run_cluster_enrichment_batch(
    session: AsyncSession,
    *,
    batch_size: int,
    table: str = "story",
) -> dict[str, int]:
    """Enrich a batch of pending clusters using their mention_identifiers.

    Args:
        session:    Active async DB session.
        batch_size: Maximum number of clusters to process per call.
        table:      "story" targets story_clusters; "root" targets root_clusters.
    """
    model = StoryCluster if table == "story" else RootCluster

    candidates = await _get_pending_candidates(session, model, limit=batch_size)
    candidate_rows = [
        {
            "cluster_id": c.cluster_id,
            "mention_identifiers": c.mention_identifiers or [],
            "source_url": c.source_url,
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
        mention_ids: list[str] = row["mention_identifiers"]
        source_url: str = row["source_url"]

        # mention_identifiers are the primary document unit; source_url is fallback.
        urls_to_try: list[str] = list(mention_ids)
        if source_url and source_url.strip() and source_url not in urls_to_try:
            urls_to_try.append(source_url)

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

            payload = await _enrich_from_url_list(urls_to_try, url_cache)

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


async def _get_pending_candidates(
    session: AsyncSession,
    model: type[StoryCluster | RootCluster],
    *,
    limit: int,
) -> list[StoryCluster | RootCluster]:
    result = await session.execute(
        select(model)
        .where(model.enrichment_status == "pending")
        .order_by(model.computed_at.asc())
        .limit(limit)
    )
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
            enrichment_status="succeeded",
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


async def _enrich_from_url_list(urls: list[str], cache: UrlCache) -> EnrichedPayload:
    """Try each URL in order, returning the first successful enrichment result.

    Both successes and failures are stored in cache to avoid redundant work
    within the same batch.
    """
    last_exc: Exception | None = None

    for url in urls:
        cached = cache.get(url)
        if isinstance(cached, Exception):
            last_exc = cached
            continue
        if cached is not None:
            logger.info("cluster_enrichment_url_cache_hit", url=url)
            return cached

        try:
            logger.info("cluster_enrichment_fetching", url=url)
            fetched = await fetch_article_html(url)
            extracted = extract_article_payload(fetched["html"])
            logger.info(
                "cluster_enrichment_fetch_ok",
                url=url,
                content_length=len(extracted.get("content", "")),
            )

            logger.info("cluster_enrichment_calling_llm", url=url)
            enriched = await enrich_article_content(extracted)
            logger.info("cluster_enrichment_llm_ok", url=url)

            payload: EnrichedPayload = {
                "article_title": enriched.article_title,
                "article_summary": enriched.article_summary,
                "cited_sources": enriched.cited_sources,
                "main_topics": enriched.main_topics,
                "keywords": enriched.keywords,
                "entities": enriched.entities.model_dump(),
            }
            cache[url] = payload
            return payload

        except Exception as exc:
            logger.warning("cluster_enrichment_url_failed", url=url, error=_stringify(exc))
            cache[url] = exc
            last_exc = exc
            continue

    raise last_exc or RuntimeError("all candidate URLs exhausted without a recorded error")


def _stringify(exc: Exception) -> str:
    msg = str(exc).strip()
    return msg or exc.__class__.__name__
