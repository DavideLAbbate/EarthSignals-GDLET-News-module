"""Builds and materialises enriched story clusters from events, mentions, and GKG."""

from __future__ import annotations

import time
from collections import Counter
from datetime import UTC, datetime
from hashlib import sha256
from statistics import mean
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.exceptions import ClusterBuildError
from app.core.logging import get_logger
from app.db.models import GdeltEvent, GdeltGkg, GdeltMention
from app.db.repositories.cluster_repository import ClusterRepository
from app.db.repositories.gkg_repository import GkgRepository
from app.db.repositories.mentions_repository import MentionsRepository
from app.integrations.event_enrichment_mapper import (
    compute_severity_score,
    compute_topic_score,
    get_event_root_code_label,
    get_quad_class_label,
)
from app.services.cluster_merger import ClusterMerger

logger = get_logger(__name__)

# asyncpg hard limit for bind parameters per statement; SQLite is even tighter.
_MAX_PG_ARGS = 32_767
_MAX_SQLITE_ARGS = 999


class ClusterService:
    """Orchestrates the story-cluster materialisation pipeline."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._mentions_repo = MentionsRepository(session)
        self._gkg_repo = GkgRepository(session)
        self._cluster_repo = ClusterRepository(session)

    async def _score_source_urls(
        self,
        since_date_added: int,
        until_date_added: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return candidate source URLs and their aggregated topic scores.

        Source URLs whose domain appears in ``cluster_source_domain_blocklist``
        are excluded before scoring. These are pure aggregators / content farms
        that produce no original journalism and pollute dominant_countries and
        themes with unrelated geographies and topics.
        """
        settings = get_settings()
        blocklist = settings.cluster_source_domain_blocklist
        section_segments = settings.cluster_section_path_segments
        require_mentions = settings.cluster_require_mentions

        filters = [
            GdeltEvent.date_added >= since_date_added,
            GdeltEvent.source_url.is_not(None),
        ]
        if until_date_added is not None:
            filters.append(GdeltEvent.date_added <= until_date_added)
        stmt = (
            select(
                GdeltEvent.source_url,
                func.count(GdeltEvent.global_event_id.distinct()).label("event_count"),
                func.coalesce(func.sum(GdeltEvent.num_articles), 0).label("num_articles"),
                func.coalesce(func.sum(GdeltEvent.num_mentions), 0).label("num_mentions"),
                func.coalesce(func.sum(GdeltEvent.num_sources), 0).label("num_sources"),
            )
            .where(*filters)
            .group_by(GdeltEvent.source_url)
        )
        result = await self._session.execute(stmt)

        candidates: list[dict[str, Any]] = []
        blocked = 0
        section_filtered = 0
        mention_filtered = 0
        for row in result.all():
            if not row.source_url:
                continue
            # Gate 0 — domain blocklist
            try:
                domain = row.source_url.split("//", 1)[1].split("/", 1)[0].lower()
            except IndexError:
                domain = ""
            if domain in blocklist:
                blocked += 1
                continue
            # Gate 1 — section/aggregator URL path pattern
            if _is_section_url(row.source_url, section_segments):
                section_filtered += 1
                continue
            # Gate 2 — zero-mention vitality gate
            if require_mentions and row.num_mentions == 0:
                mention_filtered += 1
                continue
            topic_score = compute_topic_score(
                event_count=row.event_count,
                num_articles=row.num_articles,
                num_mentions=row.num_mentions,
                num_sources=row.num_sources,
            )
            if topic_score < 3.6:
                continue
            candidates.append(
                {
                    "source_url": row.source_url,
                    "event_count": row.event_count,
                    "num_articles": row.num_articles,
                    "num_mentions": row.num_mentions,
                    "num_sources": row.num_sources,
                    "topic_score": topic_score,
                }
            )
        if blocked:
            logger.info(
                "cluster_candidates_blocked", blocked=blocked, blocklist_size=len(blocklist)
            )
        if section_filtered:
            logger.info("cluster_candidates_section_filtered", filtered=section_filtered)
        if mention_filtered:
            logger.info("cluster_candidates_mention_filtered", filtered=mention_filtered)

        candidates.sort(key=lambda item: item["topic_score"], reverse=True)
        return candidates

    async def _batch_collect_events(
        self,
        source_urls: list[str],
        since_date_added: int,
        until_date_added: int | None = None,
    ) -> dict[str, list[GdeltEvent]]:
        """Fetch events for all candidate source URLs in chunked batch queries.

        Each chunk issues one SELECT with up to _MAX_PG_ARGS − 1 bind parameters
        (one slot reserved for the date_added bound). Results are merged into a
        mapping of source_url → sorted list of GdeltEvent rows.
        """
        if not source_urls:
            return {}
        dialect = (
            self._session.bind.dialect.name if self._session.bind is not None else "postgresql"
        )
        # Reserve 1 slot for the date_added parameter (2 if until bound is present)
        slots_reserved = 2 if until_date_added is not None else 1
        chunk_size = (
            (_MAX_SQLITE_ARGS - slots_reserved)
            if dialect == "sqlite"
            else (_MAX_PG_ARGS - slots_reserved)
        )
        events_by_url: dict[str, list[GdeltEvent]] = {url: [] for url in source_urls}
        for start in range(0, len(source_urls), chunk_size):
            chunk = source_urls[start : start + chunk_size]
            filters = [
                GdeltEvent.source_url.in_(chunk),
                GdeltEvent.date_added >= since_date_added,
            ]
            if until_date_added is not None:
                filters.append(GdeltEvent.date_added <= until_date_added)
            result = await self._session.execute(
                select(GdeltEvent)
                .where(*filters)
                .order_by(GdeltEvent.date_added.asc(), GdeltEvent.global_event_id.asc())
            )
            for event in result.scalars().all():
                if event.source_url in events_by_url:
                    events_by_url[event.source_url].append(event)
        return events_by_url

    async def _batch_collect_mentions(self, event_ids: list[int]) -> dict[int, list[GdeltMention]]:
        """Fetch mentions for all event IDs in a single query.

        Returns a mapping of global_event_id → list of GdeltMention rows.
        """
        all_mentions = await self._mentions_repo.get_by_event_ids(event_ids)
        mentions_by_event: dict[int, list[GdeltMention]] = {}
        for mention in all_mentions:
            mentions_by_event.setdefault(mention.global_event_id, []).append(mention)
        return mentions_by_event

    async def _batch_collect_gkg(self, mention_identifiers: list[str]) -> dict[str, list[GdeltGkg]]:
        """Fetch GKG rows for all mention identifier URLs in a single query.

        Returns a mapping of document_identifier → list of GdeltGkg rows.
        """
        all_gkg = await self._gkg_repo.get_by_document_identifiers(mention_identifiers)
        gkg_by_identifier: dict[str, list[GdeltGkg]] = {}
        for row in all_gkg:
            if row.document_identifier:
                gkg_by_identifier.setdefault(row.document_identifier, []).append(row)
        return gkg_by_identifier

    def _build_cluster(
        self,
        doc: dict[str, Any],
        events: list[GdeltEvent],
        mentions: list[GdeltMention],
        gkg_rows: list[GdeltGkg],
    ) -> dict[str, Any]:
        """Build a single materialised cluster row.

        ``gkg_rows`` must contain only the GKG rows whose ``document_identifier``
        matches the cluster's ``source_url`` — i.e. the GKG record *of* the article
        itself, not the GKG records of articles that merely cite it. Using mention GKG
        rows would contaminate themes/persons/organizations with entities from hundreds
        of unrelated documents.
        """
        source_url = doc["source_url"]
        # cluster_id must be stable across runs: keyed solely on source_url so that
        # the same story is always upserted into the same row regardless of which day
        # the pipeline runs. A date prefix would produce a new cluster_id at midnight,
        # leaving the previous day's row stale and orphaned.
        cluster_id = sha256(source_url.encode()).hexdigest()[:24]

        event_ids = [str(event.global_event_id) for event in events]
        event_type_labels = [
            get_event_root_code_label(event.event_root_code)
            for event in events
            if event.event_root_code is not None
        ]
        quad_labels = [get_quad_class_label(event.quad_class) for event in events]
        severities = [
            compute_severity_score(event.quad_class, event.goldstein_scale, event.avg_tone)
            for event in events
        ]
        # Use only action_geo_country_code (FIPS 2-char) to avoid mixing standards:
        # actor1/2_country_code are CAMEO/ISO-3 (3-char) while action_geo_country_code
        # is FIPS (2-char). Combining them causes the same country to be counted separately
        # ("US" vs "USA") and breaks country-code filtering in the clusters API.
        country_values = [
            event.action_geo_country_code for event in events if event.action_geo_country_code
        ]
        location_values = [
            event.action_geo_full_name for event in events if event.action_geo_full_name
        ]

        mention_sources = sorted({m.mention_source_name for m in mentions if m.mention_source_name})
        mention_identifiers = sorted(
            {m.mention_identifier for m in mentions if m.mention_identifier}
        )
        mention_times = [
            _parse_gdelt_timestamp(m.mention_time_date)
            for m in mentions
            if m.mention_time_date is not None
        ]

        themes = _sorted_unique(item for row in gkg_rows for item in (row.themes or []))
        persons = _sorted_unique(item for row in gkg_rows for item in (row.persons or []))
        organizations = _sorted_unique(
            item for row in gkg_rows for item in (row.organizations or [])
        )
        gkg_locations = _sorted_unique(item for row in gkg_rows for item in (row.locations or []))
        tones = [row.document_tone for row in gkg_rows if row.document_tone is not None]

        return {
            "cluster_id": cluster_id,
            "source_url": source_url,
            "event_count": doc["event_count"],
            "num_articles": doc["num_articles"],
            "num_mentions": doc["num_mentions"],
            "num_sources": doc["num_sources"],
            "topic_score": doc["topic_score"],
            "event_ids": event_ids,
            "event_date_ref_start": min(e.sql_date for e in events) if events else None,
            "event_date_ref_end": max(e.sql_date for e in events) if events else None,
            "dominant_event_types": _top_values(event_type_labels),
            "dominant_quad_classes": _top_values(quad_labels),
            "avg_severity_score": round(mean(severities), 2) if severities else None,
            "dominant_countries": _top_values(country_values),
            "dominant_locations": _top_values(location_values),
            "mention_count": len(mentions),
            "distinct_mention_sources": mention_sources,
            "mention_identifiers": mention_identifiers,
            "first_mention_at": min(mention_times) if mention_times else None,
            "last_mention_at": max(mention_times) if mention_times else None,
            "themes": themes,
            "persons": persons,
            "organizations": organizations,
            "gkg_locations": gkg_locations,
            # gkg_doc_count tracks how many GKG documents contribute to document_tone_avg so
            # ClusterMerger can compute a weighted mean rather than a mean-of-means after fusion
            "gkg_doc_count": len(tones),
            "document_tone_avg": round(mean(tones), 2) if tones else None,
            "computed_at": datetime.now(UTC),
        }

    async def build_and_materialise(
        self,
        since_dt: datetime | int,
        until_dt: datetime | int | None = None,
    ) -> int:
        """Build and persist story clusters for candidate source URLs.

        Filters events by ``date_added`` (GDELT YYYYMMDDHHMMSS integer) to honour
        sub-day precision — e.g. a 36-hour rolling window starting mid-day.

        ``until_dt`` is optional. When provided, only events within the closed
        [since_dt, until_dt] window are considered. Useful for backfill runs that
        need to process a fixed slice without bleeding into later data.

        Uses three batch queries (one per table) across all candidates to avoid the
        N×3 sequential round-trip pattern of the previous per-candidate loop.
        """
        since_date_added = _normalize_since_date_added(since_dt)
        until_date_added = _normalize_since_date_added(until_dt) if until_dt is not None else None
        t0 = time.monotonic()
        try:
            candidates = await self._score_source_urls(since_date_added, until_date_added)
            if not candidates:
                return 0

            t_score = time.monotonic()
            logger.info(
                "cluster_phase_score",
                candidates=len(candidates),
                elapsed_s=round(t_score - t0, 2),
            )

            # ── Batch query 1: all events for all candidate source URLs ──────────
            source_urls = [c["source_url"] for c in candidates]
            events_by_url = await self._batch_collect_events(
                source_urls, since_date_added, until_date_added
            )
            total_events = sum(len(v) for v in events_by_url.values())
            t_events = time.monotonic()
            logger.info(
                "cluster_phase_events",
                total_events=total_events,
                elapsed_s=round(t_events - t_score, 2),
            )

            # ── Batch query 2: all mentions for all event IDs ────────────────────
            all_event_ids: list[int] = [
                event.global_event_id for url in source_urls for event in events_by_url.get(url, [])
            ]
            mentions_by_event = await self._batch_collect_mentions(all_event_ids)
            total_mentions = sum(len(v) for v in mentions_by_event.values())
            t_mentions = time.monotonic()
            logger.info(
                "cluster_phase_mentions",
                total_mentions=total_mentions,
                elapsed_s=round(t_mentions - t_events, 2),
            )

            # ── Batch query 3: GKG rows for the source URLs themselves ───────────
            # themes/persons/organizations/tone are extracted ONLY from the GKG row
            # whose document_identifier matches the cluster's source_url. Using GKG
            # rows from mention_identifiers (documents that cite the source) would
            # contaminate the cluster with entities from hundreds of unrelated articles.
            source_gkg_by_url = await self._batch_collect_gkg(source_urls)
            total_gkg = sum(len(v) for v in source_gkg_by_url.values())
            t_gkg = time.monotonic()
            logger.info(
                "cluster_phase_gkg",
                unique_identifiers=len(source_urls),
                total_gkg=total_gkg,
                elapsed_s=round(t_gkg - t_mentions, 2),
            )

            # ── Assemble per-candidate cluster dicts (pure Python, no more I/O) ──
            cluster_rows: list[dict[str, Any]] = []
            for candidate in candidates:
                url = candidate["source_url"]
                events = events_by_url.get(url, [])
                event_ids_for_url = [e.global_event_id for e in events]
                mentions: list[GdeltMention] = [
                    m for eid in event_ids_for_url for m in mentions_by_event.get(eid, [])
                ]
                source_gkg_rows: list[GdeltGkg] = source_gkg_by_url.get(url, [])
                cluster_rows.append(
                    self._build_cluster(candidate, events, mentions, source_gkg_rows)
                )
            t_build = time.monotonic()
            logger.info(
                "cluster_phase_build",
                cluster_rows=len(cluster_rows),
                elapsed_s=round(t_build - t_gkg, 2),
            )

            # ── Merge semantically related clusters ──────────────────────────────
            # mention_overlap_min=2 requires at least two shared mention URLs to merge,
            # preventing a single high-traffic news wire URL (e.g. Reuters) from fusing
            # unrelated stories. max_themes_for_jaccard=50 guards against O(n²) explosion.
            # max_merge_day_gap blocks merges between clusters whose event date ranges are
            # more than N calendar days apart (configurable via CLUSTER_MAX_MERGE_DAY_GAP).
            merger = ClusterMerger(
                mention_overlap_min=2,
                jaccard_threshold=0.3,
                max_merge_day_gap=get_settings().cluster_max_merge_day_gap,
            )
            cluster_rows = merger.merge(cluster_rows)
            t_merge = time.monotonic()
            logger.info(
                "cluster_phase_merge",
                merged_rows=len(cluster_rows),
                elapsed_s=round(t_merge - t_build, 2),
            )

            if not cluster_rows:
                return 0

            inserted = await self._cluster_repo.bulk_upsert(cluster_rows)
            t_upsert = time.monotonic()
            logger.info(
                "clusters_materialised",
                count=inserted,
                since_date_added=since_date_added,
                elapsed_s=round(t_upsert - t_merge, 2),
                total_elapsed_s=round(t_upsert - t0, 2),
            )
            return inserted
        except Exception as exc:  # pragma: no cover - covered via raised domain error
            raise ClusterBuildError("Failed to build story clusters", detail=str(exc)) from exc


def _top_values(values: list[str], limit: int = 5) -> list[str]:
    """Return the most common non-empty values in deterministic order."""
    counts = Counter(value for value in values if value and value != "Sconosciuto")
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [value for value, _count in ranked[:limit]]


def _sorted_unique(values: Any) -> list[str]:
    """Return sorted unique string values from an iterable."""
    return sorted({value for value in values if value})


def _normalize_since_date_added(value: datetime | int) -> int:
    """Convert datetime or CLI-style integer inputs into DATEADDED format."""
    if isinstance(value, datetime):
        return int(value.strftime("%Y%m%d%H%M%S"))

    normalized = str(value)
    if len(normalized) == 8:
        return int(f"{normalized}000000")
    if len(normalized) == 14:
        return int(normalized)

    raise ValueError("since_dt must be a datetime or a YYYYMMDD / YYYYMMDDHHMMSS integer")


def _parse_gdelt_timestamp(value: int) -> datetime:
    """Convert a GDELT YYYYMMDDHHMMSS integer timestamp into UTC datetime."""
    return datetime.strptime(str(value), "%Y%m%d%H%M%S").replace(tzinfo=UTC)


def _is_section_url(url: str, segments: tuple[str, ...]) -> bool:
    """Return True if the URL path contains any of the given section path segments.

    Matching is case-insensitive. An empty ``segments`` tuple always returns False.
    Used to discard aggregator/archive/tag pages before cluster scoring.
    """
    if not segments:
        return False
    url_lower = url.lower()
    return any(seg.lower() in url_lower for seg in segments)
