"""Tests for story cluster materialisation."""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256

import pytest
from sqlalchemy import select

from app.db.models import GdeltEvent, GdeltGkg, GdeltMention, RootCluster, StoryCluster
from app.services.cluster_service import ClusterService, _is_section_url


# cluster_id is now solely a 24-hex-char SHA-256 prefix of the source_url,
# with no date prefix, so the same URL always maps to the same row.
def _expected_cluster_id(source_url: str) -> str:
    return sha256(source_url.encode()).hexdigest()[:24]


def _add_many_events(
    db_session,
    *,
    start_event_id: int,
    count: int,
    source_url: str,
    sql_date: int,
    date_added: int,
    num_mentions: int = 1,
    num_sources: int = 1,
    num_articles: int = 1,
    action_geo_country_code: str = "IR",
) -> None:
    db_session.add_all(
        [
            _make_event(
                start_event_id + index,
                source_url=source_url,
                sql_date=sql_date,
                date_added=date_added + index,
                num_mentions=num_mentions,
                num_sources=num_sources,
                num_articles=num_articles,
                action_geo_country_code=action_geo_country_code,
            )
            for index in range(count)
        ]
    )


def _make_event(
    global_event_id: int,
    *,
    source_url: str = "https://example.com/story",
    sql_date: int = 20260310,
    date_added: int | None = None,
    event_root_code: str = "19",
    quad_class: int = 4,
    goldstein_scale: float = -10.0,
    avg_tone: float = -5.0,
    num_mentions: int = 10,
    num_sources: int = 3,
    num_articles: int = 4,
    action_geo_full_name: str = "Tehran, Tehran, Iran",
    action_geo_country_code: str = "IR",
    actor1_country_code: str = "IRN",
    actor2_country_code: str = "USA",
) -> GdeltEvent:
    return GdeltEvent(
        global_event_id=global_event_id,
        sql_date=sql_date,
        date_added=date_added if date_added is not None else 20260310010101 + global_event_id,
        source_url=source_url,
        event_code=event_root_code + "0",
        event_base_code=event_root_code,
        event_root_code=event_root_code,
        quad_class=quad_class,
        goldstein_scale=goldstein_scale,
        avg_tone=avg_tone,
        num_mentions=num_mentions,
        num_sources=num_sources,
        num_articles=num_articles,
        action_geo_full_name=action_geo_full_name,
        action_geo_country_code=action_geo_country_code,
        actor1_country_code=actor1_country_code,
        actor2_country_code=actor2_country_code,
    )


async def test_build_and_materialise_creates_story_cluster(db_session) -> None:
    source_url = "https://example.com/story"
    db_session.add_all(
        [
            _make_event(
                101, source_url=source_url, num_mentions=500, num_sources=50, num_articles=500
            ),
            _make_event(
                102, source_url=source_url, num_mentions=500, num_sources=50, num_articles=500
            ),
            GdeltMention(
                global_event_id=101,
                mention_time_date=20260310093000,
                mention_source_name="example.com",
                mention_identifier=source_url,
                mention_doc_tone=-3.5,
            ),
            GdeltMention(
                global_event_id=102,
                mention_time_date=20260310113000,
                mention_source_name="mirror.example.com",
                mention_identifier="https://mirror.example.com/story",
                mention_doc_tone=-2.0,
            ),
            GdeltGkg(
                document_identifier=source_url,
                themes=["ARMEDCONFLICT", "IRAN"],
                persons=["Mojtaba Khamenei"],
                organizations=["IRGC"],
                locations=["Tehran, Tehran, Iran"],
                document_tone=-8.0,
            ),
            GdeltGkg(
                document_identifier="https://mirror.example.com/story",
                themes=["IRAN", "MILITARY_ACTION"],
                persons=["Mojtaba Khamenei"],
                organizations=["IRGC"],
                locations=["Bahrain"],
                document_tone=-6.0,
            ),
        ]
    )
    await db_session.commit()

    service = ClusterService(db_session)
    count = await service.build_and_materialise(datetime(2026, 3, 1, tzinfo=UTC))
    await db_session.commit()

    assert count == 1

    result = await db_session.execute(select(StoryCluster))
    clusters = result.scalars().all()
    assert len(clusters) == 1

    cluster = clusters[0]
    assert cluster.cluster_id == _expected_cluster_id(source_url)
    assert cluster.event_count == 2
    assert cluster.num_articles == 1000
    assert cluster.num_mentions == 1000
    assert cluster.num_sources == 100
    assert cluster.event_ids == ["101", "102"]
    assert "Combattimento" in (cluster.dominant_event_types or [])
    assert "Conflitto materiale" in (cluster.dominant_quad_classes or [])
    assert "IR" in (cluster.dominant_countries or [])
    assert cluster.mention_count == 2
    assert sorted(cluster.distinct_mention_sources or []) == ["example.com", "mirror.example.com"]
    # themes/persons/organizations/locations/tone come only from the GKG row whose
    # document_identifier == source_url. The mirror GKG (MILITARY_ACTION, Bahrain,
    # tone=-6.0) must NOT appear — it belongs to a citing article, not the source.
    assert sorted(cluster.themes or []) == ["ARMEDCONFLICT", "IRAN"]
    assert sorted(cluster.persons or []) == ["Mojtaba Khamenei"]
    assert sorted(cluster.organizations or []) == ["IRGC"]
    assert sorted(cluster.gkg_locations or []) == ["Tehran, Tehran, Iran"]
    assert cluster.document_tone_avg == pytest.approx(-8.0)


async def test_build_and_materialise_moves_large_cluster_to_root_clusters_only(db_session) -> None:
    source_url = "https://example.com/root-story"
    _add_many_events(
        db_session,
        start_event_id=100000,
        count=6001,
        source_url=source_url,
        sql_date=20260301,
        date_added=20260301000000,
    )
    await db_session.commit()

    count = await ClusterService(db_session).build_and_materialise(20260301000000)
    await db_session.commit()

    story_rows = (await db_session.execute(select(StoryCluster))).scalars().all()
    root_rows = (await db_session.execute(select(RootCluster))).scalars().all()

    assert count == 1
    assert story_rows == []
    assert len(root_rows) == 1
    assert root_rows[0].cluster_id == _expected_cluster_id(source_url)
    assert root_rows[0].event_count == 6001


async def test_build_and_materialise_uses_strict_root_threshold_boundary(db_session) -> None:
    story_source_url = "https://example.com/story-threshold"
    root_source_url = "https://example.com/root-threshold"
    _add_many_events(
        db_session,
        start_event_id=200000,
        count=5000,
        source_url=story_source_url,
        sql_date=20260302,
        date_added=20260302000000,
    )
    _add_many_events(
        db_session,
        start_event_id=300000,
        count=5001,
        source_url=root_source_url,
        sql_date=20260302,
        date_added=20260302010000,
    )
    await db_session.commit()

    count = await ClusterService(db_session).build_and_materialise(20260302000000)
    await db_session.commit()

    story_rows = (await db_session.execute(select(StoryCluster))).scalars().all()
    root_rows = (await db_session.execute(select(RootCluster))).scalars().all()

    assert count == 2
    assert [row.cluster_id for row in story_rows] == [_expected_cluster_id(story_source_url)]
    assert [row.cluster_id for row in root_rows] == [_expected_cluster_id(root_source_url)]
    assert story_rows[0].event_count == 5000
    assert root_rows[0].event_count == 5001


async def test_build_and_materialise_reconciles_story_to_root_on_rerun(db_session) -> None:
    source_url = "https://example.com/flip-story-to-root"
    _add_many_events(
        db_session,
        start_event_id=400000,
        count=10,
        source_url=source_url,
        sql_date=20260303,
        date_added=20260303000000,
        num_mentions=500,
        num_sources=50,
        num_articles=500,
    )
    await db_session.commit()

    first_count = await ClusterService(db_session).build_and_materialise(
        20260303000000, 20260303000009
    )
    await db_session.commit()

    assert first_count == 1
    assert (await db_session.execute(select(StoryCluster))).scalars().all()
    assert (await db_session.execute(select(RootCluster))).scalars().all() == []

    _add_many_events(
        db_session,
        start_event_id=500000,
        count=5001,
        source_url=source_url,
        sql_date=20260304,
        date_added=20260304000000,
    )
    await db_session.commit()

    second_count = await ClusterService(db_session).build_and_materialise(20260304000000)
    await db_session.commit()

    story_rows = (await db_session.execute(select(StoryCluster))).scalars().all()
    root_rows = (await db_session.execute(select(RootCluster))).scalars().all()

    assert second_count == 1
    assert story_rows == []
    assert len(root_rows) == 1
    assert root_rows[0].cluster_id == _expected_cluster_id(source_url)


async def test_build_and_materialise_reconciles_root_to_story_on_rerun(db_session) -> None:
    source_url = "https://example.com/flip-root-to-story"
    _add_many_events(
        db_session,
        start_event_id=600000,
        count=5001,
        source_url=source_url,
        sql_date=20260305,
        date_added=20260305000000,
    )
    await db_session.commit()

    first_count = await ClusterService(db_session).build_and_materialise(20260305000000)
    await db_session.commit()

    assert first_count == 1
    assert (await db_session.execute(select(StoryCluster))).scalars().all() == []
    assert (await db_session.execute(select(RootCluster))).scalars().all()

    _add_many_events(
        db_session,
        start_event_id=700000,
        count=10,
        source_url=source_url,
        sql_date=20260306,
        date_added=20260306000000,
        num_mentions=500,
        num_sources=50,
        num_articles=500,
    )
    await db_session.commit()

    second_count = await ClusterService(db_session).build_and_materialise(
        20260306000000, 20260306000009
    )
    await db_session.commit()

    story_rows = (await db_session.execute(select(StoryCluster))).scalars().all()
    root_rows = (await db_session.execute(select(RootCluster))).scalars().all()

    assert second_count == 1
    assert len(story_rows) == 1
    assert root_rows == []
    assert story_rows[0].cluster_id == _expected_cluster_id(source_url)


async def test_build_and_materialise_excludes_blocklisted_domains(db_session) -> None:
    """Source URLs from blocklisted domains must never become cluster candidates."""
    blocked_url = "https://www.yahoo.com/news/iran-attack-story"
    clean_url = "https://www.reuters.com/world/iran-attack"

    for i, (url, articles) in enumerate(
        [
            (blocked_url, 500),
            (clean_url, 500),
        ]
    ):
        db_session.add(
            _make_event(
                8000 + i,
                source_url=url,
                date_added=20260313060000,
                num_articles=articles,
                num_mentions=500,
                num_sources=100,
            )
        )
    await db_session.flush()

    await ClusterService(db_session).build_and_materialise(20260313000000)
    await db_session.commit()

    result = await db_session.execute(select(StoryCluster))
    clusters = result.scalars().all()
    source_urls = {c.source_url for c in clusters}

    assert blocked_url not in source_urls
    assert clean_url in source_urls


async def test_build_and_materialise_gkg_uses_only_source_url_document(db_session) -> None:
    """themes/persons/orgs must come only from the GKG of the source URL itself.

    A mention GKG (document_identifier = mention_identifier != source_url) must
    not contaminate the cluster with unrelated entities. This was the root cause
    of clusters containing 150+ themes and hundreds of unrelated persons.
    """
    source_url = "https://example.com/iran-story"
    mention_url = "https://otherpaper.com/unrelated-article"

    db_session.add_all(
        [
            _make_event(
                201, source_url=source_url, num_mentions=500, num_sources=50, num_articles=500
            ),
            GdeltMention(
                global_event_id=201,
                mention_time_date=20260310093000,
                mention_source_name="otherpaper.com",
                mention_identifier=mention_url,
                mention_doc_tone=-1.0,
            ),
            # GKG of the source article — clean, specific
            GdeltGkg(
                document_identifier=source_url,
                themes=["ASSASSINATION", "IRAN"],
                persons=["Ali Khamenei"],
                organizations=["IRGC"],
                locations=["Tehran, Tehran, Iran"],
                document_tone=-9.0,
            ),
            # GKG of the citing article — must NOT appear in cluster
            GdeltGkg(
                document_identifier=mention_url,
                themes=["ECONOMY", "BITCOIN", "UNITED_STATES"],
                persons=["Elon Musk", "Abraham Lincoln"],
                organizations=["Federal Reserve", "American Airlines"],
                locations=["New York, New York, United States"],
                document_tone=-1.0,
            ),
        ]
    )
    await db_session.commit()

    count = await ClusterService(db_session).build_and_materialise(datetime(2026, 3, 1, tzinfo=UTC))
    await db_session.commit()
    assert count == 1

    result = await db_session.execute(select(StoryCluster))
    cluster = result.scalars().one()

    # Source GKG entities must be present
    assert "ASSASSINATION" in (cluster.themes or [])
    assert "Ali Khamenei" in (cluster.persons or [])
    assert "IRGC" in (cluster.organizations or [])

    # Mention GKG entities must NOT appear
    assert "BITCOIN" not in (cluster.themes or [])
    assert "Elon Musk" not in (cluster.persons or [])
    assert "Abraham Lincoln" not in (cluster.persons or [])
    assert "Federal Reserve" not in (cluster.organizations or [])
    assert cluster.document_tone_avg == pytest.approx(-9.0)


async def test_build_and_materialise_skips_low_scoring_sources(db_session) -> None:
    db_session.add(
        _make_event(
            201,
            source_url="https://example.com/quiet",
            num_mentions=0,
            num_sources=0,
            num_articles=0,
        )
    )
    await db_session.commit()

    service = ClusterService(db_session)
    count = await service.build_and_materialise(datetime(2026, 3, 1, tzinfo=UTC))
    await db_session.commit()

    assert count == 0

    result = await db_session.execute(select(StoryCluster))
    assert result.scalars().all() == []


async def test_build_and_materialise_accepts_date_only_integer_since(db_session) -> None:
    source_url = "https://example.com/story"
    db_session.add_all(
        [
            _make_event(
                301, source_url=source_url, num_mentions=500, num_sources=50, num_articles=500
            ),
            _make_event(
                302, source_url=source_url, num_mentions=500, num_sources=50, num_articles=500
            ),
            GdeltMention(
                global_event_id=301,
                mention_time_date=20260310093000,
                mention_source_name="example.com",
                mention_identifier=source_url,
                mention_doc_tone=-3.5,
            ),
        ]
    )
    await db_session.commit()

    service = ClusterService(db_session)
    count = await service.build_and_materialise(20260308)
    await db_session.commit()

    assert count == 1

    result = await db_session.execute(select(StoryCluster))
    clusters = result.scalars().all()
    assert len(clusters) == 1


async def test_build_and_materialise_merges_clusters_sharing_mention_url(db_session):
    """Two source URLs sharing at least 2 mention URLs must produce one cluster in DB, not two.

    mention_overlap_min=2 requires two shared mention URLs so that a single high-traffic
    news wire URL cannot fuse unrelated stories. This test verifies the stricter threshold.
    """
    from app.db.models import GdeltEvent, GdeltMention

    shared_mentions = [
        "https://shared-news.example.com/iran-story",
        "https://shared-wire.example.com/iran-story",
    ]

    # Two source URLs each with enough signal to score >= 4.0
    # 10 events each with num_articles=500, num_mentions=500, num_sources=50
    # score = ln(11)*0.4 + ln(5001)*0.3 + ln(5001)*0.2 + ln(501)*0.1 ≈ 6.0
    for source_url, base_eid in [
        ("https://source-a.example.com/article", 8000000),
        ("https://source-b.example.com/article", 8001000),
    ]:
        for i in range(10):
            eid = base_eid + i
            db_session.add(
                GdeltEvent(
                    global_event_id=eid,
                    sql_date=20260308,
                    date_added=20260308120000,
                    source_url=source_url,
                    num_articles=500,
                    num_mentions=500,
                    num_sources=50,
                )
            )
            # Each event gets both shared mentions so the pair overlap count reaches 2
            for shared_url in shared_mentions:
                db_session.add(
                    GdeltMention(
                        global_event_id=eid,
                        mention_identifier=shared_url,
                        mention_source_name="shared-news.example.com",
                    )
                )
    await db_session.flush()

    svc = ClusterService(db_session)
    count = await svc.build_and_materialise(datetime(2026, 3, 8, tzinfo=UTC))
    await db_session.flush()

    from sqlalchemy import select
    from app.db.models import StoryCluster

    result = await db_session.execute(select(StoryCluster))
    clusters = result.scalars().all()
    assert count == 1
    assert len(clusters) == 1
    # Fused cluster must contain event_ids from both source URLs (20 total)
    assert len(clusters[0].event_ids) == 20


async def test_build_and_materialise_respects_until_dt(db_session):
    """Events after until_dt must be excluded from cluster materialisation.

    Two events for the same source URL: one inside the window, one outside.
    With until_dt set to exclude the second event, the cluster must be built
    from only the first event — and the second source URL must not appear.
    """
    db_session.add(
        _make_event(
            7000001,
            source_url="https://inside.example.com/story",
            date_added=20260313060000,
            num_articles=500,
            num_mentions=500,
            num_sources=100,
        )
    )
    db_session.add(
        _make_event(
            7000002,
            source_url="https://outside.example.com/story",
            date_added=20260314180000,  # after until bound
            num_articles=500,
            num_mentions=500,
            num_sources=100,
        )
    )
    await db_session.flush()

    count = await ClusterService(db_session).build_and_materialise(
        20260313000000,
        20260314120000,  # until: 14 mar 12:00 — excludes the second event
    )
    assert count >= 1

    from sqlalchemy import select as sa_select

    clusters = (await db_session.execute(sa_select(StoryCluster))).scalars().all()
    source_urls = {c.source_url for c in clusters}
    assert "https://inside.example.com/story" in source_urls
    assert "https://outside.example.com/story" not in source_urls


async def test_score_source_urls_excludes_candidates_between_0_5_and_4(db_session):
    """A source URL scoring >= 0.5 but < 4.0 must be excluded after threshold raise."""
    from app.db.models import GdeltEvent

    # 3 events, 0 articles/mentions/sources → score ≈ 0.55 (above 0.5, below 4.0)
    for eid in [9000010, 9000011, 9000012]:
        db_session.add(
            GdeltEvent(
                global_event_id=eid,
                sql_date=20260308,
                date_added=20260308120000,
                source_url="https://medium-signal.example.com/article",
                num_articles=0,
                num_mentions=0,
                num_sources=0,
            )
        )
    await db_session.flush()

    svc = ClusterService(db_session)
    candidates = await svc._score_source_urls(20260308)
    urls = [c["source_url"] for c in candidates]
    assert "https://medium-signal.example.com/article" not in urls


# ── _is_section_url ───────────────────────────────────────────────────────────


def test_is_section_url_matches_category():
    assert (
        _is_section_url(
            "https://www.example.com/category/world/article-title",
            ("/category/", "/tag/"),
        )
        is True
    )


def test_is_section_url_matches_search():
    assert (
        _is_section_url(
            "https://www.geneamusings.com/search/label/Gu%20te%20Family",
            ("/search/", "/label/"),
        )
        is True
    )


def test_is_section_url_does_not_match_normal_article():
    assert (
        _is_section_url(
            "https://www.reuters.com/world/2026/03/15/iran-attack/",
            ("/search/", "/category/", "/tag/"),
        )
        is False
    )


def test_is_section_url_empty_segments_never_matches():
    assert _is_section_url("https://www.example.com/category/news", ()) is False


def test_is_section_url_case_insensitive():
    assert (
        _is_section_url(
            "https://www.example.com/Category/world/article",
            ("/category/",),
        )
        is True
    )


async def test_build_cluster_sets_event_date_ref_range(db_session) -> None:
    """_build_cluster must set event_date_ref_start to min(sql_date) and event_date_ref_end
    to max(sql_date) across all events for the cluster's source URL."""
    source_url = "https://example.com/date-range-story"
    db_session.add_all(
        [
            _make_event(
                4001,
                source_url=source_url,
                sql_date=20260305,
                num_mentions=500,
                num_sources=50,
                num_articles=500,
            ),
            _make_event(
                4002,
                source_url=source_url,
                sql_date=20260308,
                num_mentions=500,
                num_sources=50,
                num_articles=500,
            ),
            _make_event(
                4003,
                source_url=source_url,
                sql_date=20260301,
                num_mentions=500,
                num_sources=50,
                num_articles=500,
            ),
        ]
    )
    await db_session.commit()

    service = ClusterService(db_session)
    count = await service.build_and_materialise(datetime(2026, 3, 1, tzinfo=UTC))
    await db_session.commit()

    assert count == 1

    result = await db_session.execute(select(StoryCluster))
    cluster = result.scalars().one()
    assert cluster.event_date_ref_start == 20260301
    assert cluster.event_date_ref_end == 20260308


async def test_fused_cluster_event_date_ref_range_is_outer_envelope(db_session) -> None:
    """Fused cluster event_date_ref range must be the outer envelope of all member clusters."""
    shared_mentions = [
        "https://shared-news.example.com/iran-story",
        "https://shared-wire.example.com/iran-story",
    ]

    # source-a: events on days 20260305 and 20260306
    for i, (url, sql_date, eid) in enumerate(
        [
            ("https://source-a.example.com/article", 20260305, 5001),
            ("https://source-a.example.com/article", 20260306, 5002),
            ("https://source-b.example.com/article", 20260308, 5003),
            ("https://source-b.example.com/article", 20260309, 5004),
        ]
    ):
        db_session.add(
            GdeltEvent(
                global_event_id=eid,
                sql_date=sql_date,
                date_added=20260310120000,
                source_url=url,
                num_articles=500,
                num_mentions=500,
                num_sources=50,
            )
        )
        for shared_url in shared_mentions:
            db_session.add(
                GdeltMention(
                    global_event_id=eid,
                    mention_identifier=shared_url,
                    mention_source_name="shared-news.example.com",
                )
            )
    await db_session.flush()

    svc = ClusterService(db_session)
    count = await svc.build_and_materialise(datetime(2026, 3, 1, tzinfo=UTC))
    await db_session.flush()

    result = await db_session.execute(select(StoryCluster))
    clusters = result.scalars().all()
    assert count == 1
    assert len(clusters) == 1
    fused = clusters[0]
    # Outer envelope: min of 20260305, 20260308 → 20260305; max of 20260306, 20260309 → 20260309
    assert fused.event_date_ref_start == 20260305
    assert fused.event_date_ref_end == 20260309


# ── _score_source_urls quality gates ─────────────────────────────────────────


async def test_score_source_urls_excludes_section_path_url(db_session) -> None:
    """A source URL whose path contains a section segment must be excluded."""
    db_session.add(
        GdeltEvent(
            global_event_id=6001,
            sql_date=20260315,
            date_added=20260315120000,
            source_url="https://www.example.com/category/world/iran-story",
            num_articles=500,
            num_mentions=500,
            num_sources=100,
        )
    )
    await db_session.flush()

    svc = ClusterService(db_session)
    candidates = await svc._score_source_urls(20260315000000)
    urls = [c["source_url"] for c in candidates]
    assert "https://www.example.com/category/world/iran-story" not in urls


async def test_score_source_urls_excludes_zero_mention_candidate(db_session) -> None:
    """A source URL with num_mentions == 0 must be excluded when cluster_require_mentions=True."""
    db_session.add(
        GdeltEvent(
            global_event_id=6002,
            sql_date=20260315,
            date_added=20260315120000,
            source_url="https://www.example.com/zero-mention-story",
            num_articles=500,
            num_mentions=0,
            num_sources=100,
        )
    )
    await db_session.flush()

    svc = ClusterService(db_session)
    candidates = await svc._score_source_urls(20260315000000)
    urls = [c["source_url"] for c in candidates]
    assert "https://www.example.com/zero-mention-story" not in urls


async def test_score_source_urls_allows_normal_article_with_mentions(db_session) -> None:
    """A well-formed article URL with mentions must still pass both gates."""
    db_session.add(
        GdeltEvent(
            global_event_id=6003,
            sql_date=20260315,
            date_added=20260315120000,
            source_url="https://www.reuters.com/world/2026/03/15/iran-story/",
            num_articles=500,
            num_mentions=500,
            num_sources=100,
        )
    )
    await db_session.flush()

    svc = ClusterService(db_session)
    candidates = await svc._score_source_urls(20260315000000)
    urls = [c["source_url"] for c in candidates]
    assert "https://www.reuters.com/world/2026/03/15/iran-story/" in urls
