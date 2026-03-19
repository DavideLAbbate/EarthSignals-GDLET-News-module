"""Tests for story cluster materialisation."""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256

import pytest
from sqlalchemy import select

from app.db.models import GdeltEvent, GdeltGkg, GdeltMention, StoryCluster
from app.services.cluster_service import ClusterService


# cluster_id is now solely a 24-hex-char SHA-256 prefix of the source_url,
# with no date prefix, so the same URL always maps to the same row.
def _expected_cluster_id(source_url: str) -> str:
    return sha256(source_url.encode()).hexdigest()[:24]


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
    assert sorted(cluster.themes or []) == ["ARMEDCONFLICT", "IRAN", "MILITARY_ACTION"]
    assert sorted(cluster.persons or []) == ["Mojtaba Khamenei"]
    assert sorted(cluster.organizations or []) == ["IRGC"]
    assert sorted(cluster.gkg_locations or []) == ["Bahrain", "Tehran, Tehran, Iran"]
    assert cluster.document_tone_avg == pytest.approx(-7.0)


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
