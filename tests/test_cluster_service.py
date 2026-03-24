"""Tests for story cluster materialisation."""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256

import pytest
from sqlalchemy import select

from app.core.config import get_settings
from app.core.exceptions import ClusterBuildError
from app.db.models import (
    ClusterComponent,
    ClusterComponentEvent,
    GdeltEvent,
    GdeltGkg,
    GdeltMention,
    RootCluster,
    StoryCluster,
)
from app.integrations.event_enrichment_mapper import compute_component_topic_score
from app.services.cluster_service import ClusterService, _is_section_url


def _expected_cluster_id_for_events(event_ids: list[int]) -> str:
    joined = ",".join(str(event_id) for event_id in sorted(event_ids))
    return sha256(joined.encode()).hexdigest()[:24]


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
    assert cluster.cluster_id == _expected_cluster_id_for_events([101, 102])
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
    assert root_rows[0].cluster_id == _expected_cluster_id_for_events(list(range(100000, 106001)))
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
    assert [row.cluster_id for row in story_rows] == [
        _expected_cluster_id_for_events(list(range(200000, 205000)))
    ]
    assert [row.cluster_id for row in root_rows] == [
        _expected_cluster_id_for_events(list(range(300000, 305001)))
    ]
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
    assert root_rows[0].cluster_id == _expected_cluster_id_for_events(list(range(500000, 505001)))


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
    assert story_rows[0].cluster_id == _expected_cluster_id_for_events(list(range(700000, 700010)))


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


async def test_build_candidate_components_groups_connected_event_and_mention_nodes(
    db_session,
) -> None:
    db_session.add_all(
        [
            _make_event(901, source_url="https://example.com/story-a", date_added=20260311010101),
            _make_event(902, source_url="https://example.com/story-b", date_added=20260311020202),
            GdeltMention(
                global_event_id=901,
                mention_time_date=20260311090000,
                mention_source_name="shared.example.com",
                mention_identifier="https://shared.example.com/story",
            ),
            GdeltMention(
                global_event_id=902,
                mention_time_date=20260311100000,
                mention_source_name="shared.example.com",
                mention_identifier="https://shared.example.com/story",
            ),
            GdeltMention(
                global_event_id=902,
                mention_time_date=20260311110000,
                mention_source_name="isolated.example.com",
                mention_identifier="https://isolated.example.com/story",
            ),
        ]
    )
    await db_session.commit()

    components = await ClusterService(db_session)._build_candidate_components(20260311000000)

    assert len(components) == 1
    assert components[0]["event_ids"] == {901, 902}
    assert components[0]["mention_identifiers"] == {
        "https://shared.example.com/story",
        "https://isolated.example.com/story",
    }
    assert components[0]["source_urls"] == {
        "https://example.com/story-a",
        "https://example.com/story-b",
    }


async def test_build_candidate_components_filters_blocklisted_and_section_mentions(
    db_session,
) -> None:
    db_session.add_all(
        [
            _make_event(903, source_url="https://example.com/story-a", date_added=20260311010101),
            _make_event(904, source_url="https://example.com/story-b", date_added=20260311020202),
            GdeltMention(
                global_event_id=903,
                mention_identifier="https://shared.example.com/story",
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=904,
                mention_identifier="https://shared.example.com/story",
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=903,
                mention_identifier="https://www.yahoo.com/news/blocked-story",
                mention_source_name="yahoo.com",
            ),
            GdeltMention(
                global_event_id=904,
                mention_identifier="https://www.example.com/category/world/archive-page",
                mention_source_name="example.com",
            ),
        ]
    )
    await db_session.commit()

    components = await ClusterService(db_session)._build_candidate_components(20260311000000)

    assert len(components) == 1
    assert components[0]["mention_identifiers"] == {"https://shared.example.com/story"}


async def test_build_candidate_components_omits_singleton_components(db_session) -> None:
    db_session.add_all(
        [
            _make_event(905, source_url="https://example.com/story-a", date_added=20260311010101),
            _make_event(906, source_url="https://example.com/story-b", date_added=20260311020202),
            _make_event(907, source_url="https://example.com/story-c", date_added=20260311030303),
            GdeltMention(
                global_event_id=905,
                mention_identifier="https://shared.example.com/story",
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=906,
                mention_identifier="https://shared.example.com/story",
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=907,
                mention_identifier="https://singleton.example.com/story",
                mention_source_name="singleton.example.com",
            ),
        ]
    )
    await db_session.commit()

    components = await ClusterService(db_session)._build_candidate_components(20260311000000)

    assert len(components) == 1
    assert components[0]["event_ids"] == {905, 906}


async def test_build_and_materialise_rejects_singleton_components_by_default(db_session) -> None:
    db_session.add(
        _make_event(
            9501,
            source_url="https://example.com/lone-story",
            date_added=20260312000000,
            num_articles=5,
            num_mentions=1,
            num_sources=1,
        )
    )
    await db_session.commit()

    count = await ClusterService(db_session).build_and_materialise(20260312000000)
    await db_session.commit()

    assert count == 0
    assert (await db_session.execute(select(StoryCluster))).scalars().all() == []


def test_component_metrics_capture_size_density_and_time_span() -> None:
    service = object.__new__(ClusterService)
    component = {
        "event_ids": {1001, 1002, 1003},
        "mention_identifiers": {
            "https://mentions.example.com/story-a",
            "https://mentions.example.com/story-b",
        },
        "edges": {
            (1001, "https://mentions.example.com/story-a"),
            (1002, "https://mentions.example.com/story-a"),
            (1002, "https://mentions.example.com/story-b"),
            (1003, "https://mentions.example.com/story-b"),
        },
    }
    events = [
        _make_event(
            1001,
            source_url="https://www.reuters.com/world/story-a",
            date_added=20260312000000,
        ),
        _make_event(
            1002,
            source_url="https://apnews.com/article/story-b",
            date_added=20260312030000,
        ),
        _make_event(
            1003,
            source_url="https://www.reuters.com/world/story-c",
            date_added=20260312060000,
        ),
    ]

    metrics = service._compute_component_metrics(component, events)

    assert metrics["event_id_count"] == 3
    assert metrics["source_url_count"] == 3
    assert metrics["domain_count"] == 2
    assert metrics["component_density"] == pytest.approx(4 / 6, rel=1e-3)
    assert metrics["event_time_span_hours"] == pytest.approx(6.0)


@pytest.mark.parametrize(
    ("metric_name", "metric_value", "expected_gate"),
    [
        ("event_id_count", 1, "min_event_ids"),
        ("source_url_count", 1, "min_source_urls"),
        ("domain_count", 1, "min_domains"),
        ("event_time_span_hours", 25.0, "max_event_span_hours"),
        ("component_density", 0.1, "min_density"),
    ],
)
def test_component_gate_rejects_failed_metric(
    monkeypatch,
    metric_name: str,
    metric_value: float | int,
    expected_gate: str,
) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "cluster_candidate_min_event_ids", 2)
    monkeypatch.setattr(settings, "cluster_candidate_min_source_urls", 2)
    monkeypatch.setattr(settings, "cluster_candidate_min_domains", 2)
    monkeypatch.setattr(settings, "cluster_candidate_max_event_span_hours", 24.0)
    monkeypatch.setattr(settings, "cluster_candidate_min_density", 0.5)

    metrics = {
        "event_id_count": 3,
        "source_url_count": 3,
        "domain_count": 3,
        "event_time_span_hours": 6.0,
        "component_density": 0.75,
    }
    metrics[metric_name] = metric_value

    accepted, failed_gates = object.__new__(ClusterService)._evaluate_component_gates(metrics)

    assert accepted is False
    assert failed_gates == [expected_gate]


def test_component_gate_accepts_when_all_metrics_pass(monkeypatch) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "cluster_candidate_min_event_ids", 2)
    monkeypatch.setattr(settings, "cluster_candidate_min_source_urls", 2)
    monkeypatch.setattr(settings, "cluster_candidate_min_domains", 2)
    monkeypatch.setattr(settings, "cluster_candidate_max_event_span_hours", 24.0)
    monkeypatch.setattr(settings, "cluster_candidate_min_density", 0.5)

    accepted, failed_gates = object.__new__(ClusterService)._evaluate_component_gates(
        {
            "event_id_count": 3,
            "source_url_count": 3,
            "domain_count": 2,
            "event_time_span_hours": 6.0,
            "component_density": 0.75,
        }
    )

    assert accepted is True
    assert failed_gates == []


def test_component_rejection_logs_failed_gates_and_metrics(monkeypatch, mocker) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "cluster_candidate_min_event_ids", 2)
    monkeypatch.setattr(settings, "cluster_candidate_min_source_urls", 2)
    monkeypatch.setattr(settings, "cluster_candidate_min_domains", 2)
    monkeypatch.setattr(settings, "cluster_candidate_max_event_span_hours", 24.0)
    monkeypatch.setattr(settings, "cluster_candidate_min_density", 0.5)

    component = {
        "event_ids": {1101, 1102},
        "mention_identifiers": {"https://shared.example.com/story"},
        "edges": {
            (1101, "https://shared.example.com/story"),
            (1102, "https://shared.example.com/story"),
        },
        "source_urls": {"https://example.com/story-a"},
    }
    events_by_id = {
        1101: _make_event(
            1101,
            source_url="https://example.com/story-a",
            date_added=20260312000000,
        ),
        1102: _make_event(
            1102,
            source_url="https://example.com/story-a",
            date_added=20260312010000,
        ),
    }
    logger_info = mocker.patch("app.services.cluster_service.logger.info")

    admitted = object.__new__(ClusterService)._admit_component_candidates([component], events_by_id)

    assert admitted == []
    logger_info.assert_called_once()
    event_name = logger_info.call_args.args[0]
    payload = logger_info.call_args.kwargs
    assert event_name == "cluster_component_rejected"
    assert payload["failed_gates"] == ["min_source_urls", "min_domains"]
    assert payload["metrics"] == {
        "event_id_count": 2,
        "source_url_count": 1,
        "domain_count": 1,
        "component_density": 1.0,
        "event_time_span_hours": 1.0,
    }
    assert payload["component_id"]


def test_component_topic_score_uses_component_level_counts() -> None:
    component = {
        "event_ids": {1201, 1202, 1203, 1204},
        "mention_identifiers": {
            "https://shared.example.com/story-a",
            "https://shared.example.com/story-b",
        },
        "edges": {
            (1201, "https://shared.example.com/story-a"),
            (1202, "https://shared.example.com/story-a"),
            (1203, "https://shared.example.com/story-b"),
            (1204, "https://shared.example.com/story-b"),
        },
        "source_urls": {
            "https://www.reuters.com/world/story-a",
            "https://apnews.com/article/story-b",
            "https://www.bbc.com/news/story-c",
        },
    }
    events_by_id = {
        1201: _make_event(1201, source_url="https://www.reuters.com/world/story-a"),
        1202: _make_event(1202, source_url="https://apnews.com/article/story-b"),
        1203: _make_event(1203, source_url="https://www.bbc.com/news/story-c"),
        1204: _make_event(1204, source_url="https://www.reuters.com/world/story-a"),
    }

    admitted = object.__new__(ClusterService)._admit_component_candidates([component], events_by_id)

    assert len(admitted) == 1
    assert admitted[0]["topic_score"] == compute_component_topic_score(
        event_id_count=4,
        source_url_count=3,
        domain_count=3,
    )


def test_choose_canonical_component_prefers_oldest_first_seen() -> None:
    service = object.__new__(ClusterService)
    candidates = [
        {"component_id": "newer", "first_seen_at": datetime(2026, 3, 24, tzinfo=UTC)},
        {"component_id": "older", "first_seen_at": datetime(2026, 3, 20, tzinfo=UTC)},
    ]

    canonical = service._choose_canonical_component(candidates)

    assert canonical["component_id"] == "older"


def test_event_overlap_returns_count_and_historical_ratio() -> None:
    service = object.__new__(ClusterService)

    overlap = service._event_overlap({"1001", "1002", "1003"}, {"1002", "1003", "1004", "1005"})

    assert overlap["count"] == 2
    assert overlap["historical_ratio"] == pytest.approx(0.5)
    assert overlap["current_ratio"] == pytest.approx(2 / 3)


def test_find_matching_components_returns_multi_match_candidates() -> None:
    service = object.__new__(ClusterService)
    current_payload = {"event_ids": {"1001", "1002", "1003"}}
    persisted_components = [
        {"component_id": "comp-a", "first_seen_at": datetime(2026, 3, 20, tzinfo=UTC)},
        {"component_id": "comp-b", "first_seen_at": datetime(2026, 3, 21, tzinfo=UTC)},
        {"component_id": "comp-c", "first_seen_at": datetime(2026, 3, 22, tzinfo=UTC)},
    ]
    active_membership = {
        "comp-a": {"1001", "1002"},
        "comp-b": {"1002", "1003"},
        "comp-c": {"2000"},
    }

    matches = service._find_matching_components(
        current_payload, persisted_components, active_membership
    )

    assert [match["component_id"] for match in matches] == ["comp-a", "comp-b"]


def test_find_split_candidates_uses_historical_overlap_ratio() -> None:
    service = object.__new__(ClusterService)
    historical_component = {"component_id": "comp-a"}
    active_membership = {"comp-a": {"1001", "1002", "1003", "1004", "1005"}}
    current_payloads = [
        {"cluster_id": "cluster-a", "event_ids": {"1001", "1002", "1003"}},
        {"cluster_id": "cluster-b", "event_ids": {"1003", "1004", "1005"}},
        {"cluster_id": "cluster-c", "event_ids": {"9001"}},
    ]

    split_candidates = service._find_split_candidates(
        historical_component,
        current_payloads,
        active_membership,
        overlap_min=2,
        overlap_ratio=0.6,
    )

    assert [candidate["cluster_id"] for candidate in split_candidates] == ["cluster-a", "cluster-b"]


async def test_build_and_materialise_uses_component_candidates_instead_of_single_source_url(
    db_session,
) -> None:
    source_url_a = "https://www.reuters.com/world/story-a"
    source_url_b = "https://apnews.com/article/story-b"
    shared_mention = "https://shared.example.com/story"
    db_session.add_all(
        [
            _make_event(
                1301,
                source_url=source_url_a,
                date_added=20260312000000,
                num_articles=1,
                num_mentions=0,
                num_sources=1,
            ),
            _make_event(
                1302,
                source_url=source_url_b,
                date_added=20260312010000,
                num_articles=1,
                num_mentions=0,
                num_sources=1,
            ),
            GdeltMention(
                global_event_id=1301,
                mention_time_date=20260312090000,
                mention_source_name="shared.example.com",
                mention_identifier=shared_mention,
            ),
            GdeltMention(
                global_event_id=1302,
                mention_time_date=20260312100000,
                mention_source_name="shared.example.com",
                mention_identifier=shared_mention,
            ),
            GdeltGkg(
                document_identifier=source_url_a,
                themes=["IRAN", "ATTACK"],
                persons=["Person A"],
                organizations=["Org A"],
                locations=["Tehran, Tehran, Iran"],
                document_tone=-5.0,
            ),
            GdeltGkg(
                document_identifier=source_url_b,
                themes=["IRAN", "SANCTIONS"],
                persons=["Person B"],
                organizations=["Org B"],
                locations=["Washington, District of Columbia, United States"],
                document_tone=-2.0,
            ),
        ]
    )
    await db_session.commit()

    count = await ClusterService(db_session).build_and_materialise(20260312000000)
    await db_session.commit()

    assert count == 1

    cluster = (await db_session.execute(select(StoryCluster))).scalars().one()
    assert set(cluster.event_ids or []) == {"1301", "1302"}
    assert cluster.topic_score == pytest.approx(
        compute_component_topic_score(
            event_id_count=2,
            source_url_count=2,
            domain_count=2,
        )
    )
    assert cluster.cluster_id == _expected_cluster_id_for_events([1301, 1302])
    assert set(cluster.themes or []) == {"ATTACK", "IRAN", "SANCTIONS"}
    assert cluster.source_url in {source_url_a, source_url_b}


async def test_build_and_materialise_preserves_component_id_across_growth(db_session) -> None:
    shared_mention = "https://shared.example.com/persistent-story"
    db_session.add_all(
        [
            _make_event(2101, source_url="https://a.example.com/story", date_added=20260324000000),
            _make_event(2102, source_url="https://b.example.com/story", date_added=20260324010000),
            _make_event(2104, source_url="https://d.example.com/story", date_added=20260324011000),
            GdeltMention(
                global_event_id=2101,
                mention_identifier=shared_mention,
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=2102,
                mention_identifier=shared_mention,
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=2104,
                mention_identifier=shared_mention,
                mention_source_name="shared.example.com",
            ),
        ]
    )
    await db_session.commit()

    service = ClusterService(db_session)
    await service.build_and_materialise(20260324000000, 20260324020000)
    await db_session.commit()

    first_component = (await db_session.execute(select(ClusterComponent))).scalars().one()

    db_session.add_all(
        [
            _make_event(2103, source_url="https://c.example.com/story", date_added=20260324020000),
            GdeltMention(
                global_event_id=2103,
                mention_identifier=shared_mention,
                mention_source_name="shared.example.com",
            ),
        ]
    )
    await db_session.commit()

    await service.build_and_materialise(20260324000000, 20260324030000)
    await db_session.commit()

    components = (await db_session.execute(select(ClusterComponent))).scalars().all()
    memberships = (await db_session.execute(select(ClusterComponentEvent))).scalars().all()

    assert len(components) == 1
    assert components[0].component_id == first_component.component_id
    assert {membership.event_id for membership in memberships if membership.is_active} == {
        "2101",
        "2102",
        "2103",
        "2104",
    }


async def test_build_and_materialise_marks_non_canonical_match_as_merged_into(db_session) -> None:
    now = datetime(2026, 3, 24, tzinfo=UTC)
    db_session.add_all(
        [
            ClusterComponent(
                component_id="older-component",
                status="active",
                anchor_source_url="https://older.example.com/story",
                component_source_urls=["https://older.example.com/story"],
                anchor_locked_at=now,
                first_seen_at=datetime(2026, 3, 20, tzinfo=UTC),
                last_seen_at=now,
                has_gkg=False,
            ),
            ClusterComponent(
                component_id="newer-component",
                status="active",
                anchor_source_url="https://newer.example.com/story",
                component_source_urls=["https://newer.example.com/story"],
                anchor_locked_at=now,
                first_seen_at=datetime(2026, 3, 22, tzinfo=UTC),
                last_seen_at=now,
                has_gkg=False,
            ),
            ClusterComponentEvent(
                component_id="older-component",
                event_id="3101",
                first_seen_at=now,
                last_seen_at=now,
                is_active=True,
            ),
            ClusterComponentEvent(
                component_id="older-component",
                event_id="3102",
                first_seen_at=now,
                last_seen_at=now,
                is_active=True,
            ),
            ClusterComponentEvent(
                component_id="newer-component",
                event_id="3103",
                first_seen_at=now,
                last_seen_at=now,
                is_active=True,
            ),
            ClusterComponentEvent(
                component_id="newer-component",
                event_id="3104",
                first_seen_at=now,
                last_seen_at=now,
                is_active=True,
            ),
            _make_event(3101, source_url="https://a.example.com/story", date_added=20260324000000),
            _make_event(3102, source_url="https://b.example.com/story", date_added=20260324000100),
            _make_event(3103, source_url="https://c.example.com/story", date_added=20260324000200),
            _make_event(3104, source_url="https://d.example.com/story", date_added=20260324000300),
            GdeltMention(
                global_event_id=3101,
                mention_identifier="https://shared.example.com/merge-story",
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=3102,
                mention_identifier="https://shared.example.com/merge-story",
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=3103,
                mention_identifier="https://shared.example.com/merge-story",
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=3104,
                mention_identifier="https://shared.example.com/merge-story",
                mention_source_name="shared.example.com",
            ),
        ]
    )
    await db_session.commit()

    await ClusterService(db_session).build_and_materialise(20260324000000, 20260324010000)
    await db_session.commit()

    components = {
        component.component_id: component
        for component in (await db_session.execute(select(ClusterComponent))).scalars().all()
    }

    assert components["older-component"].status == "active"
    assert components["newer-component"].status == "merged"
    assert components["newer-component"].merged_into_component_id == "older-component"


async def test_build_and_materialise_keeps_original_anchor_on_matched_component(db_session) -> None:
    now = datetime(2026, 3, 24, tzinfo=UTC)
    db_session.add_all(
        [
            ClusterComponent(
                component_id="component-1",
                status="active",
                anchor_source_url="https://anchor.example.com/original-story",
                component_source_urls=["https://anchor.example.com/original-story"],
                anchor_locked_at=now,
                first_seen_at=datetime(2026, 3, 20, tzinfo=UTC),
                last_seen_at=now,
                has_gkg=False,
            ),
            ClusterComponentEvent(
                component_id="component-1",
                event_id="4101",
                first_seen_at=now,
                last_seen_at=now,
                is_active=True,
            ),
            ClusterComponentEvent(
                component_id="component-1",
                event_id="4102",
                first_seen_at=now,
                last_seen_at=now,
                is_active=True,
            ),
            _make_event(
                4101, source_url="https://z.example.com/new-story", date_added=20260324000000
            ),
            _make_event(
                4102, source_url="https://y.example.com/new-story", date_added=20260324000100
            ),
            GdeltMention(
                global_event_id=4101,
                mention_identifier="https://shared.example.com/anchor-story",
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=4102,
                mention_identifier="https://shared.example.com/anchor-story",
                mention_source_name="shared.example.com",
            ),
        ]
    )
    await db_session.commit()

    await ClusterService(db_session).build_and_materialise(20260324000000, 20260324010000)
    await db_session.commit()

    component = (await db_session.execute(select(ClusterComponent))).scalars().one()
    assert component.anchor_source_url == "https://anchor.example.com/original-story"


async def test_build_and_materialise_marks_component_split_when_history_branches(
    db_session,
    monkeypatch,
) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "cluster_component_split_overlap_min", 2)
    monkeypatch.setattr(settings, "cluster_component_split_overlap_ratio", 0.6)

    now = datetime(2026, 3, 24, tzinfo=UTC)
    db_session.add(
        ClusterComponent(
            component_id="component-1",
            status="active",
            anchor_source_url="https://anchor.example.com/original-story",
            component_source_urls=["https://anchor.example.com/original-story"],
            anchor_locked_at=now,
            first_seen_at=datetime(2026, 3, 20, tzinfo=UTC),
            last_seen_at=now,
            has_gkg=False,
        )
    )
    for event_id in [5101, 5102, 5103, 5104, 5105, 5106]:
        db_session.add(
            ClusterComponentEvent(
                component_id="component-1",
                event_id=str(event_id),
                first_seen_at=now,
                last_seen_at=now,
                is_active=True,
            )
        )

    shared_a = "https://shared.example.com/split-a"
    shared_b = "https://shared.example.com/split-b"
    db_session.add_all(
        [
            _make_event(5101, source_url="https://a.example.com/story", date_added=20260324000000),
            _make_event(5102, source_url="https://b.example.com/story", date_added=20260324000100),
            _make_event(5103, source_url="https://c.example.com/story", date_added=20260324000200),
            _make_event(5104, source_url="https://d.example.com/story", date_added=20260324000300),
            _make_event(5105, source_url="https://e.example.com/story", date_added=20260324000400),
            _make_event(5106, source_url="https://f.example.com/story", date_added=20260324000500),
            GdeltMention(
                global_event_id=5101,
                mention_identifier=shared_a,
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=5102,
                mention_identifier=shared_a,
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=5103,
                mention_identifier=shared_a,
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=5106,
                mention_identifier=shared_b,
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=5104,
                mention_identifier=shared_b,
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=5105,
                mention_identifier=shared_b,
                mention_source_name="shared.example.com",
            ),
        ]
    )
    await db_session.commit()

    await ClusterService(db_session).build_and_materialise(20260324000000, 20260324010000)
    await db_session.commit()

    components = {
        component.component_id: component
        for component in (await db_session.execute(select(ClusterComponent))).scalars().all()
    }
    assert components["component-1"].status == "split"


async def test_build_and_materialise_marks_component_stale_after_missed_runs(
    db_session,
    monkeypatch,
) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "cluster_component_stale_after_missing_runs", 3)

    now = datetime(2026, 3, 24, tzinfo=UTC)
    db_session.add(
        ClusterComponent(
            component_id="component-1",
            status="active",
            anchor_source_url="https://anchor.example.com/original-story",
            component_source_urls=["https://anchor.example.com/original-story"],
            anchor_locked_at=now,
            first_seen_at=datetime(2026, 3, 20, tzinfo=UTC),
            last_seen_at=now,
            missing_run_count=2,
            has_gkg=False,
        )
    )
    db_session.add(
        ClusterComponentEvent(
            component_id="component-1",
            event_id="6101",
            first_seen_at=now,
            last_seen_at=now,
            is_active=True,
        )
    )
    db_session.add_all(
        [
            _make_event(6201, source_url="https://a.example.com/story", date_added=20260324000000),
            _make_event(6202, source_url="https://b.example.com/story", date_added=20260324000100),
            _make_event(6203, source_url="https://c.example.com/story", date_added=20260324000200),
            GdeltMention(
                global_event_id=6201,
                mention_identifier="https://shared.example.com/new",
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=6202,
                mention_identifier="https://shared.example.com/new",
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=6203,
                mention_identifier="https://shared.example.com/new",
                mention_source_name="shared.example.com",
            ),
        ]
    )
    await db_session.commit()

    await ClusterService(db_session).build_and_materialise(20260324000000, 20260324010000)
    await db_session.commit()

    component = (
        (
            await db_session.execute(
                select(ClusterComponent).where(ClusterComponent.component_id == "component-1")
            )
        )
        .scalars()
        .one()
    )
    assert component.status == "stale"
    assert component.missing_run_count == 3


async def test_build_and_materialise_updates_component_soft_reference_for_story_cluster(
    db_session,
) -> None:
    shared_mention = "https://shared.example.com/soft-story"
    db_session.add_all(
        [
            _make_event(7101, source_url="https://a.example.com/story", date_added=20260324000000),
            _make_event(7102, source_url="https://b.example.com/story", date_added=20260324000100),
            _make_event(7103, source_url="https://c.example.com/story", date_added=20260324000200),
            GdeltMention(
                global_event_id=7101,
                mention_identifier=shared_mention,
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=7102,
                mention_identifier=shared_mention,
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=7103,
                mention_identifier=shared_mention,
                mention_source_name="shared.example.com",
            ),
        ]
    )
    await db_session.commit()

    await ClusterService(db_session).build_and_materialise(20260324000000, 20260324010000)
    await db_session.commit()

    component = (await db_session.execute(select(ClusterComponent))).scalars().one()
    cluster = (await db_session.execute(select(StoryCluster))).scalars().one()
    assert component.current_cluster_id == cluster.cluster_id
    assert component.current_table == "story_clusters"


async def test_build_and_materialise_updates_component_soft_reference_for_root_cluster(
    db_session,
    monkeypatch,
) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "cluster_candidate_min_source_urls", 1)
    monkeypatch.setattr(settings, "cluster_candidate_min_domains", 1)
    shared_mention = "https://shared.example.com/soft-root"
    _add_many_events(
        db_session,
        start_event_id=720000,
        count=5001,
        source_url="https://root.example.com/story",
        sql_date=20260324,
        date_added=20260324000000,
    )
    for event_id in range(720000, 725001):
        db_session.add(
            GdeltMention(
                global_event_id=event_id,
                mention_identifier=shared_mention,
                mention_source_name="shared.example.com",
            )
        )
    await db_session.commit()

    await ClusterService(db_session).build_and_materialise(20260324000000, 20260325000000)
    await db_session.commit()

    component = (await db_session.execute(select(ClusterComponent))).scalars().one()
    cluster = (await db_session.execute(select(RootCluster))).scalars().one()
    assert component.current_cluster_id == cluster.cluster_id
    assert component.current_table == "root_clusters"


async def test_validate_materialized_consistency_fails_on_duplicate_cluster_ids(db_session) -> None:
    service = ClusterService(db_session)
    now = datetime(2026, 3, 24, tzinfo=UTC)
    db_session.add_all(
        [
            StoryCluster(
                cluster_id="dup-cluster", source_url="https://a.example.com", computed_at=now
            ),
            RootCluster(
                cluster_id="dup-cluster", source_url="https://b.example.com", computed_at=now
            ),
        ]
    )
    await db_session.commit()

    with pytest.raises(ClusterBuildError):
        await service._validate_materialized_consistency()


async def test_validate_materialized_consistency_fails_on_missing_soft_link(db_session) -> None:
    service = ClusterService(db_session)
    now = datetime(2026, 3, 24, tzinfo=UTC)
    db_session.add(
        ClusterComponent(
            component_id="component-1",
            status="active",
            anchor_source_url="https://anchor.example.com/original-story",
            component_source_urls=["https://anchor.example.com/original-story"],
            anchor_locked_at=now,
            first_seen_at=now,
            last_seen_at=now,
            current_cluster_id="missing-cluster",
            current_table="story_clusters",
            current_computed_at=now,
            has_gkg=False,
        )
    )
    await db_session.commit()

    with pytest.raises(ClusterBuildError):
        await service._validate_materialized_consistency()


async def test_build_and_materialise_persists_no_gkg_component_state(db_session, mocker) -> None:
    shared_mention = "https://shared.example.com/no-gkg"
    db_session.add_all(
        [
            _make_event(8101, source_url="https://a.example.com/story", date_added=20260324000000),
            _make_event(8102, source_url="https://b.example.com/story", date_added=20260324000100),
            _make_event(8103, source_url="https://c.example.com/story", date_added=20260324000200),
            GdeltMention(
                global_event_id=8101,
                mention_identifier=shared_mention,
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=8102,
                mention_identifier=shared_mention,
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=8103,
                mention_identifier=shared_mention,
                mention_source_name="shared.example.com",
            ),
        ]
    )
    await db_session.commit()

    logger_info = mocker.patch("app.services.cluster_service.logger.info")

    count = await ClusterService(db_session).build_and_materialise(20260324000000, 20260324010000)
    await db_session.commit()

    component = (await db_session.execute(select(ClusterComponent))).scalars().one()
    cluster = (await db_session.execute(select(StoryCluster))).scalars().one()

    assert count == 1
    assert cluster.cluster_id
    assert component.has_gkg is False
    assert any(
        call.args and call.args[0] == "cluster_component_no_gkg"
        for call in logger_info.call_args_list
    )


async def test_new_component_is_not_aged_as_missed_in_creation_run(db_session, monkeypatch) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "cluster_component_stale_after_missing_runs", 1)

    shared_mention = "https://shared.example.com/new-component"
    db_session.add_all(
        [
            _make_event(9101, source_url="https://a.example.com/story", date_added=20260324000000),
            _make_event(9102, source_url="https://b.example.com/story", date_added=20260324000100),
            GdeltMention(
                global_event_id=9101,
                mention_identifier=shared_mention,
                mention_source_name="shared.example.com",
            ),
            GdeltMention(
                global_event_id=9102,
                mention_identifier=shared_mention,
                mention_source_name="shared.example.com",
            ),
        ]
    )
    await db_session.commit()

    await ClusterService(db_session).build_and_materialise(20260324000000, 20260324010000)
    await db_session.commit()

    component = (await db_session.execute(select(ClusterComponent))).scalars().one()
    assert component.status == "active"
    assert component.missing_run_count == 0


async def test_zero_result_window_ages_historical_components_to_stale(
    db_session, monkeypatch
) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "cluster_component_stale_after_missing_runs", 1)

    now = datetime(2026, 3, 24, tzinfo=UTC)
    db_session.add(
        ClusterComponent(
            component_id="component-1",
            status="active",
            anchor_source_url="https://anchor.example.com/story",
            component_source_urls=["https://anchor.example.com/story"],
            anchor_locked_at=now,
            first_seen_at=now,
            last_seen_at=now,
            has_gkg=False,
        )
    )
    db_session.add(
        ClusterComponentEvent(
            component_id="component-1",
            event_id="9991",
            first_seen_at=now,
            last_seen_at=now,
            is_active=True,
        )
    )
    await db_session.commit()

    count = await ClusterService(db_session).build_and_materialise(20260325000000, 20260325010000)
    await db_session.commit()

    component = (
        (
            await db_session.execute(
                select(ClusterComponent).where(ClusterComponent.component_id == "component-1")
            )
        )
        .scalars()
        .one()
    )
    assert count == 0
    assert component.status == "stale"
    assert component.missing_run_count == 1


async def test_build_cluster_deduplicates_mention_count_with_shared_identifier(db_session) -> None:
    service = ClusterService(db_session)
    events = [
        _make_event(1401, source_url="https://example.com/story-a"),
        _make_event(1402, source_url="https://example.com/story-b"),
    ]
    mentions = [
        GdeltMention(
            global_event_id=1401,
            mention_identifier="https://shared.example.com/story",
            mention_source_name="shared.example.com",
        ),
        GdeltMention(
            global_event_id=1402,
            mention_identifier="https://shared.example.com/story",
            mention_source_name="shared.example.com",
        ),
    ]

    cluster = service._build_cluster(
        {
            "cluster_id": _expected_cluster_id_for_events([1401, 1402]),
            "source_url": "https://example.com/story-a",
            "event_count": 2,
            "num_articles": 2,
            "num_mentions": 2,
            "num_sources": 2,
            "topic_score": 1.0,
        },
        events,
        mentions,
        [],
    )

    assert cluster["mention_identifiers"] == ["https://shared.example.com/story"]
    assert cluster["mention_count"] == 1


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
