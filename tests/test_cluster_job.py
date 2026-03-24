"""Tests for the scheduled cluster materialisation entrypoint."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest


async def test_run_cluster_job_uses_cluster_service_with_session() -> None:
    from app.scheduler.cluster_job import run_cluster_job

    session = MagicMock()
    session.scalar = AsyncMock(return_value=20260324120000)
    session.commit = AsyncMock()
    context_manager = MagicMock()
    context_manager.__aenter__ = AsyncMock(return_value=session)
    context_manager.__aexit__ = AsyncMock(return_value=False)
    session_factory = MagicMock(return_value=context_manager)

    import unittest.mock

    with unittest.mock.patch("app.scheduler.cluster_job.ClusterService") as cluster_service_cls:
        cluster_service = cluster_service_cls.return_value
        cluster_service.build_and_materialise = AsyncMock(return_value=3)

        count = await run_cluster_job(session_factory)

    assert count == 3
    session_factory.assert_called_once_with()
    cluster_service_cls.assert_called_once_with(session)
    cluster_service.build_and_materialise.assert_awaited_once()


async def test_run_cluster_job_anchors_window_to_latest_ingested_dateadded() -> None:
    from app.scheduler.cluster_job import run_cluster_job

    session = MagicMock()
    session.scalar = AsyncMock(return_value=20260324120000)
    session.commit = AsyncMock()
    context_manager = MagicMock()
    context_manager.__aenter__ = AsyncMock(return_value=session)
    context_manager.__aexit__ = AsyncMock(return_value=False)
    session_factory = MagicMock(return_value=context_manager)

    import unittest.mock

    with unittest.mock.patch("app.scheduler.cluster_job.ClusterService") as cluster_service_cls:
        cluster_service = cluster_service_cls.return_value
        cluster_service.build_and_materialise = AsyncMock(return_value=3)

        await run_cluster_job(session_factory)

    await_args = cluster_service.build_and_materialise.await_args
    assert await_args is not None
    assert await_args.args[0] == datetime(2026, 3, 23, 0, 0, tzinfo=UTC)
    assert await_args.args[1] == 20260324120000


async def test_run_cluster_job_commits_after_success() -> None:
    from app.scheduler.cluster_job import run_cluster_job

    session = MagicMock()
    session.scalar = AsyncMock(return_value=20260324120000)
    session.commit = AsyncMock()
    context_manager = MagicMock()
    context_manager.__aenter__ = AsyncMock(return_value=session)
    context_manager.__aexit__ = AsyncMock(return_value=False)
    session_factory = MagicMock(return_value=context_manager)

    import unittest.mock

    with unittest.mock.patch("app.scheduler.cluster_job.ClusterService") as cluster_service_cls:
        cluster_service = cluster_service_cls.return_value
        cluster_service.build_and_materialise = AsyncMock(return_value=3)

        await run_cluster_job(session_factory)

    session.commit.assert_awaited_once()


async def test_run_cluster_job_raises_actionable_error_when_component_tables_missing() -> None:
    from app.scheduler.cluster_job import run_cluster_job

    session = MagicMock()
    session.scalar = AsyncMock(return_value=20260324120000)
    session.commit = AsyncMock()
    context_manager = MagicMock()
    context_manager.__aenter__ = AsyncMock(return_value=session)
    context_manager.__aexit__ = AsyncMock(return_value=False)
    session_factory = MagicMock(return_value=context_manager)

    import unittest.mock

    with unittest.mock.patch("app.scheduler.cluster_job.ClusterService") as cluster_service_cls:
        from app.core.exceptions import ClusterBuildError

        cluster_service = cluster_service_cls.return_value
        cluster_service.build_and_materialise = AsyncMock(
            side_effect=ClusterBuildError(
                "Failed to build story clusters",
                detail='relation "cluster_components" does not exist',
            )
        )

        with pytest.raises(RuntimeError, match="alembic upgrade head"):
            await run_cluster_job(session_factory)
