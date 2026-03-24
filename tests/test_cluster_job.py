"""Tests for the scheduled cluster materialisation entrypoint."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock


async def test_run_cluster_job_uses_cluster_service_with_session() -> None:
    from app.scheduler.cluster_job import run_cluster_job

    session = MagicMock()
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
