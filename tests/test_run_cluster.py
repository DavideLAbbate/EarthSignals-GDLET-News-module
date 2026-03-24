"""Tests for the manual cluster materialisation CLI entrypoint."""

from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest


def _load_run_cluster_module(module_name: str):
    module_path = Path(__file__).resolve().parents[1] / "run_cluster.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_run_cluster_import_does_not_execute_cli(monkeypatch) -> None:
    run_calls: list[object] = []

    def _fake_run(coro):
        run_calls.append(coro)
        coro.close()

    monkeypatch.setattr(asyncio, "run", _fake_run)

    _load_run_cluster_module("run_cluster_import_test")

    assert run_calls == []


def test_run_cluster_helper_is_defined_before_main_guard() -> None:
    module_path = Path(__file__).resolve().parents[1] / "run_cluster.py"
    lines = module_path.read_text(encoding="utf-8").splitlines()

    helper_line = next(
        index
        for index, line in enumerate(lines, start=1)
        if line.startswith("def _raise_if_component_tables_missing")
    )
    main_guard_line = next(
        index
        for index, line in enumerate(lines, start=1)
        if line.startswith('if __name__ == "__main__"')
    )

    assert helper_line < main_guard_line


async def test_run_cluster_main_materialises_and_commits() -> None:
    module = _load_run_cluster_module("run_cluster_main_test")

    session = MagicMock()
    session.commit = AsyncMock()
    context_manager = MagicMock()
    context_manager.__aenter__ = AsyncMock(return_value=session)
    context_manager.__aexit__ = AsyncMock(return_value=False)
    factory = MagicMock(return_value=context_manager)

    module._get_session_factory = MagicMock(return_value=factory)
    module.ClusterService = MagicMock()
    module.ClusterService.return_value.build_and_materialise = AsyncMock(return_value=2)
    module.sys.argv = ["run_cluster.py", "20260308", "20260309"]

    await module.main()

    module._get_session_factory.assert_called_once_with()
    factory.assert_called_once_with()
    module.ClusterService.assert_called_once_with(session)
    module.ClusterService.return_value.build_and_materialise.assert_awaited_once_with(
        20260308, 20260309
    )
    session.commit.assert_awaited_once_with()


async def test_run_cluster_main_raises_actionable_error_when_component_tables_missing() -> None:
    module = _load_run_cluster_module("run_cluster_missing_table_test")

    session = MagicMock()
    session.commit = AsyncMock()
    context_manager = MagicMock()
    context_manager.__aenter__ = AsyncMock(return_value=session)
    context_manager.__aexit__ = AsyncMock(return_value=False)
    factory = MagicMock(return_value=context_manager)

    from app.core.exceptions import ClusterBuildError

    module._get_session_factory = MagicMock(return_value=factory)
    module.ClusterService = MagicMock()
    module.ClusterService.return_value.build_and_materialise = AsyncMock(
        side_effect=ClusterBuildError(
            "Failed to build story clusters",
            detail='relation "cluster_components" does not exist',
        )
    )
    module.sys.argv = ["run_cluster.py", "20260308"]

    with pytest.raises(RuntimeError, match="alembic upgrade head"):
        await module.main()
