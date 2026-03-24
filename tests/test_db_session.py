"""Tests for SQLAlchemy engine/session configuration."""

from __future__ import annotations

from unittest.mock import patch


def test_get_engine_disables_sql_echo_even_in_development() -> None:
    """App DB engine should not print SQL statements or bound values to the terminal."""
    from app.db import session as session_module

    session_module._engine = None
    session_module._session_factory = None

    with (
        patch.object(session_module, "create_async_engine") as mock_create_engine,
        patch.object(session_module, "get_settings") as mock_get_settings,
    ):
        mock_get_settings.return_value.database_url = "sqlite+aiosqlite:///:memory:"
        mock_get_settings.return_value.is_development = True
        mock_create_engine.return_value = object()

        engine = session_module._get_engine()

    assert engine is mock_create_engine.return_value
    assert mock_create_engine.call_args.kwargs["echo"] is False

    session_module._engine = None
    session_module._session_factory = None
