"""
Tests for global error handlers.

Verifies that each domain exception maps to the correct HTTP status code
and returns a structured JSON error body.
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.error_handlers import register_error_handlers
from app.core.exceptions import (
    AnthropicUnavailableError,
    BigQueryError,
    ClusterBuildError,
    ClusterError,
    FilterInterpretationError,
    QueryValidationError,
    SyncError,
)


def _make_test_app_with_route(exc: Exception) -> FastAPI:
    """Create a minimal FastAPI app that raises the given exception on GET /test."""
    app = FastAPI()
    register_error_handlers(app)

    @app.get("/test")
    async def trigger():
        raise exc

    return app


def test_filter_interpretation_error_returns_422():
    app = _make_test_app_with_route(FilterInterpretationError("Bad filter", detail="x"))
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/test")
    assert response.status_code == 422
    assert response.json()["error"] == "filter_interpretation_error"
    assert "Bad filter" in response.json()["message"]


def test_query_validation_error_returns_400():
    app = _make_test_app_with_route(QueryValidationError("Date range too wide"))
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/test")
    assert response.status_code == 400
    assert response.json()["error"] == "query_validation_error"


def test_bigquery_error_returns_502():
    app = _make_test_app_with_route(BigQueryError("BQ failed"))
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/test")
    assert response.status_code == 502
    assert response.json()["error"] == "bigquery_error"


def test_anthropic_unavailable_returns_503_with_retry_after():
    app = _make_test_app_with_route(AnthropicUnavailableError("Claude down"))
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/test")
    assert response.status_code == 503
    assert response.json()["error"] == "anthropic_unavailable"
    assert "Retry-After" in response.headers


def test_sync_error_returns_500():
    app = _make_test_app_with_route(SyncError("Sync failed"))
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/test")
    assert response.status_code == 500
    assert response.json()["error"] == "sync_error"


def test_cluster_build_error_returns_503():
    app = _make_test_app_with_route(ClusterBuildError("Cluster job unavailable"))
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/test")
    assert response.status_code == 503
    assert response.json()["error"] == "cluster_build_error"


def test_cluster_error_returns_500():
    app = _make_test_app_with_route(ClusterError("Cluster build failed"))
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/test")
    assert response.status_code == 500
    assert response.json()["error"] == "cluster_error"


def test_unhandled_exception_returns_500():
    app = _make_test_app_with_route(ValueError("Something totally unexpected"))
    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/test")
    assert response.status_code == 500
    assert response.json()["error"] == "unexpected_error"


def test_error_response_always_json():
    """All error responses contain valid JSON with 'error' and 'message' keys."""
    exceptions = [
        FilterInterpretationError("x"),
        QueryValidationError("x"),
        BigQueryError("x"),
        AnthropicUnavailableError("x"),
        SyncError("x"),
        ClusterBuildError("x"),
        ClusterError("x"),
    ]
    for exc in exceptions:
        app = _make_test_app_with_route(exc)
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/test")
        data = response.json()
        assert "error" in data
        assert "message" in data
