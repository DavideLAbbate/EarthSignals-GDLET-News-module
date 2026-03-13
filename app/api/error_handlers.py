"""
Global exception handlers.

All application domain errors are caught here and converted to
structured JSON responses with appropriate HTTP status codes.
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.core.exceptions import (
    AnthropicUnavailableError,
    ClusterBuildError,
    ClusterError,
    FilterInterpretationError,
    GDELTBackendError,
    IngestionError,
    LocalQueryError,
    QueryValidationError,
    RetentionError,
    SyncError,
)


def _error_response(
    status_code: int, error_type: str, message: str, detail: str | None = None
) -> JSONResponse:
    body = {"error": error_type, "message": message}
    if detail:
        body["detail"] = detail
    return JSONResponse(status_code=status_code, content=body)


def register_error_handlers(app: FastAPI) -> None:
    """Register all domain exception handlers on the FastAPI app."""

    @app.exception_handler(FilterInterpretationError)
    async def handle_filter_interpretation_error(
        request: Request, exc: FilterInterpretationError
    ) -> JSONResponse:
        return _error_response(
            422,
            "filter_interpretation_error",
            exc.message,
            exc.detail,
        )

    @app.exception_handler(QueryValidationError)
    async def handle_query_validation_error(
        request: Request, exc: QueryValidationError
    ) -> JSONResponse:
        return _error_response(
            400,
            "query_validation_error",
            exc.message,
            exc.detail,
        )

    @app.exception_handler(AnthropicUnavailableError)
    async def handle_anthropic_unavailable(
        request: Request, exc: AnthropicUnavailableError
    ) -> JSONResponse:
        response = _error_response(
            503,
            "anthropic_unavailable",
            "Filter normalization service is temporarily unavailable. Please retry.",
            exc.detail,
        )
        response.headers["Retry-After"] = "30"
        return response

    @app.exception_handler(SyncError)
    async def handle_sync_error(request: Request, exc: SyncError) -> JSONResponse:
        return _error_response(
            500,
            "sync_error",
            "Internal sync error. The 15-minute sync job encountered an error.",
            exc.detail,
        )

    @app.exception_handler(IngestionError)
    async def ingestion_error_handler(
        request: Request,
        exc: IngestionError,
    ) -> JSONResponse:
        """Handle IngestionError - returns 500."""
        return _error_response(
            500,
            "ingestion_error",
            exc.message,
            exc.detail,
        )

    @app.exception_handler(RetentionError)
    async def retention_error_handler(
        request: Request,
        exc: RetentionError,
    ) -> JSONResponse:
        """Handle RetentionError - returns 500."""
        return _error_response(
            500,
            "retention_error",
            exc.message,
            exc.detail,
        )

    @app.exception_handler(LocalQueryError)
    async def local_query_error_handler(
        request: Request,
        exc: LocalQueryError,
    ) -> JSONResponse:
        """Handle LocalQueryError - returns 502."""
        return _error_response(
            502,
            "local_query_error",
            exc.message,
            exc.detail,
        )

    @app.exception_handler(ClusterBuildError)
    async def cluster_build_error_handler(
        request: Request,
        exc: ClusterBuildError,
    ) -> JSONResponse:
        """Handle ClusterBuildError — returns 503 Service Unavailable."""
        return _error_response(
            503,
            "cluster_build_error",
            exc.message,
            exc.detail,
        )

    @app.exception_handler(ClusterError)
    async def cluster_error_handler(
        request: Request,
        exc: ClusterError,
    ) -> JSONResponse:
        """Handle ClusterError — returns 500 Internal Server Error."""
        return _error_response(
            500,
            "cluster_error",
            exc.message,
            exc.detail,
        )

    @app.exception_handler(GDELTBackendError)
    async def handle_generic_gdelt_error(request: Request, exc: GDELTBackendError) -> JSONResponse:
        return _error_response(
            500,
            "internal_error",
            exc.message,
            exc.detail,
        )

    @app.exception_handler(Exception)
    async def handle_unhandled_exception(request: Request, exc: Exception) -> JSONResponse:
        return _error_response(
            500,
            "unexpected_error",
            "An unexpected error occurred.",
            str(exc),
        )
