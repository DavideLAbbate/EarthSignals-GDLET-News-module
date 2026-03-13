"""
Typed domain exceptions.

All exceptions raised within the application's service and integration
layers inherit from GDELTBackendError, enabling uniform handler registration.
"""

from __future__ import annotations


class GDELTBackendError(Exception):
    """Base class for all application domain errors."""

    def __init__(self, message: str, detail: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail


class FilterInterpretationError(GDELTBackendError):
    """
    Raised when Claude returns a response that cannot be parsed or validated
    into a NormalizedFilters schema.

    Maps to HTTP 422 Unprocessable Entity.
    """


class QueryValidationError(GDELTBackendError):
    """
    Raised when query parameters fail validation
    (e.g., date range exceeds limits, missing required filter fields).

    Maps to HTTP 400 Bad Request.
    """


class SyncError(GDELTBackendError):
    """
    Raised when the 15-minute metadata sync job encounters a fatal error.

    Maps to HTTP 500 Internal Server Error.
    """


class AnthropicUnavailableError(GDELTBackendError):
    """
    Raised when the Anthropic API is unavailable after all retry attempts.

    Maps to HTTP 503 Service Unavailable with Retry-After header.
    """


class IngestionError(GDELTBackendError):
    """Raised when event ingestion fails (HTTP download, parse, or DB write error)."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class RetentionError(GDELTBackendError):
    """Raised when retention cleanup fails."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class LocalQueryError(GDELTBackendError):
    """Raised when a local PostgreSQL query fails at the transport or execution level.

    Maps to HTTP 502 Bad Gateway.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class ArticleProcessingError(GDELTBackendError):
    """Raised when deterministic article fetching or extraction fails."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class ClusterError(GDELTBackendError):
    """General cluster pipeline error.

    Maps to HTTP 500 Internal Server Error.
    """


class ClusterBuildError(ClusterError):
    """Raised when the cluster materialisation job fails.

    Maps to HTTP 503 Service Unavailable.
    """
