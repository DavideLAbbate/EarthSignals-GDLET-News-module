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
    Raised when the constructed BigQuery parameters fail validation
    (e.g., date range exceeds MAX_BQ_SCAN_DAYS, missing required filter fields).

    Maps to HTTP 400 Bad Request.
    """


class BigQueryError(GDELTBackendError):
    """
    Raised when a BigQuery query fails at the transport or execution level.

    Maps to HTTP 502 Bad Gateway.
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
