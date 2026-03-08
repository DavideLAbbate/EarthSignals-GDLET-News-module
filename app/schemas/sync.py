"""
Sync-related Pydantic response schemas.
"""

from __future__ import annotations

from pydantic import BaseModel


class TopCountry(BaseModel):
    fips_code: str
    event_count: int


class TopEventCode(BaseModel):
    root_code: str
    label: str
    event_count: int


class SyncStatusResponse(BaseModel):
    """Response schema for GET /sync/status."""

    last_sync_at: str | None = None  # ISO 8601 UTC
    latest_sqldate: int | None = None  # Most recent GDELT record date (YYYYMMDD)
    mapping_version: str | None = None  # ISO 8601 UTC
    sync_status: str = "unknown"  # "success" | "error" | "unknown"
    error_message: str | None = None
    top_countries: list[TopCountry] = []
    top_event_root_codes: list[TopEventCode] = []


class FiltersMetadataResponse(BaseModel):
    """Response schema for GET /filters/metadata."""

    top_countries: list[TopCountry] = []
    top_event_root_codes: list[TopEventCode] = []
    last_sync_at: str | None = None
    mapping_version: str | None = None
