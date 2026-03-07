"""
Filter-related Pydantic schemas.

RawFilterInput    — what the frontend sends
NormalizedFilters — what Claude returns (validated)
ClaudeFilterResponse — Claude's raw JSON response schema
DateRange         — reusable date range model
"""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator


class DateRange(BaseModel):
    """Year-based date range from the frontend."""

    from_year: int = Field(..., alias="from", ge=1979, le=2100)
    to_year: int = Field(..., alias="to", ge=1979, le=2100)

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def validate_range(self) -> "DateRange":
        if self.from_year > self.to_year:
            raise ValueError(f"date_range.from ({self.from_year}) must be <= date_range.to ({self.to_year})")
        return self


class RawFilterInput(BaseModel):
    """
    The filter payload sent by the frontend.

    All fields are optional. At least one non-null field must be present
    (enforced in the filter service, not here).
    """

    country: str | None = Field(
        default=None,
        description="Country name in any language/format (e.g. 'Italy', 'Italia', 'IT')",
        max_length=200,
    )
    event_type: str | None = Field(
        default=None,
        description="Event type in user-friendly terms (e.g. 'protest', 'war', 'diplomacy')",
        max_length=200,
    )
    macro_topic: str | None = Field(
        default=None,
        description="Macro topic or theme (e.g. 'energy', 'climate', 'economy')",
        max_length=200,
    )
    date_range: DateRange | None = Field(
        default=None,
        description="Year range for filtering events",
    )

    def has_any_filter(self) -> bool:
        """Return True if at least one filter field is set."""
        return any([self.country, self.event_type, self.macro_topic, self.date_range])

    def to_canonical_dict(self) -> dict:
        """
        Return a canonical dict for cache key computation.
        String values are lowercased and stripped.
        """
        d = {}
        if self.country:
            d["country"] = self.country.lower().strip()
        if self.event_type:
            d["event_type"] = self.event_type.lower().strip()
        if self.macro_topic:
            d["macro_topic"] = self.macro_topic.lower().strip()
        if self.date_range:
            d["date_from"] = self.date_range.from_year
            d["date_to"] = self.date_range.to_year
        return d


class ClaudeFilterResponse(BaseModel):
    """
    Schema for Claude's JSON response.

    Validated immediately after receipt — if any required field is
    missing or malformed, a FilterInterpretationError is raised.
    """

    cameo_country_code: str | None = Field(
        default=None,
        description="CAMEO 3-letter country code for actor filtering",
        max_length=10,
    )
    fips_country_code: str | None = Field(
        default=None,
        description="FIPS 10-4 2-letter country code for geographic filtering",
        max_length=5,
    )
    event_root_codes: list[str] = Field(
        default_factory=list,
        description="CAMEO event root codes (2-digit strings, e.g. ['14', '19'])",
    )
    event_base_codes: list[str] = Field(
        default_factory=list,
        description="CAMEO event base codes (3-digit strings, optional refinement)",
    )
    date_from_sqldate: int = Field(
        description="Start date as YYYYMMDD integer",
    )
    date_to_sqldate: int = Field(
        description="End date as YYYYMMDD integer",
    )
    normalization_notes: str = Field(
        default="",
        description="Claude's reasoning about the normalization choices",
        max_length=1000,
    )


class NormalizedFilters(BaseModel):
    """
    The fully normalized filter structure used to build BigQuery queries.
    Derived from ClaudeFilterResponse and passed to the query service.
    """

    cameo_country_code: str | None = None
    fips_country_code: str | None = None
    event_root_codes: list[str] = Field(default_factory=list)
    event_base_codes: list[str] = Field(default_factory=list)
    date_from_sqldate: int
    date_to_sqldate: int
    normalization_notes: str = ""

    @classmethod
    def from_claude_response(cls, response: ClaudeFilterResponse) -> "NormalizedFilters":
        return cls(
            cameo_country_code=response.cameo_country_code,
            fips_country_code=response.fips_country_code,
            event_root_codes=response.event_root_codes,
            event_base_codes=response.event_base_codes,
            date_from_sqldate=response.date_from_sqldate,
            date_to_sqldate=response.date_to_sqldate,
            normalization_notes=response.normalization_notes,
        )
