"""
Filter-related Pydantic schemas.

RawFilterInput        — what the frontend sends
NormalizedFilters    — fully normalized BigQuery-ready filters
ClaudeFilterResponse — Claude's JSON response for free-text normalization
Nested filter models — sentiment, impact, actors, source, and event codes
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
            raise ValueError(
                f"date_range.from ({self.from_year}) must be <= date_range.to ({self.to_year})"
            )
        return self


class SentimentFilterInput(BaseModel):
    """Optional sentiment/intensity thresholds."""

    tone_min: float | None = Field(default=None, ge=-100, le=100)
    tone_max: float | None = Field(default=None, ge=-100, le=100)
    goldstein_min: float | None = Field(default=None, ge=-10, le=10)
    goldstein_max: float | None = Field(default=None, ge=-10, le=10)

    @model_validator(mode="after")
    def validate_ranges(self) -> "SentimentFilterInput":
        if (
            self.tone_min is not None
            and self.tone_max is not None
            and self.tone_min > self.tone_max
        ):
            raise ValueError("sentiment.tone_min must be <= sentiment.tone_max")
        if (
            self.goldstein_min is not None
            and self.goldstein_max is not None
            and self.goldstein_min > self.goldstein_max
        ):
            raise ValueError("sentiment.goldstein_min must be <= sentiment.goldstein_max")
        return self

    def has_any_filter(self) -> bool:
        return any(
            value is not None
            for value in [
                self.tone_min,
                self.tone_max,
                self.goldstein_min,
                self.goldstein_max,
            ]
        )


class ImpactFilterInput(BaseModel):
    """Optional relevance thresholds."""

    min_mentions: int | None = Field(default=None, ge=0)
    min_sources: int | None = Field(default=None, ge=0)
    min_articles: int | None = Field(default=None, ge=0)

    def has_any_filter(self) -> bool:
        return any(
            value is not None for value in [self.min_mentions, self.min_sources, self.min_articles]
        )


class ActorFilterInput(BaseModel):
    """Optional actor-country filters."""

    actor1_country: str | None = Field(default=None, max_length=200)
    actor2_country: str | None = Field(default=None, max_length=200)

    def has_any_filter(self) -> bool:
        return any([self.actor1_country, self.actor2_country])


class SourceFilterInput(BaseModel):
    """Optional source domain filtering."""

    domains: list[str] = Field(default_factory=list, max_length=50)

    def has_any_filter(self) -> bool:
        return bool(self.domains)


class EventCodeFilterInput(BaseModel):
    """Optional direct CAMEO code filters from the UI."""

    root_codes: list[str] = Field(default_factory=list)
    base_codes: list[str] = Field(default_factory=list)
    full_codes: list[str] = Field(default_factory=list)

    def has_any_filter(self) -> bool:
        return bool(self.root_codes or self.base_codes or self.full_codes)


class RawFilterInput(BaseModel):
    """
    The filter payload sent by the frontend.

    Supports both free-text filters (normalized by Claude) and structured
    filters that can be applied directly to BigQuery.
    """

    country: str | None = Field(
        default=None,
        description="Country name in any language/format (e.g. 'Italy', 'Italia', 'IT')",
        max_length=200,
    )
    countries: list[str] = Field(
        default_factory=list,
        description="Optional list of geographic countries for multi-country filtering",
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
    sentiment: SentimentFilterInput | None = Field(default=None)
    impact: ImpactFilterInput | None = Field(default=None)
    actors: ActorFilterInput | None = Field(default=None)
    source: SourceFilterInput | None = Field(default=None)
    event_codes: EventCodeFilterInput | None = Field(default=None)
    quad_classes: list[int] = Field(default_factory=list)

    def has_any_filter(self) -> bool:
        """Return True if at least one filter field is set."""
        return any(
            [
                self.country,
                self.countries,
                self.event_type,
                self.macro_topic,
                self.date_range,
                self.sentiment and self.sentiment.has_any_filter(),
                self.impact and self.impact.has_any_filter(),
                self.actors and self.actors.has_any_filter(),
                self.source and self.source.has_any_filter(),
                self.event_codes and self.event_codes.has_any_filter(),
                self.quad_classes,
            ]
        )

    def has_free_text_filters(self) -> bool:
        """Return True if a Claude normalization pass is needed."""
        return any([self.country, self.event_type, self.macro_topic])

    def to_canonical_dict(self) -> dict:
        """
        Return a canonical dict for cache key computation.
        String values are lowercased and stripped.
        """
        d: dict[str, object] = {}
        if self.country:
            d["country"] = self.country.lower().strip()
        if self.countries:
            d["countries"] = sorted(
                country.lower().strip() for country in self.countries if country.strip()
            )
        if self.event_type:
            d["event_type"] = self.event_type.lower().strip()
        if self.macro_topic:
            d["macro_topic"] = self.macro_topic.lower().strip()
        if self.date_range:
            d["date_from"] = self.date_range.from_year
            d["date_to"] = self.date_range.to_year
        if self.sentiment and self.sentiment.has_any_filter():
            d["sentiment"] = self.sentiment.model_dump(exclude_none=True)
        if self.impact and self.impact.has_any_filter():
            d["impact"] = self.impact.model_dump(exclude_none=True)
        if self.actors and self.actors.has_any_filter():
            d["actors"] = {
                key: value.lower().strip()
                for key, value in self.actors.model_dump(exclude_none=True).items()
            }
        if self.source and self.source.has_any_filter():
            d["source_domains"] = sorted(
                domain.lower().strip() for domain in self.source.domains if domain.strip()
            )
        if self.event_codes and self.event_codes.has_any_filter():
            d["event_codes"] = {
                key: sorted(value)
                for key, value in self.event_codes.model_dump(exclude_none=True).items()
                if value
            }
        if self.quad_classes:
            d["quad_classes"] = sorted(self.quad_classes)
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
    date_from_sqldate: int = Field(description="Start date as YYYYMMDD integer")
    date_to_sqldate: int = Field(description="End date as YYYYMMDD integer")
    normalization_notes: str = Field(
        default="",
        description="Claude's reasoning about the normalization choices",
        max_length=1000,
    )


class NormalizedFilters(BaseModel):
    """
    Normalized search filters - backend-agnostic.

    These filters represent canonical search intent, independent of the
    underlying query engine. Use PostgresQueryCompiler for local PostgreSQL
    queries, or BigQueryCompiler for upstream ingestion queries.

    All fields are optional - omit fields to skip that filter dimension.
    """

    cameo_country_code: str | None = None
    fips_country_code: str | None = None
    geo_country_codes: list[str] = Field(default_factory=list)
    actor1_country_code: str | None = None
    actor2_country_code: str | None = None
    event_root_codes: list[str] = Field(default_factory=list)
    event_base_codes: list[str] = Field(default_factory=list)
    event_codes: list[str] = Field(default_factory=list)
    quad_classes: list[int] = Field(default_factory=list)
    source_domains: list[str] = Field(default_factory=list)
    tone_min: float | None = None
    tone_max: float | None = None
    goldstein_min: float | None = None
    goldstein_max: float | None = None
    min_mentions: int | None = None
    min_sources: int | None = None
    min_articles: int | None = None
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
