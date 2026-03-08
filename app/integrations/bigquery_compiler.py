"""BigQuery query compiler for upstream ingestion."""

from __future__ import annotations

from google.cloud import bigquery

from app.schemas.filters import NormalizedFilters


# Columns selected for ingestion queries
GDELT_SELECT_COLUMNS = [
    "GLOBALEVENTID",
    "SQLDATE",
    "DATEADDED",
    "Actor1CountryCode",
    "Actor2CountryCode",
    "EventCode",
    "EventBaseCode",
    "EventRootCode",
    "QuadClass",
    "GoldsteinScale",
    "AvgTone",
    "NumMentions",
    "NumSources",
    "NumArticles",
    "ActionGeo_FullName",
    "ActionGeo_CountryCode",
    "SOURCEURL",
]


class BigQueryCompiler:
    """
    Compiles NormalizedFilters to BigQuery queries.

    This compiler is used for ingestion-side queries (fetching from BigQuery
    to populate the local store). For runtime search, use PostgresQueryCompiler.
    """

    def compile(
        self,
        filters: NormalizedFilters,
    ) -> tuple[str, list[bigquery.ScalarQueryParameter]]:
        """Compile filters to BigQuery SQL and parameters."""
        conditions = self._build_conditions(filters)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        query = f"""
        SELECT {", ".join(GDELT_SELECT_COLUMNS)}
        FROM `gdelt-bq.gdeltv2.events`
        {where_clause}
        ORDER BY DATEADDED DESC
        LIMIT @limit OFFSET @offset
        """

        params = self._build_params(filters)
        params.extend(
            [
                bigquery.ScalarQueryParameter("limit", "INT64", 100),
                bigquery.ScalarQueryParameter("offset", "INT64", 0),
            ]
        )

        return query, params

    def _build_conditions(self, filters: NormalizedFilters) -> list[str]:
        """Build WHERE conditions for BigQuery."""
        conditions: list[str] = []

        if filters.date_from_sqldate:
            conditions.append("SQLDATE >= @date_from")
        if filters.date_to_sqldate:
            conditions.append("SQLDATE <= @date_to")

        if filters.actor1_country_code:
            if len(filters.actor1_country_code) == 1:
                conditions.append("Actor1CountryCode = @actor1_country")
            else:
                conditions.append("Actor1CountryCode IN UNNEST(@actor1_country)")
        if filters.actor2_country_code:
            if len(filters.actor2_country_code) == 1:
                conditions.append("Actor2CountryCode = @actor2_country")
            else:
                conditions.append("Actor2CountryCode IN UNNEST(@actor2_country)")
        if filters.geo_country_codes:
            if len(filters.geo_country_codes) == 1:
                conditions.append("ActionGeo_CountryCode = @action_geo_country")
            else:
                conditions.append("ActionGeo_CountryCode IN UNNEST(@action_geo_country)")

        if filters.event_codes:
            if len(filters.event_codes) == 1:
                conditions.append("EventCode = @event_code")
            else:
                conditions.append("EventCode IN UNNEST(@event_code)")
        if filters.event_root_codes:
            if len(filters.event_root_codes) == 1:
                conditions.append("EventRootCode = @event_root_code")
            else:
                conditions.append("EventRootCode IN UNNEST(@event_root_code)")
        if filters.quad_classes:
            if len(filters.quad_classes) == 1:
                conditions.append("QuadClass = @quad_class")
            else:
                conditions.append("QuadClass IN UNNEST(@quad_class)")

        if filters.goldstein_min is not None:
            conditions.append("GoldsteinScale >= @goldstein_min")
        if filters.goldstein_max is not None:
            conditions.append("GoldsteinScale <= @goldstein_max")

        if filters.tone_min is not None:
            conditions.append("AvgTone >= @tone_min")
        if filters.tone_max is not None:
            conditions.append("AvgTone <= @tone_max")

        if filters.min_mentions is not None:
            conditions.append("NumMentions >= @min_mentions")
        if filters.min_sources is not None:
            conditions.append("NumSources >= @min_sources")
        if filters.min_articles is not None:
            conditions.append("NumArticles >= @min_articles")

        return conditions

    def _build_params(
        self,
        filters: NormalizedFilters,
    ) -> list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter]:
        """Build query parameters."""
        params: list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter] = []

        if filters.date_from_sqldate:
            params.append(
                bigquery.ScalarQueryParameter("date_from", "INT64", filters.date_from_sqldate)
            )
        if filters.date_to_sqldate:
            params.append(
                bigquery.ScalarQueryParameter("date_to", "INT64", filters.date_to_sqldate)
            )

        if filters.actor1_country_code:
            if len(filters.actor1_country_code) == 1:
                params.append(
                    bigquery.ScalarQueryParameter(
                        "actor1_country", "STRING", filters.actor1_country_code[0]
                    )
                )
            else:
                params.append(
                    bigquery.ArrayQueryParameter(
                        "actor1_country", "STRING", filters.actor1_country_code
                    )
                )
        if filters.actor2_country_code:
            if len(filters.actor2_country_code) == 1:
                params.append(
                    bigquery.ScalarQueryParameter(
                        "actor2_country", "STRING", filters.actor2_country_code[0]
                    )
                )
            else:
                params.append(
                    bigquery.ArrayQueryParameter(
                        "actor2_country", "STRING", filters.actor2_country_code
                    )
                )
        if filters.geo_country_codes:
            if len(filters.geo_country_codes) == 1:
                params.append(
                    bigquery.ScalarQueryParameter(
                        "action_geo_country", "STRING", filters.geo_country_codes[0]
                    )
                )
            else:
                params.append(
                    bigquery.ArrayQueryParameter(
                        "action_geo_country", "STRING", filters.geo_country_codes
                    )
                )

        if filters.event_codes:
            if len(filters.event_codes) == 1:
                params.append(
                    bigquery.ScalarQueryParameter("event_code", "STRING", filters.event_codes[0])
                )
            else:
                params.append(
                    bigquery.ArrayQueryParameter("event_code", "STRING", filters.event_codes)
                )
        if filters.event_root_codes:
            if len(filters.event_root_codes) == 1:
                params.append(
                    bigquery.ScalarQueryParameter(
                        "event_root_code", "STRING", filters.event_root_codes[0]
                    )
                )
            else:
                params.append(
                    bigquery.ArrayQueryParameter(
                        "event_root_code", "STRING", filters.event_root_codes
                    )
                )
        if filters.quad_classes:
            if len(filters.quad_classes) == 1:
                params.append(
                    bigquery.ScalarQueryParameter("quad_class", "INT64", filters.quad_classes[0])
                )
            else:
                params.append(
                    bigquery.ArrayQueryParameter("quad_class", "INT64", filters.quad_classes)
                )

        if filters.goldstein_min is not None:
            params.append(
                bigquery.ScalarQueryParameter("goldstein_min", "FLOAT64", filters.goldstein_min)
            )
        if filters.goldstein_max is not None:
            params.append(
                bigquery.ScalarQueryParameter("goldstein_max", "FLOAT64", filters.goldstein_max)
            )

        if filters.tone_min is not None:
            params.append(bigquery.ScalarQueryParameter("tone_min", "FLOAT64", filters.tone_min))
        if filters.tone_max is not None:
            params.append(bigquery.ScalarQueryParameter("tone_max", "FLOAT64", filters.tone_max))

        if filters.min_mentions is not None:
            params.append(
                bigquery.ScalarQueryParameter("min_mentions", "INT64", filters.min_mentions)
            )
        if filters.min_sources is not None:
            params.append(
                bigquery.ScalarQueryParameter("min_sources", "INT64", filters.min_sources)
            )
        if filters.min_articles is not None:
            params.append(
                bigquery.ScalarQueryParameter("min_articles", "INT64", filters.min_articles)
            )

        return params
