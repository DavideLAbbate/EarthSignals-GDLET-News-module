# Event Enrichment Design

Date: 2026-03-09  
Status: Approved

## Goal

Expand the existing event enrichment milestone to persist a broader semantic payload while keeping the current backend-driven enrichment pipeline, scheduler, and deterministic fetch-and-extract flow unchanged.

## New Storage Model

Approved storage approach is option 1: store stable top-level fields as dedicated columns on `gdelt_events`, and store only the entity breakdown as a single JSON field.

Persist the following enrichment fields on `gdelt_events`:

- `article_title`
- `article_summary`
- `cited_sources` (JSON array)
- `main_topics` (JSON array)
- `keywords` (JSON array)
- `entities` (JSON object)
- existing technical fields `enrichment_status`, `enriched_at`, `enrichment_error`

The previous `sources` field is renamed to `cited_sources` to better reflect that the values represent sources cited by the article, not source ingestion metadata.

## Internal Service Response Contract

The internal enrichment service continues to receive extracted article input from the backend and must now return the following payload shape:

```json
{
  "article_title": "string | null",
  "article_summary": "string | null",
  "cited_sources": ["string"],
  "main_topics": ["string"],
  "keywords": ["string"],
  "entities": {
    "persons_cited": ["string"],
    "organizations_cited": ["string"],
    "locations": ["string"],
    "ethnicities_cited": ["string"],
    "religions_cited": ["string"],
    "occupations_cited": ["string"],
    "political_affiliations_cited": ["string"],
    "industries_cited": ["string"],
    "products_cited": ["string"],
    "brands_cited": ["string"]
  }
}
```

This payload remains bounded and article-level. Full article content, raw extraction output, and intermediate model artifacts are still not persisted.

## Migration Impact

- add new columns for `cited_sources`, `main_topics`, `keywords`, and `entities`
- rename the existing `sources` column to `cited_sources`
- update the enrichment write path, read models, and any internal serializers to use the renamed field and expanded payload
- preserve existing enrichment lifecycle handling through `enrichment_status`, `enriched_at`, and `enrichment_error`

No scheduler redesign or pipeline restructuring is required; this is a schema and contract expansion on top of the current enrichment milestone.

## Out Of Scope

- changing how the scheduler selects or retries events
- moving HTML fetching or extraction into the internal enrichment service
- persisting full article bodies, raw HTML, or model reasoning output
- introducing public enrichment endpoints or client-facing ingestion changes
