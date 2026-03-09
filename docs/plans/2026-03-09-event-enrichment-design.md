# Event Enrichment Design

Date: 2026-03-09  
Status: Approved

## Problem

Event records need richer article metadata for downstream consumers, but enrichment must remain bounded, deterministic, and simple to operate.

## Goal

Add database-backed enrichment that stores only `article_title`, `article_summary`, and `sources`, without persisting full article content.

## Architecture

Enrichment is a DB-only backend capability. On a 30-minute schedule, the backend selects events needing enrichment, performs deterministic article fetch + extraction, and then calls a separate internal enrichment service. That service does not fetch HTML; it returns exactly `article_title`, `article_summary`, and `sources`, which are written back to the event record.

## Components

- Scheduler trigger running every 30 minutes
- Backend enrichment workflow for deterministic fetch + extraction
- Separate internal enrichment service returning `article_title`, `article_summary`, and `sources`
- Event lifecycle state tracking: `pending`, `processing`, `enriched`, `failed`
- Database fields for `article_title`, `article_summary`, and `sources`

## Data Flow

1. Events requiring enrichment are marked `pending`.
2. The scheduled job selects pending records and moves them to `processing`.
3. The backend fetches the article HTML and performs deterministic extraction.
4. The backend calls the internal enrichment service with the extracted input.
5. The service returns exactly `article_title`, `article_summary`, and `sources`.
6. The event is updated to `enriched` on success or `failed` on terminal failure.

## Database Changes

- Add enrichment state field with values `pending`, `processing`, `enriched`, `failed`
- Add `article_title`
- Add `article_summary`
- Add `sources` stored as a JSON array of strings

## Failure And Retry Rules

Processing starts from `pending`, is locked as `processing`, and ends as `enriched` or `failed`. Retry behavior is driven by the 30-minute scheduled job. Terminal failures remain `failed` until explicitly re-queued by backend logic or future operational tooling.

## Out Of Scope

- Persisting full article bodies or raw HTML
- HTML fetching inside the internal enrichment service
- Client-side fetching or extraction
- Non-deterministic enrichment pipelines
- Public enrichment endpoints

## Notes

This design keeps enrichment intentionally narrow: database updates only, deterministic fetch + extraction in the backend, and a separate internal enrichment service that returns only `article_title`, `article_summary`, and `sources`.
