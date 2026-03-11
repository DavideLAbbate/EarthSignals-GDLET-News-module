# Bootstrap Range Design

## Goal

Add a manual CLI bootstrap that ingests GDELT `export`, `mentions`, and `gkg`
CSV archives over a user-supplied time range, without relying on BigQuery.

## Context

The current bootstrap path already uses the HTTP CSV pipeline, but its range is
derived only from `retention_days` and it is not exposed as a manual backfill
tool. The desired behavior is a dedicated CLI that accepts two timestamps,
supports both date-only and full `DATEADDED` precision, and fills all three
local GDELT tables.

## Decisions

- Add a dedicated CLI entrypoint for manual backfills rather than exposing this
  through the API or overloading scheduler startup bootstrap behavior.
- Extract a shared range-based bootstrap service so automatic bootstrap can keep
  working by delegating to the same ingestion core.
- Keep the HTTP CSV ingestion path for all three datasets and remove any
  bootstrap-specific dependency on BigQuery.
- Accept both `YYYYMMDD` and `YYYYMMDDHHMMSS` on the CLI.
- Normalize `start` date-only values to `000000` and `end` date-only values to
  `235959`.
- Validate `start <= end` before opening the DB session.
- Process export files chronologically and, for each export timestamp, ingest
  the matching mentions and GKG files when present.
- Preserve best-effort semantics for mentions and GKG failures so a secondary
  file issue does not roll back already committed event data.
- Add explicit chunking to event inserts in the range bootstrap path so large
  fetched CSV batches do not build oversized statements.

## Data Flow

1. User runs `python run_bootstrap_range.py <start> <end>`.
2. CLI normalizes the two inputs into inclusive `DATEADDED` timestamps.
3. Service fetches `masterfilelist.txt` export URLs inside the range.
4. For each export file timestamp:
   - download and insert events in chunks
   - download and insert matching mentions in chunks
   - download and insert matching GKG rows in chunks
5. Service updates the ingestion run watermark to the normalized end timestamp.
6. CLI prints final inserted counts.

## Error Handling

- Invalid CLI timestamps fail fast with a clear argument error.
- Event download/insert failures fail the ingestion run.
- Mentions and GKG failures stay best-effort: rollback that sub-batch, log, and
  continue.
- Empty ranges complete successfully with zero inserted rows.

## Verification

- Add service tests for range normalization and explicit range ingestion.
- Add CLI tests for accepted input formats and invalid ranges.
- Verify all three tables are populated for a range run.
- Verify chunked insertion is used for events, mentions, and GKG rows.
