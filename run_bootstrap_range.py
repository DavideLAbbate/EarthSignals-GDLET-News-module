"""Run bootstrap ingestion for an explicit DATEADDED timestamp range."""

from __future__ import annotations

import asyncio
from datetime import datetime
import os
import sys


def _parse_timestamp_argument(raw_value: str, *, is_end: bool) -> int:
    """Parse a YYYYMMDD or YYYYMMDDHHMMSS CLI argument into a DATEADDED timestamp."""
    if not raw_value.isdigit() or len(raw_value) not in {8, 14}:
        raise ValueError(f"Invalid timestamp: {raw_value}")

    normalized_value = (
        raw_value + ("235959" if is_end else "000000") if len(raw_value) == 8 else raw_value
    )

    try:
        parsed = datetime.strptime(normalized_value, "%Y%m%d%H%M%S")
    except ValueError as exc:
        raise ValueError(f"Invalid timestamp: {raw_value}") from exc

    if parsed.strftime("%Y%m%d%H%M%S") != normalized_value:
        raise ValueError(f"Invalid timestamp: {raw_value}")

    return int(normalized_value)


def parse_bootstrap_range(start: str, end: str) -> tuple[int, int]:
    """Parse and validate inclusive bootstrap range CLI arguments."""
    start_ts = _parse_timestamp_argument(start, is_end=False)
    end_ts = _parse_timestamp_argument(end, is_end=True)

    if start_ts > end_ts:
        raise ValueError("Bootstrap start timestamp must be less than or equal to end timestamp")

    return start_ts, end_ts


def _prepare_runtime_environment() -> None:
    """Load runtime env and normalize DATABASE_URL for async SQLAlchemy startup."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        load_dotenv = None

    if load_dotenv is not None:
        load_dotenv()

    raw_url = (
        os.environ.get("DATABASE_URL")
        or os.environ.get("POSTGRES_URL")
        or os.environ.get("NEON_URL")
        or ""
    )
    for sync_prefix in ("postgresql://", "postgres://"):
        if raw_url.startswith(sync_prefix):
            raw_url = "postgresql+asyncpg://" + raw_url[len(sync_prefix) :]
            break

    if raw_url:
        os.environ["DATABASE_URL"] = raw_url


async def main(argv: list[str] | None = None) -> int:
    """Parse CLI arguments and run bootstrap ingestion for the requested range."""
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 2:
        raise SystemExit("Usage: python run_bootstrap_range.py <start> <end>")

    since_ts, until_ts = parse_bootstrap_range(args[0], args[1])

    _prepare_runtime_environment()

    from app.db.session import _get_session_factory
    from app.services.ingestion_service import run_bootstrap_range as execute_bootstrap_range

    factory = _get_session_factory()
    async with factory() as session:
        result = await execute_bootstrap_range(session, since_ts=since_ts, until_ts=until_ts)
        await session.commit()

    print(
        "Bootstrap completed for range "
        f"{since_ts}..{until_ts} (events_ingested={result['events_ingested']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
