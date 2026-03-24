"""Manual CLI entrypoint for cluster materialisation."""

from __future__ import annotations

import asyncio
import os
import sys

# Load .env and ensure asyncpg driver before any app.* imports
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# Normalise DATABASE_URL to use asyncpg driver
_raw_url = (
    os.environ.get("DATABASE_URL")
    or os.environ.get("POSTGRES_URL")
    or os.environ.get("NEON_URL")
    or ""
)
for _sync_prefix in ("postgresql://", "postgres://"):
    if _raw_url.startswith(_sync_prefix):
        _raw_url = "postgresql+asyncpg://" + _raw_url[len(_sync_prefix) :]
        break
if _raw_url:
    os.environ["DATABASE_URL"] = _raw_url

from app.db.session import _get_session_factory  # noqa: E402
from app.services.cluster_service import ClusterService  # noqa: E402


async def main() -> None:
    since = int(sys.argv[1]) if len(sys.argv) > 1 else 20260308
    until = int(sys.argv[2]) if len(sys.argv) > 2 else None
    factory = _get_session_factory()
    async with factory() as session:
        count = await ClusterService(session).build_and_materialise(since, until)
        await session.commit()
        print(f"Materialised {count} clusters")


if __name__ == "__main__":
    asyncio.run(main())
