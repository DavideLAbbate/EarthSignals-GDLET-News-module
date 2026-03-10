import os
import sys
import asyncio

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


async def main():
    since = int(sys.argv[1]) if len(sys.argv) > 1 else 20260308
    factory = _get_session_factory()
    async with factory() as session:
        count = await ClusterService(session).build_and_materialise(since)
        await session.commit()
        print(f"Materialised {count} clusters")


asyncio.run(main())
