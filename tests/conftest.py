"""
Shared test fixtures.

Uses pytest-asyncio with asyncio_mode = "auto" (set in pyproject.toml).
All DB tests use an in-memory SQLite database via SQLAlchemy async engine.
BigQuery and Anthropic clients are always mocked.
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Set required env vars before importing app modules
os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/fake-key.json")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("ANTHROPIC_API_KEY", "sk-ant-test")
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///./test.db")
os.environ.setdefault("API_KEY", "test-api-key")
os.environ.setdefault("CORS_ORIGINS", '["http://localhost:3000"]')

from app.db.models import Base
from app.db.session import get_async_session
from app.main import create_app


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def db_engine():
    """Create an in-memory async SQLite engine for testing."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.fixture
async def db_session(db_engine):
    """Yield a test database session."""
    factory = async_sessionmaker(
        bind=db_engine, class_=AsyncSession, expire_on_commit=False
    )
    async with factory() as session:
        yield session
        await session.rollback()


@pytest.fixture
def mock_bq_client():
    """Mock BigQuery client that returns empty results by default."""
    client = MagicMock()
    client.run_query = AsyncMock(return_value=[])
    client.run_query_single_value = AsyncMock(return_value=None)
    client.shutdown = MagicMock()
    return client


@pytest.fixture
def mock_anthropic_client():
    """Mock Anthropic async client."""
    client = AsyncMock()
    return client


@pytest.fixture
def app(db_session, mock_bq_client, mock_anthropic_client):
    """Create a test FastAPI app with mocked dependencies."""
    test_app = create_app()

    # Override dependencies
    async def override_get_db():
        yield db_session

    test_app.dependency_overrides[get_async_session] = override_get_db
    test_app.state.bq_client = mock_bq_client
    test_app.state.anthropic_client = mock_anthropic_client
    test_app.state.scheduler = MagicMock(running=True)

    return test_app


@pytest.fixture
async def async_client(app):
    """Async HTTP client for testing endpoints."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
def api_headers():
    """Headers with valid API key."""
    return {"X-API-Key": "test-api-key"}
