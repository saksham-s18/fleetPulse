"""Integration tests that require the local PostgreSQL service.

Run with:

    docker compose up -d postgres
    pytest -m integration
"""

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text

from app.core.config import Settings, get_settings
from app.infrastructure.database import create_engine, create_session_factory
from app.main import create_app

pytestmark = pytest.mark.integration


def test_readiness_returns_ready_with_postgresql() -> None:
    application = create_app()

    with TestClient(application) as client:
        response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "checks": {"database": "ok"}}


@pytest.mark.asyncio
async def test_session_can_execute_query_against_postgresql() -> None:
    engine = create_engine(get_settings())
    session_factory = create_session_factory(engine)

    try:
        async with session_factory() as session:
            result = await session.execute(text("SELECT 1"))
            assert result.scalar_one() == 1
    finally:
        await engine.dispose()


def test_readiness_returns_503_when_database_is_unreachable() -> None:
    settings = Settings(
        database_url=SecretStr("postgresql+asyncpg://fleetpulse:fleetpulse@127.0.0.1:1/fleetpulse"),
        db_connect_timeout_seconds=1.0,
    )
    application = create_app(settings)

    with TestClient(application) as client:
        response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "checks": {"database": "unavailable"},
    }
