"""Tests for the root, liveness and readiness endpoints."""

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncEngine

from app.api.routes import health as health_routes
from app.core.config import get_settings
from app.main import app


@pytest.fixture
def client() -> Iterator[TestClient]:
    """Return an HTTP client with application startup and shutdown handled."""
    with TestClient(app) as test_client:
        yield test_client


def test_root_returns_api_information(client: TestClient) -> None:
    settings = get_settings()

    response = client.get("/")

    assert response.status_code == 200
    assert response.json() == {
        "name": settings.app_name,
        "version": settings.app_version,
        "environment": settings.app_env,
        "docs_url": "/docs" if settings.is_development else None,
    }


def test_liveness_reports_process_is_alive(client: TestClient) -> None:
    response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readiness_reports_ready_when_database_is_available(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def database_is_available(_engine: AsyncEngine) -> bool:
        return True

    monkeypatch.setattr(health_routes, "check_database", database_is_available)

    response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "checks": {"database": "ok"}}


def test_readiness_reports_unavailable_when_database_is_down(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def database_is_unavailable(_engine: AsyncEngine) -> bool:
        return False

    monkeypatch.setattr(health_routes, "check_database", database_is_unavailable)

    response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "checks": {"database": "unavailable"},
    }


def test_openapi_documentation_matches_environment(client: TestClient) -> None:
    settings = get_settings()

    response = client.get("/openapi.json")

    if settings.is_development:
        assert response.status_code == 200
        assert response.json()["info"]["title"] == settings.app_name
        assert response.json()["info"]["version"] == settings.app_version
    else:
        assert response.status_code == 404
