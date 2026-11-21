"""Tests for the root, liveness and readiness endpoints."""

import pytest
from fastapi.testclient import TestClient

from app.core.config import get_settings
from app.main import app


@pytest.fixture
def client() -> TestClient:
    """Return an HTTP client bound to the application under test."""
    return TestClient(app)


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


def test_readiness_reports_configuration_check(client: TestClient) -> None:
    settings = get_settings()

    response = client.get("/health/ready")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ready"
    assert payload["checks"]["configuration"] == f"ok ({settings.app_env})"


def test_openapi_documentation_matches_environment(client: TestClient) -> None:
    settings = get_settings()

    response = client.get("/openapi.json")

    if settings.is_development:
        assert response.status_code == 200
        assert response.json()["info"]["title"] == settings.app_name
        assert response.json()["info"]["version"] == settings.app_version
    else:
        assert response.status_code == 404
