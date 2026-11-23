"""Shared fixtures for PostgreSQL integration tests."""

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from app.api.dependencies import get_gps_publisher
from app.main import create_app
from tests.gps_publisher_fakes import RecordingGpsEventPublisher


@pytest.fixture
def gps_publisher() -> RecordingGpsEventPublisher:
    """Application-scoped publisher double recording delivered events."""
    return RecordingGpsEventPublisher()


@pytest.fixture
def client(gps_publisher: RecordingGpsEventPublisher) -> Iterator[TestClient]:
    """Return a client with application lifespan and a real database engine.

    Kafka is disabled for the suite (see ``tests/conftest.py``), so the GPS
    publisher dependency is overridden with an in-memory recorder: the API
    contract (202 after publication) stays testable without a broker.
    """
    app = create_app()
    app.dependency_overrides[get_gps_publisher] = lambda: gps_publisher
    try:
        with TestClient(app) as test_client:
            yield test_client
    finally:
        app.dependency_overrides.clear()
