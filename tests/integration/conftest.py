"""Shared fixtures for PostgreSQL integration tests."""

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from app.main import create_app


@pytest.fixture
def client() -> Iterator[TestClient]:
    """Return a client with application lifespan and a real database engine."""
    with TestClient(create_app()) as test_client:
        yield test_client
