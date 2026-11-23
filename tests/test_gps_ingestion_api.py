"""Unit tests for the GPS ingestion endpoint wiring (no PostgreSQL required)."""

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client() -> Iterator[TestClient]:
    with TestClient(app) as test_client:
        yield test_client


def test_invalid_gps_payload_returns_consistent_422(client: TestClient) -> None:
    response = client.post(
        "/api/v1/locations",
        json={
            "event_id": "not-a-uuid",
            "vehicle_id": "0b6f2c9a-1d2e-4f3a-8b7c-9d0e1f2a3b4c",
            "latitude": 120.0,
            "longitude": 13.405,
            "speed_kmph": -1.0,
            "recorded_at": "2026-01-01T12:00:00",
        },
    )

    assert response.status_code == 422
    body = response.json()
    assert body["error"]["code"] == "validation_error"
    assert body["error"]["message"] == "Request validation failed."
    details = body["error"]["details"]
    assert isinstance(details, list)
    reported_locations = {tuple(detail["location"]) for detail in details}
    assert ("body", "event_id") in reported_locations
    assert ("body", "latitude") in reported_locations
    assert ("body", "recorded_at") in reported_locations


def test_unknown_field_in_gps_payload_is_rejected(client: TestClient) -> None:
    response = client.post(
        "/api/v1/locations",
        json={
            "event_id": "6f1c2b1e-9f7a-4b1a-9c2d-3a4b5c6d7e8f",
            "vehicle_id": "0b6f2c9a-1d2e-4f3a-8b7c-9d0e1f2a3b4c",
            "latitude": 52.52,
            "longitude": 13.405,
            "speed_kmph": 30.0,
            "recorded_at": "2026-01-01T12:00:00+00:00",
            "unexpected": True,
        },
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


def test_openapi_exposes_locations_ingestion(client: TestClient) -> None:
    response = client.get("/openapi.json")

    assert response.status_code == 200
    operation = response.json()["paths"]["/api/v1/locations"]["post"]
    assert "202" in operation["responses"]
    assert operation["requestBody"]["content"]["application/json"]["schema"]["$ref"].endswith(
        "GpsEvent"
    )
