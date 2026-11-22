"""Shared helpers for API integration tests."""

from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import uuid4

from fastapi.testclient import TestClient
from httpx2 import Response


def unique(prefix: str) -> str:
    """Return a unique value so tests do not depend on an empty database."""
    return f"{prefix}-{uuid4().hex[:12]}"


def create_vehicle(
    client: TestClient,
    *,
    registration_number: str | None = None,
    vehicle_type: str | None = None,
    status: str = "active",
    device_id: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "registration_number": registration_number or unique("REG"),
        "vehicle_type": vehicle_type or "van",
        "status": status,
    }
    if device_id is not None:
        payload["device_id"] = device_id

    response = client.post("/api/v1/vehicles", json=payload)
    assert response.status_code == 201, response.text
    return cast(dict[str, Any], response.json())


def create_driver(
    client: TestClient,
    *,
    full_name: str | None = None,
    license_number: str | None = None,
    phone: str | None = None,
    status: str = "active",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "full_name": full_name or "Test Driver",
        "license_number": license_number or unique("LIC"),
        "status": status,
    }
    if phone is not None:
        payload["phone"] = phone

    response = client.post("/api/v1/drivers", json=payload)
    assert response.status_code == 201, response.text
    return cast(dict[str, Any], response.json())


def create_shipment(
    client: TestClient,
    *,
    reference: str | None = None,
    expected_delivery_at: datetime | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "reference": reference or unique("SHP"),
        "origin": {
            "name": "Berlin",
            "latitude": 52.52,
            "longitude": 13.405,
        },
        "destination": {
            "name": "Hamburg",
            "latitude": 53.551,
            "longitude": 9.993,
        },
        "expected_delivery_at": (
            expected_delivery_at or datetime.now(UTC) + timedelta(hours=4)
        ).isoformat(),
    }

    response = client.post("/api/v1/shipments", json=payload)
    assert response.status_code == 201, response.text
    return cast(dict[str, Any], response.json())


def post_assignment(
    client: TestClient,
    vehicle_id: str,
    driver_id: str,
    shipment_id: str,
) -> Response:
    return client.post(
        "/api/v1/assignments",
        json={
            "vehicle_id": vehicle_id,
            "driver_id": driver_id,
            "shipment_id": shipment_id,
        },
    )
