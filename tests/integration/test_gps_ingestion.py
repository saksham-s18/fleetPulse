"""Integration tests for the GPS ingestion endpoint against PostgreSQL."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from tests.integration.helpers import (
    create_driver,
    create_shipment,
    create_vehicle,
    post_assignment,
)

pytestmark = pytest.mark.integration


def gps_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "event_id": str(uuid4()),
        "vehicle_id": str(uuid4()),
        "latitude": 52.52,
        "longitude": 13.405,
        "speed_kmph": 31.5,
        "heading": 87.5,
        "recorded_at": datetime(2026, 1, 1, 12, 0, tzinfo=UTC).isoformat(),
    }
    payload.update(overrides)
    return payload


def test_valid_event_without_shipment_is_accepted(client: TestClient) -> None:
    vehicle = create_vehicle(client)
    event_id = str(uuid4())

    response = client.post(
        "/api/v1/locations",
        json=gps_payload(event_id=event_id, vehicle_id=vehicle["id"]),
    )

    assert response.status_code == 202, response.text
    body = response.json()
    assert body["event_id"] == event_id
    assert body["vehicle_id"] == vehicle["id"]
    assert body["shipment_id"] is None
    assert body["status"] == "accepted"
    assert body["persisted"] is False


def test_valid_event_with_active_vehicle_shipment_assignment_is_accepted(
    client: TestClient,
) -> None:
    vehicle = create_vehicle(client)
    driver = create_driver(client)
    shipment = create_shipment(client)
    assignment = post_assignment(client, vehicle["id"], driver["id"], shipment["id"])
    assert assignment.status_code == 201, assignment.text

    response = client.post(
        "/api/v1/locations",
        json=gps_payload(vehicle_id=vehicle["id"], shipment_id=shipment["id"]),
    )

    assert response.status_code == 202, response.text
    assert response.json()["shipment_id"] == shipment["id"]
    assert response.json()["persisted"] is False


def test_unknown_vehicle_is_rejected(client: TestClient) -> None:
    response = client.post("/api/v1/locations", json=gps_payload())

    assert response.status_code == 404
    assert response.json()["error"] == {
        "code": "not_found",
        "message": "Vehicle not found.",
        "details": None,
    }


def test_inactive_vehicle_is_rejected(client: TestClient) -> None:
    vehicle = create_vehicle(client, status="inactive")

    response = client.post(
        "/api/v1/locations",
        json=gps_payload(vehicle_id=vehicle["id"]),
    )

    assert response.status_code == 409
    body = response.json()
    assert body["error"]["code"] == "conflict"
    assert "not active" in body["error"]["message"]


def test_unknown_shipment_is_rejected(client: TestClient) -> None:
    vehicle = create_vehicle(client)

    response = client.post(
        "/api/v1/locations",
        json=gps_payload(vehicle_id=vehicle["id"], shipment_id=str(uuid4())),
    )

    assert response.status_code == 404
    assert response.json()["error"]["message"] == "Shipment not found."


def test_shipment_not_on_vehicle_active_assignment_is_rejected(client: TestClient) -> None:
    vehicle = create_vehicle(client)
    driver = create_driver(client)
    shipment = create_shipment(client)
    other_shipment = create_shipment(client)

    # The vehicle has no active assignment at all.
    response = client.post(
        "/api/v1/locations",
        json=gps_payload(vehicle_id=vehicle["id"], shipment_id=other_shipment["id"]),
    )
    assert response.status_code == 409
    assert "active assignment" in response.json()["error"]["message"]

    assignment = post_assignment(client, vehicle["id"], driver["id"], shipment["id"])
    assert assignment.status_code == 201, assignment.text

    # The vehicle's active assignment points at a different shipment.
    response = client.post(
        "/api/v1/locations",
        json=gps_payload(vehicle_id=vehicle["id"], shipment_id=other_shipment["id"]),
    )
    assert response.status_code == 409

    # Ending the assignment leaves no current assignment to report against.
    ended = client.post(f"/api/v1/assignments/{assignment.json()['id']}/end")
    assert ended.status_code == 200, ended.text

    response = client.post(
        "/api/v1/locations",
        json=gps_payload(vehicle_id=vehicle["id"], shipment_id=shipment["id"]),
    )
    assert response.status_code == 409


def test_shipment_assigned_to_another_vehicle_is_rejected(client: TestClient) -> None:
    vehicle = create_vehicle(client)
    other_vehicle = create_vehicle(client)
    other_driver = create_driver(client)
    shipment = create_shipment(client)
    assignment = post_assignment(
        client,
        other_vehicle["id"],
        other_driver["id"],
        shipment["id"],
    )
    assert assignment.status_code == 201, assignment.text

    response = client.post(
        "/api/v1/locations",
        json=gps_payload(vehicle_id=vehicle["id"], shipment_id=shipment["id"]),
    )

    assert response.status_code == 409
    assert "active assignment" in response.json()["error"]["message"]
