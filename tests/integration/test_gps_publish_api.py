"""Integration tests for GPS publication semantics against PostgreSQL.

Kafka is disabled for the suite, so the ``client`` fixture injects an
in-memory recording publisher (see ``tests/integration/conftest.py``). The
tests prove the API contract: ``202`` only after the publisher accepted the
event, a safe ``503`` envelope when publication fails, and unchanged
validation behaviour that never publishes invalid events.
"""

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from app.services.gps_publisher import GpsPublishError
from tests.gps_publisher_fakes import RecordingGpsEventPublisher
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


def test_202_returns_ack_after_publisher_received_event(
    client: TestClient,
    gps_publisher: RecordingGpsEventPublisher,
) -> None:
    vehicle = create_vehicle(client)
    payload = gps_payload(vehicle_id=vehicle["id"])

    response = client.post("/api/v1/locations", json=payload)

    # The response is only produced after publication, so by the time the
    # client sees 202 the publisher already holds the event.
    assert response.status_code == 202, response.text
    assert len(gps_publisher.events) == 1
    published = gps_publisher.events[0]
    assert published.event_id == UUID(payload["event_id"])  # type: ignore[arg-type]
    assert published.vehicle_id == UUID(vehicle["id"])
    assert published.shipment_id is None
    assert published.latitude == 52.52
    assert published.longitude == 13.405
    assert published.speed_kmph == 31.5
    assert published.heading == 87.5

    ack = response.json()
    assert ack == {
        "event_id": payload["event_id"],
        "vehicle_id": vehicle["id"],
        "shipment_id": None,
        "status": "accepted",
        "persisted": False,
    }


def test_event_with_shipment_is_published_with_association(
    client: TestClient,
    gps_publisher: RecordingGpsEventPublisher,
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
    assert response.json()["persisted"] is False
    assert len(gps_publisher.events) == 1
    assert gps_publisher.events[0].shipment_id == UUID(shipment["id"])


def test_publish_failure_returns_safe_503_envelope(
    client: TestClient,
    gps_publisher: RecordingGpsEventPublisher,
) -> None:
    vehicle = create_vehicle(client)
    gps_publisher.fail_with = GpsPublishError("connect to localhost:9092 refused")

    response = client.post(
        "/api/v1/locations",
        json=gps_payload(vehicle_id=vehicle["id"]),
    )

    # A delivery failure must never look like acceptance.
    assert response.status_code == 503, response.text
    assert gps_publisher.events == []
    body = response.json()
    assert body["error"]["code"] == "service_unavailable"
    assert "unavailable" in body["error"]["message"]
    assert body["error"]["details"] is None
    # Broker addresses and client internals stay server-side only.
    assert "localhost" not in body["error"]["message"]
    assert "9092" not in body["error"]["message"]


def test_validation_failures_never_reach_the_publisher(
    client: TestClient,
    gps_publisher: RecordingGpsEventPublisher,
) -> None:
    gps_publisher.fail_with = GpsPublishError("broker down")

    # Unknown vehicle -> 404 even though the publisher would fail: the
    # validation order (validate -> publish) is unchanged.
    response = client.post("/api/v1/locations", json=gps_payload())
    assert response.status_code == 404
    assert gps_publisher.events == []

    vehicle = create_vehicle(client, status="inactive")
    response = client.post("/api/v1/locations", json=gps_payload(vehicle_id=vehicle["id"]))
    assert response.status_code == 409
    assert gps_publisher.events == []

    active_vehicle = create_vehicle(client)
    response = client.post(
        "/api/v1/locations",
        json=gps_payload(vehicle_id=active_vehicle["id"], shipment_id=str(uuid4())),
    )
    assert response.status_code == 404
    assert response.json()["error"]["message"] == "Shipment not found."
    assert gps_publisher.events == []


def test_malformed_event_is_rejected_before_publication(
    client: TestClient,
    gps_publisher: RecordingGpsEventPublisher,
) -> None:
    payload = gps_payload(latitude=120.0)
    payload["speed_kmph"] = -1.0

    response = client.post("/api/v1/locations", json=payload)

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    assert gps_publisher.events == []
