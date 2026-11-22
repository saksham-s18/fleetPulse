"""Integration tests for the core fleet REST APIs against PostgreSQL."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from tests.integration.helpers import (
    create_driver,
    create_shipment,
    create_vehicle,
    post_assignment,
    unique,
)

pytestmark = pytest.mark.integration


def test_vehicle_crud_pagination_and_filtering(client: TestClient) -> None:
    vehicle_type = unique("type")
    created = [create_vehicle(client, vehicle_type=vehicle_type) for _ in range(3)]
    created_ids = {vehicle["id"] for vehicle in created}

    response = client.get("/api/v1/vehicles", params={"vehicle_type": vehicle_type})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert body["pages"] == 1
    assert {item["id"] for item in body["items"]} == created_ids

    response = client.get(
        "/api/v1/vehicles",
        params={"vehicle_type": vehicle_type.upper()},
    )
    assert response.json()["total"] == 3

    response = client.get(
        "/api/v1/vehicles",
        params={"vehicle_type": vehicle_type, "page": 1, "page_size": 2},
    )
    body = response.json()
    assert len(body["items"]) == 2
    assert body["total"] == 3
    assert body["pages"] == 2

    response = client.get(
        "/api/v1/vehicles",
        params={"vehicle_type": vehicle_type, "page": 2, "page_size": 2},
    )
    body = response.json()
    assert len(body["items"]) == 1
    assert body["page"] == 2

    response = client.get(
        "/api/v1/vehicles",
        params={"vehicle_type": vehicle_type, "status": "active"},
    )
    body = response.json()
    assert body["total"] == 3
    assert all(item["status"] == "active" for item in body["items"])

    target = created[0]
    response = client.get(f"/api/v1/vehicles/{target['id']}")
    assert response.status_code == 200
    assert response.json()["registration_number"] == target["registration_number"]

    new_type = unique("type")
    response = client.patch(
        f"/api/v1/vehicles/{target['id']}",
        json={"vehicle_type": new_type, "device_id": None},
    )
    assert response.status_code == 200
    assert response.json()["vehicle_type"] == new_type
    assert response.json()["device_id"] is None

    response = client.patch(
        f"/api/v1/vehicles/{target['id']}/status",
        json={"status": "maintenance"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "maintenance"

    response = client.patch(
        f"/api/v1/vehicles/{target['id']}/status",
        json={"status": "flying"},
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


def test_vehicle_duplicates_and_not_found(client: TestClient) -> None:
    vehicle = create_vehicle(client)

    response = client.post(
        "/api/v1/vehicles",
        json={
            "registration_number": vehicle["registration_number"],
            "vehicle_type": "truck",
        },
    )
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "conflict"
    assert "registration number" in response.json()["error"]["message"]

    response = client.post(
        "/api/v1/vehicles",
        json={
            "registration_number": unique("REG"),
            "vehicle_type": "truck",
            "device_id": unique("DEV"),
        },
    )
    assert response.status_code == 201
    response = client.post(
        "/api/v1/vehicles",
        json={
            "registration_number": unique("REG"),
            "vehicle_type": "truck",
            "device_id": response.json()["device_id"],
        },
    )
    assert response.status_code == 409
    assert "device id" in response.json()["error"]["message"]

    response = client.get(f"/api/v1/vehicles/{uuid4()}")
    assert response.status_code == 404
    assert response.json()["error"] == {
        "code": "not_found",
        "message": "Vehicle not found.",
        "details": None,
    }

    response = client.patch(f"/api/v1/vehicles/{uuid4()}", json={"vehicle_type": "van"})
    assert response.status_code == 404

    response = client.patch(f"/api/v1/vehicles/{uuid4()}/status", json={"status": "active"})
    assert response.status_code == 404


def test_driver_crud_filtering_and_duplicates(client: TestClient) -> None:
    license_number = unique("LIC")
    phone = f"+49 30 {uuid4().int % 10_000_000:07d}"
    driver = create_driver(
        client,
        full_name="Ada Driver",
        license_number=license_number,
        phone=phone,
    )
    second_phone = f"+49 40 {uuid4().int % 10_000_000:07d}"
    create_driver(client, full_name="Grace Driver", phone=second_phone)

    response = client.get(
        "/api/v1/drivers",
        params={"status": "active", "page_size": 1},
    )
    assert response.status_code == 200
    body = response.json()
    assert len(body["items"]) == 1
    assert body["total"] >= 2
    assert body["pages"] >= 2
    assert body["items"][0]["status"] == "active"

    response = client.get(f"/api/v1/drivers/{driver['id']}")
    assert response.status_code == 200
    assert response.json()["full_name"] == "Ada Driver"

    response = client.patch(
        f"/api/v1/drivers/{driver['id']}",
        json={"full_name": "Ada Lovelace", "phone": None},
    )
    assert response.status_code == 200
    assert response.json()["full_name"] == "Ada Lovelace"
    assert response.json()["phone"] is None

    response = client.patch(
        f"/api/v1/drivers/{driver['id']}/status",
        json={"status": "inactive"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "inactive"

    response = client.get(
        "/api/v1/drivers",
        params={"status": "inactive", "page_size": 100},
    )
    body = response.json()
    assert body["total"] >= 1
    assert all(item["status"] == "inactive" for item in body["items"])

    response = client.post(
        "/api/v1/drivers",
        json={
            "full_name": "Duplicate License",
            "license_number": license_number,
        },
    )
    assert response.status_code == 409
    assert "license number" in response.json()["error"]["message"]

    response = client.post(
        "/api/v1/drivers",
        json={
            "full_name": "Duplicate Phone",
            "license_number": unique("LIC"),
            "phone": second_phone,
        },
    )
    assert response.status_code == 409
    assert "phone number" in response.json()["error"]["message"]

    response = client.post(
        "/api/v1/drivers",
        json={
            "full_name": "Bad Phone",
            "license_number": unique("LIC"),
            "phone": "not-a-phone",
        },
    )
    assert response.status_code == 422

    response = client.get(f"/api/v1/drivers/{uuid4()}")
    assert response.status_code == 404
    assert response.json()["error"]["message"] == "Driver not found."


def test_shipment_crud_validation_and_duplicates(client: TestClient) -> None:
    reference = unique("SHP")
    shipment = create_shipment(client, reference=reference)

    assert shipment["status"] == "created"
    assert shipment["origin"]["name"] == "Berlin"
    assert shipment["origin"]["latitude"] == pytest.approx(52.52)
    assert shipment["destination"]["name"] == "Hamburg"

    response = client.get(f"/api/v1/shipments/{shipment['id']}")
    assert response.status_code == 200
    assert response.json()["reference"] == reference

    response = client.patch(
        f"/api/v1/shipments/{shipment['id']}",
        json={
            "destination": {
                "name": "Munich",
                "latitude": 48.137,
                "longitude": 11.575,
            },
            "expected_delivery_at": datetime.now(UTC).isoformat(),
        },
    )
    assert response.status_code == 200
    assert response.json()["destination"]["name"] == "Munich"

    response = client.get(
        "/api/v1/shipments",
        params={"status": "created", "page_size": 1},
    )
    assert response.status_code == 200
    body = response.json()
    assert len(body["items"]) == 1
    assert body["total"] >= 1
    assert body["items"][0]["status"] == "created"

    response = client.post(
        "/api/v1/shipments",
        json={
            "reference": reference,
            "origin": {"name": "Berlin", "latitude": 52.52, "longitude": 13.405},
            "destination": {"name": "Hamburg", "latitude": 53.551, "longitude": 9.993},
        },
    )
    assert response.status_code == 409
    assert "reference" in response.json()["error"]["message"]

    response = client.post(
        "/api/v1/shipments",
        json={
            "reference": unique("SHP"),
            "origin": {"name": "Berlin", "latitude": 100.0, "longitude": 13.405},
            "destination": {"name": "Hamburg", "latitude": 53.551, "longitude": 9.993},
        },
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"

    response = client.post(
        "/api/v1/shipments",
        json={
            "reference": unique("SHP"),
            "origin": {"name": "Berlin", "latitude": 52.52, "longitude": 13.405},
            "destination": {"name": "Hamburg", "latitude": 53.551, "longitude": 9.993},
            "expected_delivery_at": "2026-01-01T12:00:00",
        },
    )
    assert response.status_code == 422

    response = client.get(f"/api/v1/shipments/{uuid4()}")
    assert response.status_code == 404
    assert response.json()["error"]["message"] == "Shipment not found."


def test_shipment_valid_and_invalid_transitions(client: TestClient) -> None:
    shipment = create_shipment(client)
    shipment_id = shipment["id"]

    for target in ("assigned", "in_transit", "delivered"):
        response = client.patch(
            f"/api/v1/shipments/{shipment_id}/status",
            json={"status": target},
        )
        assert response.status_code == 200, response.text
        assert response.json()["status"] == target

    response = client.patch(
        f"/api/v1/shipments/{shipment_id}/status",
        json={"status": "cancelled"},
    )
    assert response.status_code == 409
    assert "Invalid shipment status transition" in response.json()["error"]["message"]

    created = create_shipment(client)
    response = client.patch(
        f"/api/v1/shipments/{created['id']}/status",
        json={"status": "delivered"},
    )
    assert response.status_code == 409
    assert "created" in response.json()["error"]["message"]
    assert "delivered" in response.json()["error"]["message"]

    response = client.patch(
        f"/api/v1/shipments/{created['id']}/status",
        json={"status": "created"},
    )
    assert response.status_code == 409

    cancelled = create_shipment(client)
    response = client.patch(
        f"/api/v1/shipments/{cancelled['id']}/status",
        json={"status": "cancelled"},
    )
    assert response.status_code == 200
    response = client.patch(
        f"/api/v1/shipments/{cancelled['id']}/status",
        json={"status": "assigned"},
    )
    assert response.status_code == 409


def test_assignment_lifecycle_history_and_reenlistment(client: TestClient) -> None:
    vehicle = create_vehicle(client)
    driver = create_driver(client)
    shipment = create_shipment(client)

    response = post_assignment(client, vehicle["id"], driver["id"], shipment["id"])
    assert response.status_code == 201, response.text
    assignment = response.json()
    assert assignment["ended_at"] is None
    assert assignment["vehicle"]["id"] == vehicle["id"]
    assert assignment["driver"]["id"] == driver["id"]
    assert assignment["shipment"]["id"] == shipment["id"]

    response = client.get(f"/api/v1/shipments/{shipment['id']}")
    assert response.json()["status"] == "assigned"

    response = client.get(f"/api/v1/assignments/{assignment['id']}")
    assert response.status_code == 200
    assert response.json()["id"] == assignment["id"]

    for path in (
        "/api/v1/assignments",
        f"/api/v1/vehicles/{vehicle['id']}/assignments",
        f"/api/v1/drivers/{driver['id']}/assignments",
        f"/api/v1/shipments/{shipment['id']}/assignments",
    ):
        params = {"vehicle_id": vehicle["id"]} if path == "/api/v1/assignments" else None
        response = client.get(path, params=params)
        assert response.status_code == 200
        assert response.json()["total"] == 1

    response = client.post(f"/api/v1/assignments/{assignment['id']}/end")
    assert response.status_code == 200, response.text
    ended = response.json()
    assert ended["ended_at"] is not None

    response = client.get(f"/api/v1/shipments/{shipment['id']}")
    assert response.json()["status"] == "created"

    response = client.post(f"/api/v1/assignments/{assignment['id']}/end")
    assert response.status_code == 409
    assert response.json()["error"]["message"] == "Assignment has already ended."

    response = client.get(
        f"/api/v1/vehicles/{vehicle['id']}/assignments",
        params={"active": False},
    )
    assert response.json()["total"] == 1
    assert response.json()["items"][0]["ended_at"] is not None

    response = client.get(
        f"/api/v1/vehicles/{vehicle['id']}/assignments",
        params={"active": True},
    )
    assert response.json()["total"] == 0

    response = post_assignment(client, vehicle["id"], driver["id"], shipment["id"])
    assert response.status_code == 201, response.text
    second = response.json()
    assert second["id"] != assignment["id"]

    response = client.get(f"/api/v1/vehicles/{vehicle['id']}/assignments")
    body = response.json()
    assert body["total"] == 2
    assert [item["ended_at"] is None for item in body["items"]] == [True, False]

    response = client.get(
        "/api/v1/assignments",
        params={"vehicle_id": vehicle["id"], "active": True},
    )
    assert response.json()["total"] == 1
    assert response.json()["items"][0]["id"] == second["id"]

    response = client.get(
        "/api/v1/assignments",
        params={"vehicle_id": vehicle["id"], "page": 1, "page_size": 1},
    )
    body = response.json()
    assert len(body["items"]) == 1
    assert body["total"] == 2
    assert body["pages"] == 2

    response = client.post(f"/api/v1/assignments/{second['id']}/end")
    assert response.status_code == 200
    assert response.json()["ended_at"] is not None


def test_assignment_rejects_inactive_resources_and_ineligible_shipments(
    client: TestClient,
) -> None:
    inactive_vehicle = create_vehicle(client, status="inactive")
    driver = create_driver(client)
    shipment = create_shipment(client)
    response = post_assignment(
        client,
        inactive_vehicle["id"],
        driver["id"],
        shipment["id"],
    )
    assert response.status_code == 409
    assert "Vehicle is not active" in response.json()["error"]["message"]

    active_vehicle = create_vehicle(client)
    inactive_driver = create_driver(client, status="inactive")
    response = post_assignment(
        client,
        active_vehicle["id"],
        inactive_driver["id"],
        shipment["id"],
    )
    assert response.status_code == 409
    assert "Driver is not active" in response.json()["error"]["message"]

    cancelled = create_shipment(client)
    response = client.patch(
        f"/api/v1/shipments/{cancelled['id']}/status",
        json={"status": "cancelled"},
    )
    assert response.status_code == 200
    response = post_assignment(
        client,
        active_vehicle["id"],
        driver["id"],
        cancelled["id"],
    )
    assert response.status_code == 409
    assert "not eligible" in response.json()["error"]["message"]


def test_assignment_rejects_duplicate_active_resources(client: TestClient) -> None:
    vehicle = create_vehicle(client)
    driver = create_driver(client)
    shipment = create_shipment(client)
    response = post_assignment(client, vehicle["id"], driver["id"], shipment["id"])
    assert response.status_code == 201

    other_driver = create_driver(client)
    other_shipment = create_shipment(client)
    response = post_assignment(
        client,
        vehicle["id"],
        other_driver["id"],
        other_shipment["id"],
    )
    assert response.status_code == 409
    assert "vehicle already has an active assignment" in response.json()["error"]["message"]

    other_vehicle = create_vehicle(client)
    third_shipment = create_shipment(client)
    response = post_assignment(
        client,
        other_vehicle["id"],
        driver["id"],
        third_shipment["id"],
    )
    assert response.status_code == 409
    assert "driver already has an active assignment" in response.json()["error"]["message"]


def test_empty_update_payloads_return_400(client: TestClient) -> None:
    vehicle = create_vehicle(client)
    driver = create_driver(client)
    shipment = create_shipment(client)

    for path in (
        f"/api/v1/vehicles/{vehicle['id']}",
        f"/api/v1/drivers/{driver['id']}",
        f"/api/v1/shipments/{shipment['id']}",
    ):
        response = client.patch(path, json={})
        assert response.status_code == 400
        assert response.json()["error"]["code"] == "bad_request"
        assert response.json()["error"]["message"] == "No fields were provided for update."


def test_assignment_not_found_responses(client: TestClient) -> None:
    vehicle = create_vehicle(client)
    driver = create_driver(client)
    shipment = create_shipment(client)

    response = post_assignment(client, str(uuid4()), driver["id"], shipment["id"])
    assert response.status_code == 404
    assert response.json()["error"]["message"] == "Vehicle not found."

    response = post_assignment(client, vehicle["id"], str(uuid4()), shipment["id"])
    assert response.status_code == 404
    assert response.json()["error"]["message"] == "Driver not found."

    response = post_assignment(client, vehicle["id"], driver["id"], str(uuid4()))
    assert response.status_code == 404
    assert response.json()["error"]["message"] == "Shipment not found."

    response = client.get(f"/api/v1/assignments/{uuid4()}")
    assert response.status_code == 404
    assert response.json()["error"]["message"] == "Assignment not found."

    response = client.post(f"/api/v1/assignments/{uuid4()}/end")
    assert response.status_code == 404

    response = client.get(f"/api/v1/vehicles/{uuid4()}/assignments")
    assert response.status_code == 404

    response = client.get(f"/api/v1/drivers/{uuid4()}/assignments")
    assert response.status_code == 404

    response = client.get(f"/api/v1/shipments/{uuid4()}/assignments")
    assert response.status_code == 404
