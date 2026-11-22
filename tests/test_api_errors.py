"""Unit tests for consistent API error envelopes and OpenAPI wiring."""

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from app.main import app, create_app


@pytest.fixture
def client() -> Iterator[TestClient]:
    with TestClient(app) as test_client:
        yield test_client


def test_schema_validation_returns_consistent_422(client: TestClient) -> None:
    response = client.post(
        "/api/v1/vehicles",
        json={"registration_number": "B-AB 123", "vehicle_type": "van", "unknown": True},
    )

    assert response.status_code == 422
    body = response.json()
    assert body["error"]["code"] == "validation_error"
    assert body["error"]["message"] == "Request validation failed."
    assert isinstance(body["error"]["details"], list)


def test_invalid_uuid_path_returns_consistent_422(client: TestClient) -> None:
    response = client.get("/api/v1/vehicles/not-a-uuid")

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


def test_unknown_route_returns_consistent_404(client: TestClient) -> None:
    response = client.get("/api/v1/does-not-exist")

    assert response.status_code == 404
    assert response.json() == {
        "error": {
            "code": "not_found",
            "message": "Not Found",
            "details": None,
        }
    }


def test_wrong_method_returns_consistent_405(client: TestClient) -> None:
    response = client.delete("/api/v1/vehicles")

    assert response.status_code == 405
    assert response.json()["error"]["code"] == "method_not_allowed"


def test_unexpected_errors_are_masked() -> None:
    application = create_app()

    @application.get("/boom")
    async def boom() -> None:
        raise RuntimeError("secret internal detail")

    with TestClient(application, raise_server_exceptions=False) as test_client:
        response = test_client.get("/boom")

    assert response.status_code == 500
    assert response.json() == {
        "error": {
            "code": "internal_error",
            "message": "An unexpected error occurred.",
            "details": None,
        }
    }
    assert "secret internal detail" not in response.text


def test_openapi_exposes_core_fleet_endpoints(client: TestClient) -> None:
    response = client.get("/openapi.json")

    assert response.status_code == 200
    paths = response.json()["paths"]
    for path in (
        "/api/v1/vehicles",
        "/api/v1/vehicles/{vehicle_id}",
        "/api/v1/vehicles/{vehicle_id}/status",
        "/api/v1/drivers",
        "/api/v1/drivers/{driver_id}",
        "/api/v1/drivers/{driver_id}/status",
        "/api/v1/shipments",
        "/api/v1/shipments/{shipment_id}",
        "/api/v1/shipments/{shipment_id}/status",
        "/api/v1/assignments",
        "/api/v1/assignments/{assignment_id}",
        "/api/v1/assignments/{assignment_id}/end",
        "/api/v1/vehicles/{vehicle_id}/assignments",
        "/api/v1/drivers/{driver_id}/assignments",
        "/api/v1/shipments/{shipment_id}/assignments",
    ):
        assert path in paths
