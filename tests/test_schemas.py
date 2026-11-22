"""Unit tests for request/response schema validation."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.models.enums import DriverStatus, ShipmentStatus, VehicleStatus
from app.schemas.assignment import AssignmentCreate
from app.schemas.common import Location, Page
from app.schemas.driver import DriverCreate, DriverUpdate
from app.schemas.shipment import ShipmentCreate, ShipmentUpdate
from app.schemas.vehicle import VehicleCreate, VehicleRead, VehicleUpdate


def test_location_accepts_valid_coordinates() -> None:
    location = Location(name="Berlin", latitude=52.52, longitude=13.405)

    assert location.name == "Berlin"
    assert location.latitude == 52.52
    assert location.longitude == 13.405


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("latitude", 90.01),
        ("latitude", -90.01),
        ("longitude", 180.01),
        ("longitude", -180.01),
    ],
)
def test_location_rejects_out_of_range_coordinates(field: str, value: float) -> None:
    payload = {"name": "Berlin", "latitude": 52.52, "longitude": 13.405, field: value}

    with pytest.raises(ValidationError):
        Location.model_validate(payload)


def test_location_rejects_missing_name_and_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        Location.model_validate({"latitude": 1.0, "longitude": 2.0})

    with pytest.raises(ValidationError):
        Location.model_validate(
            {"name": "Berlin", "latitude": 1.0, "longitude": 2.0, "country": "DE"}
        )


def test_vehicle_create_defaults_and_normalisation() -> None:
    vehicle = VehicleCreate(
        registration_number="  B-AB 123  ",
        vehicle_type="  van  ",
        device_id="   ",
    )

    assert vehicle.registration_number == "B-AB 123"
    assert vehicle.vehicle_type == "van"
    assert vehicle.device_id is None
    assert vehicle.status == VehicleStatus.ACTIVE


@pytest.mark.parametrize("registration_number", ["", "   ", "X" * 33])
def test_vehicle_create_rejects_invalid_registration(registration_number: str) -> None:
    with pytest.raises(ValidationError):
        VehicleCreate(registration_number=registration_number, vehicle_type="van")


def test_vehicle_update_keeps_unset_fields_out_of_dump() -> None:
    update = VehicleUpdate(vehicle_type="truck")

    assert update.model_dump(exclude_unset=True) == {"vehicle_type": "truck"}


def test_vehicle_read_accepts_orm_like_objects() -> None:
    class Row:
        id = uuid4()
        registration_number = "B-AB 123"
        vehicle_type = "van"
        status = VehicleStatus.ACTIVE
        device_id = None
        created_at = datetime.now(UTC)
        updated_at = datetime.now(UTC)

    vehicle = VehicleRead.model_validate(Row())

    assert vehicle.registration_number == "B-AB 123"


def test_driver_create_defaults_and_phone_normalisation() -> None:
    driver = DriverCreate(full_name="Ada Driver", license_number="LIC-1", phone="  ")

    assert driver.phone is None
    assert driver.status == DriverStatus.ACTIVE


@pytest.mark.parametrize("phone", ["abc", "12", "++--"])
def test_driver_create_rejects_invalid_phone(phone: str) -> None:
    with pytest.raises(ValidationError):
        DriverCreate(full_name="Ada Driver", license_number="LIC-1", phone=phone)


def test_driver_update_allows_clearing_phone() -> None:
    update = DriverUpdate(phone=None)

    assert update.model_dump(exclude_unset=True) == {"phone": None}


def test_shipment_create_requires_aware_delivery_timestamp() -> None:
    payload = {
        "reference": "SHP-1",
        "origin": {"name": "Berlin", "latitude": 52.52, "longitude": 13.405},
        "destination": {"name": "Hamburg", "latitude": 53.55, "longitude": 9.99},
    }

    shipment = ShipmentCreate(
        **payload,
        expected_delivery_at=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
    )
    assert shipment.expected_delivery_at is not None

    with pytest.raises(ValidationError):
        ShipmentCreate(**payload, expected_delivery_at=datetime(2026, 1, 1, 12, 0))


def test_shipment_create_rejects_unstructured_locations() -> None:
    with pytest.raises(ValidationError):
        ShipmentCreate(
            reference="SHP-1",
            origin={"city": "Berlin"},
            destination={"name": "Hamburg", "latitude": 53.55, "longitude": 9.99},
        )


def test_shipment_update_keeps_origin_and_destination_structured() -> None:
    update = ShipmentUpdate(origin={"name": "Munich", "latitude": 48.13, "longitude": 11.58})

    assert update.origin is not None
    assert update.origin.name == "Munich"


def test_assignment_create_parses_uuids() -> None:
    vehicle_id, driver_id, shipment_id = uuid4(), uuid4(), uuid4()

    assignment = AssignmentCreate(
        vehicle_id=vehicle_id,
        driver_id=driver_id,
        shipment_id=shipment_id,
    )

    assert assignment.vehicle_id == vehicle_id
    assert assignment.driver_id == driver_id
    assert assignment.shipment_id == shipment_id


def test_page_serialises_items_and_metadata() -> None:
    page: Page[str] = Page(items=["a", "b"], total=5, page=1, page_size=2, pages=3)

    assert page.model_dump() == {
        "items": ["a", "b"],
        "total": 5,
        "page": 1,
        "page_size": 2,
        "pages": 3,
    }


def test_shipment_status_enum_values_are_validated() -> None:
    with pytest.raises(ValidationError):
        ShipmentCreate.model_validate(
            {
                "reference": "SHP-1",
                "origin": {"name": "Berlin", "latitude": 52.52, "longitude": 13.405},
                "destination": {"name": "Hamburg", "latitude": 53.55, "longitude": 9.99},
                "status": "lost",
            }
        )

    assert {status.value for status in ShipmentStatus} == {
        "created",
        "assigned",
        "in_transit",
        "delivered",
        "cancelled",
    }
