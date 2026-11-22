"""Unit tests for the GPS event contract."""

import json
from datetime import UTC, datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.schemas.gps import GPS_SCHEMA_VERSION, GpsEvent


def make_payload(**overrides: object) -> dict[str, object]:
    """Return a valid event payload with optional overrides."""
    payload: dict[str, object] = {
        "event_id": uuid4(),
        "vehicle_id": uuid4(),
        "latitude": 52.52,
        "longitude": 13.405,
        "speed_kmph": 42.5,
        "heading": 90.0,
        "recorded_at": datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
    }
    payload.update(overrides)
    return payload


def test_valid_event_applies_defaults() -> None:
    event = GpsEvent.model_validate(make_payload(heading=None))

    assert event.shipment_id is None
    assert event.heading is None
    assert event.schema_version == GPS_SCHEMA_VERSION
    assert event.schema_version == "1.0"


def test_event_accepts_optional_shipment_and_custom_schema_version() -> None:
    shipment_id = uuid4()

    event = GpsEvent.model_validate(
        make_payload(shipment_id=shipment_id, schema_version="2.0", heading=360.0)
    )

    assert event.shipment_id == shipment_id
    assert event.schema_version == "2.0"
    assert event.heading == 360.0


def test_event_serialises_to_a_json_compatible_payload() -> None:
    event = GpsEvent.model_validate(make_payload())

    payload: dict[str, Any] = event.model_dump(mode="json")

    assert isinstance(payload["event_id"], str)
    assert isinstance(payload["vehicle_id"], str)
    assert isinstance(payload["recorded_at"], str)
    assert json.loads(json.dumps(payload))["latitude"] == 52.52


def test_event_normalises_offset_timestamps_to_utc() -> None:
    recorded_at = datetime(2026, 1, 1, 14, 0, tzinfo=timezone(timedelta(hours=2)))

    event = GpsEvent.model_validate(make_payload(recorded_at=recorded_at))

    assert event.recorded_at == datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    assert event.recorded_at.tzinfo is UTC
    assert event.recorded_at.utcoffset() == timedelta(0)


def test_event_accepts_iso_string_with_offset() -> None:
    event = GpsEvent.model_validate(make_payload(recorded_at="2026-01-01T14:00:00+02:00"))

    assert event.recorded_at == datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    "recorded_at",
    [
        datetime(2026, 1, 1, 12, 0),
        "2026-01-01T12:00:00",
    ],
)
def test_event_rejects_naive_timestamps(recorded_at: object) -> None:
    with pytest.raises(ValidationError, match="timezone"):
        GpsEvent.model_validate(make_payload(recorded_at=recorded_at))


@pytest.mark.parametrize(
    ("latitude", "longitude"),
    [
        (90.0, 180.0),
        (-90.0, -180.0),
        (0.0, 0.0),
    ],
)
def test_event_accepts_coordinate_boundaries(latitude: float, longitude: float) -> None:
    event = GpsEvent.model_validate(make_payload(latitude=latitude, longitude=longitude))

    assert event.latitude == latitude
    assert event.longitude == longitude


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("latitude", 90.0001),
        ("latitude", -90.0001),
        ("longitude", 180.0001),
        ("longitude", -180.0001),
        ("latitude", float("nan")),
        ("latitude", float("inf")),
        ("longitude", float("-inf")),
    ],
)
def test_event_rejects_invalid_coordinates(field: str, value: float) -> None:
    with pytest.raises(ValidationError):
        GpsEvent.model_validate(make_payload(**{field: value}))


def test_event_accepts_zero_speed() -> None:
    event = GpsEvent.model_validate(make_payload(speed_kmph=0.0))

    assert event.speed_kmph == 0.0


@pytest.mark.parametrize("speed_kmph", [-0.01, -1.0, float("nan"), float("inf")])
def test_event_rejects_invalid_speed(speed_kmph: float) -> None:
    with pytest.raises(ValidationError):
        GpsEvent.model_validate(make_payload(speed_kmph=speed_kmph))


@pytest.mark.parametrize("heading", [0.0, 180.0, 360.0])
def test_event_accepts_heading_boundaries(heading: float) -> None:
    event = GpsEvent.model_validate(make_payload(heading=heading))

    assert event.heading == heading


@pytest.mark.parametrize("heading", [-0.01, 360.01, float("nan"), float("inf")])
def test_event_rejects_invalid_heading(heading: float) -> None:
    with pytest.raises(ValidationError):
        GpsEvent.model_validate(make_payload(heading=heading))


@pytest.mark.parametrize(
    "field",
    ["event_id", "vehicle_id", "latitude", "longitude", "speed_kmph", "recorded_at"],
)
def test_event_rejects_missing_required_fields(field: str) -> None:
    payload = make_payload()
    del payload[field]

    with pytest.raises(ValidationError):
        GpsEvent.model_validate(payload)


def test_event_rejects_invalid_uuid() -> None:
    with pytest.raises(ValidationError):
        GpsEvent.model_validate(make_payload(event_id="not-a-uuid"))


def test_event_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        GpsEvent.model_validate(make_payload(altitude=100.0))


@pytest.mark.parametrize("schema_version", ["", "   ", "X" * 33])
def test_event_rejects_invalid_schema_version(schema_version: str) -> None:
    with pytest.raises(ValidationError):
        GpsEvent.model_validate(make_payload(schema_version=schema_version))
