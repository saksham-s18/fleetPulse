"""Shipment request and response schemas."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.models.enums import ShipmentStatus
from app.schemas.common import Location, RequestModel


def _require_timezone(value: datetime | None) -> datetime | None:
    """Reject naive datetimes so timestamps are unambiguous."""
    if value is not None and value.tzinfo is None:
        raise ValueError("expected_delivery_at must include a timezone offset")
    return value


class ShipmentCreate(RequestModel):
    """Payload for creating a shipment."""

    reference: str = Field(min_length=1, max_length=64)
    origin: Location
    destination: Location
    expected_delivery_at: datetime | None = None

    _validate_expected_delivery_at = field_validator("expected_delivery_at")(_require_timezone)


class ShipmentUpdate(RequestModel):
    """Payload for partially updating a shipment."""

    reference: str | None = Field(default=None, min_length=1, max_length=64)
    origin: Location | None = None
    destination: Location | None = None
    expected_delivery_at: datetime | None = None

    _validate_expected_delivery_at = field_validator("expected_delivery_at")(_require_timezone)


class ShipmentStatusUpdate(RequestModel):
    """Payload for changing only a shipment's status."""

    status: ShipmentStatus


class LocationRead(BaseModel):
    """Location representation.

    Fields are optional so legacy rows with partial JSONB locations can still be
    returned without breaking the API. New requests are validated by the strict
    :class:`~app.schemas.common.Location` schema.
    """

    name: str | None = None
    latitude: float | None = None
    longitude: float | None = None


class ShipmentRead(BaseModel):
    """Public shipment representation."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    reference: str
    origin: LocationRead
    destination: LocationRead
    status: ShipmentStatus
    expected_delivery_at: datetime | None
    created_at: datetime
    updated_at: datetime
