"""Vehicle request and response schemas."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.models.enums import VehicleStatus
from app.schemas.common import RequestModel


def _blank_to_none(value: object) -> object:
    """Treat an empty optional string as an omitted value."""
    if isinstance(value, str) and not value.strip():
        return None
    return value


class VehicleCreate(RequestModel):
    """Payload for creating a vehicle."""

    registration_number: str = Field(min_length=1, max_length=32)
    vehicle_type: str = Field(min_length=1, max_length=50)
    status: VehicleStatus = VehicleStatus.ACTIVE
    device_id: str | None = Field(default=None, max_length=100)

    _normalise_device_id = field_validator("device_id", mode="before")(_blank_to_none)


class VehicleUpdate(RequestModel):
    """Payload for partially updating a vehicle."""

    registration_number: str | None = Field(default=None, min_length=1, max_length=32)
    vehicle_type: str | None = Field(default=None, min_length=1, max_length=50)
    status: VehicleStatus | None = None
    device_id: str | None = Field(default=None, max_length=100)

    _normalise_device_id = field_validator("device_id", mode="before")(_blank_to_none)


class VehicleStatusUpdate(RequestModel):
    """Payload for changing only a vehicle's status."""

    status: VehicleStatus


class VehicleRead(BaseModel):
    """Public vehicle representation."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    registration_number: str
    vehicle_type: str
    status: VehicleStatus
    device_id: str | None
    created_at: datetime
    updated_at: datetime
