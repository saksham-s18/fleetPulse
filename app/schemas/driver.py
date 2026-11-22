"""Driver request and response schemas."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.models.enums import DriverStatus
from app.schemas.common import RequestModel

_PHONE_PATTERN = r"^[0-9+()\-\s]+$"


def _blank_to_none(value: object) -> object:
    """Treat an empty optional string as an omitted value."""
    if isinstance(value, str) and not value.strip():
        return None
    return value


def _validate_phone(value: str | None) -> str | None:
    """Require a plausible phone number with at least three digits."""
    if value is not None and sum(character.isdigit() for character in value) < 3:
        raise ValueError("phone must contain at least three digits")
    return value


class DriverCreate(RequestModel):
    """Payload for creating a driver."""

    full_name: str = Field(min_length=1, max_length=200)
    phone: str | None = Field(default=None, min_length=3, max_length=32, pattern=_PHONE_PATTERN)
    license_number: str = Field(min_length=1, max_length=64)
    status: DriverStatus = DriverStatus.ACTIVE

    _normalise_phone = field_validator("phone", mode="before")(_blank_to_none)
    _check_phone = field_validator("phone")(_validate_phone)


class DriverUpdate(RequestModel):
    """Payload for partially updating a driver."""

    full_name: str | None = Field(default=None, min_length=1, max_length=200)
    phone: str | None = Field(default=None, min_length=3, max_length=32, pattern=_PHONE_PATTERN)
    license_number: str | None = Field(default=None, min_length=1, max_length=64)
    status: DriverStatus | None = None

    _normalise_phone = field_validator("phone", mode="before")(_blank_to_none)
    _check_phone = field_validator("phone")(_validate_phone)


class DriverStatusUpdate(RequestModel):
    """Payload for changing only a driver's status."""

    status: DriverStatus


class DriverRead(BaseModel):
    """Public driver representation."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    full_name: str
    phone: str | None
    license_number: str
    status: DriverStatus
    created_at: datetime
    updated_at: datetime
