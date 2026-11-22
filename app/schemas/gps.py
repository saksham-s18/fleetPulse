"""Validated GPS event contract shared by ingestion and simulation.

The schema deliberately has no broker, database or framework dependency. A GPS
event can therefore be produced by a device, a simulator or a future Kafka
consumer and validated in exactly the same way.
"""

from datetime import UTC, datetime
from uuid import UUID

from pydantic import Field, field_validator

from app.schemas.common import RequestModel

GPS_SCHEMA_VERSION = "1.0"


def _normalise_recorded_at(value: datetime) -> datetime:
    """Reject naive timestamps and normalise aware values to UTC."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("recorded_at must include a timezone offset")
    return value.astimezone(UTC)


class GpsEvent(RequestModel):
    """A single GPS observation reported by a vehicle.

    ``heading`` follows compass degrees: north is ``0``, east is ``90`` and
    ``360`` is accepted as an equivalent representation of north. A missing
    heading means the device did not report one, for example while stationary.
    """

    event_id: UUID
    vehicle_id: UUID
    shipment_id: UUID | None = None
    latitude: float = Field(ge=-90, le=90, allow_inf_nan=False)
    longitude: float = Field(ge=-180, le=180, allow_inf_nan=False)
    speed_kmph: float = Field(ge=0, allow_inf_nan=False)
    heading: float | None = Field(default=None, ge=0, le=360, allow_inf_nan=False)
    recorded_at: datetime
    schema_version: str = Field(default=GPS_SCHEMA_VERSION, min_length=1, max_length=32)

    _validate_recorded_at = field_validator("recorded_at")(_normalise_recorded_at)
