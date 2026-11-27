"""Live-location models stored in the Redis projection.

These models are transport- and storage-neutral: the repository serialises
them into a Redis hash, and the service adds freshness metadata. Redis stores
``speed_kmh`` (the contract's ``speed_kmph`` is mapped by the service) and
keeps ``recorded_at`` as both an ISO-8601 string and epoch milliseconds; the
millisecond value is the ordering key used by the atomic update script.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class LocationUpdateOutcome(StrEnum):
    """Result of attempting to apply one GPS observation."""

    APPLIED = "applied"
    DUPLICATE = "duplicate"
    STALE = "stale"


@dataclass(frozen=True, slots=True)
class LiveLocationWrite:
    """A validated observation ready to be written to Redis."""

    vehicle_id: UUID
    event_id: UUID
    latitude: float
    longitude: float
    speed_kmh: float
    heading: float | None
    recorded_at: datetime
    received_at: datetime
    shipment_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class LocationUpdateResult:
    """Outcome reported to the service after the atomic Redis update."""

    outcome: LocationUpdateOutcome
    vehicle_id: UUID
    event_id: UUID


class LiveLocationRecord(BaseModel):
    """Latest location as stored in Redis, without freshness metadata."""

    model_config = ConfigDict(frozen=True)

    vehicle_id: UUID
    event_id: UUID
    latitude: float
    longitude: float
    speed_kmh: float
    heading: float | None = None
    recorded_at: datetime
    received_at: datetime
    shipment_id: UUID | None = None

    @field_validator("recorded_at", "received_at")
    @classmethod
    def _require_aware_timestamp(cls, value: datetime) -> datetime:
        """Reject naive or corrupt timestamps instead of crashing freshness math."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("live-location timestamps must include a timezone offset")
        return value.astimezone(UTC)


class LiveLocation(LiveLocationRecord):
    """Latest location plus a timestamp-derived stale indicator.

    Freshness is calculated from ``recorded_at`` and the configured threshold;
    it is never expressed as a Redis key TTL, so a silent vehicle keeps its
    last known position available and is explicitly marked stale.
    """

    stale: bool
    age_seconds: float = Field(ge=0)


class NearbyVehicle(BaseModel):
    """One vehicle returned by a geographic radius query."""

    model_config = ConfigDict(frozen=True)

    vehicle_id: UUID
    latitude: float
    longitude: float
    distance_km: float = Field(ge=0)


@dataclass(frozen=True, slots=True)
class NearbyLocationRecord:
    """A stored latest location paired with its GEO distance in kilometres."""

    location: LiveLocationRecord
    distance_km: float


class DistanceUnit(StrEnum):
    """Units accepted and returned by the nearby-vehicle endpoint."""

    METERS = "m"
    KILOMETERS = "km"
    MILES = "mi"


class LiveLocationRead(BaseModel):
    """Public representation of one live location plus freshness metadata."""

    model_config = ConfigDict(frozen=True)

    vehicle_id: UUID
    event_id: UUID
    latitude: float
    longitude: float
    speed_kmh: float
    heading: float | None = None
    recorded_at: datetime
    received_at: datetime
    shipment_id: UUID | None = None
    is_stale: bool
    age_seconds: float = Field(ge=0)

    @classmethod
    def from_location(cls, location: LiveLocation) -> "LiveLocationRead":
        """Map the internal freshness model onto the public response model."""
        return cls(
            **location.model_dump(exclude={"stale"}),
            is_stale=location.stale,
        )


class LiveLocationPage(BaseModel):
    """One deterministic page of live locations."""

    items: list[LiveLocationRead]
    page: int = Field(ge=1)
    page_size: int = Field(ge=1)
    total: int = Field(ge=0)
    total_pages: int = Field(ge=0)


class GeoCenter(BaseModel):
    """Coordinates echoed back by a nearby search."""

    model_config = ConfigDict(frozen=True)

    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)


class NearbyVehicleRead(LiveLocationRead):
    """A live location plus its distance from the requested centre."""

    distance: float = Field(ge=0)


class NearbySearchResponse(BaseModel):
    """Result of a GEO radius query in the caller's chosen unit."""

    center: GeoCenter
    radius: float = Field(gt=0)
    unit: DistanceUnit
    items: list[NearbyVehicleRead]
