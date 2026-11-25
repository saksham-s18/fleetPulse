"""Live-location models stored in the Redis projection.

These models are transport- and storage-neutral: the repository serialises
them into a Redis hash, and the service adds freshness metadata. Redis stores
``speed_kmh`` (the contract's ``speed_kmph`` is mapped by the service) and
keeps ``recorded_at`` as both an ISO-8601 string and epoch milliseconds; the
millisecond value is the ordering key used by the atomic update script.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


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
