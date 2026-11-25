"""Redis persistence for the live vehicle-location projection.

Key design (all keys share the configured prefix, default ``fleetpulse``):

- ``{prefix}:live:location:{vehicle_id}`` — hash with the latest observation.
- ``{prefix}:live:geo`` — GEO sorted set of every currently indexed vehicle.

The hash and the GEO entry are updated by one Lua script so an accepted event
can never leave the two structures out of sync. The script compares
``recorded_at`` (epoch milliseconds) first and the canonical ``event_id``
string second: an older observation is rejected, an identical observation is
a duplicate, and only a strictly newer observation is written. Redis key TTLs
are deliberately not used for freshness — a stale vehicle keeps its last known
position.
"""

import logging
from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.schemas.live_location import (
    LiveLocationRecord,
    LiveLocationWrite,
    LocationUpdateOutcome,
    NearbyVehicle,
)

logger = logging.getLogger(__name__)

GEO_KEY_SUFFIX = "live:geo"
LOCATION_KEY_SUFFIX = "live:location"


class LiveLocationStoreError(Exception):
    """Raised when Redis cannot read or write live-location state.

    The message never contains the Redis URL, credentials or raw command
    output; the original exception is retained only as a cause for logs.
    """


@dataclass(frozen=True, slots=True)
class LiveLocationKeys:
    """Deterministic key names for one namespace prefix."""

    prefix: str = "fleetpulse"

    def __post_init__(self) -> None:
        if (
            not self.prefix
            or self.prefix != self.prefix.strip()
            or any(character.isspace() for character in self.prefix)
        ):
            raise ValueError("prefix must be a non-empty string without whitespace")

    @property
    def geo(self) -> str:
        """Key of the GEO sorted set indexing every vehicle position."""
        return f"{self.prefix}:{GEO_KEY_SUFFIX}"

    def location(self, vehicle_id: UUID) -> str:
        """Key of the latest-location hash for ``vehicle_id``."""
        return f"{self.prefix}:{LOCATION_KEY_SUFFIX}:{vehicle_id}"


# Atomic compare-and-set. Returns 1 when applied, 0 for an exact duplicate and
# -1 for an observation older than (or losing the tie-break to) the stored one.
#
# KEYS[1] = latest-location hash, KEYS[2] = GEO sorted set.
# ARGV: vehicle_id, event_id, recorded_at_ms, latitude, longitude, speed_kmh,
#       heading, recorded_at, received_at, received_at_ms, shipment_id.
UPDATE_SCRIPT = """
local existing_ms = redis.call('HGET', KEYS[1], 'recorded_at_ms')
if existing_ms then
  local existing_event = redis.call('HGET', KEYS[1], 'event_id') or ''
  local existing_number = tonumber(existing_ms)
  local incoming_number = tonumber(ARGV[3])
  if existing_number > incoming_number then
    return -1
  end
  if existing_number == incoming_number then
    if existing_event > ARGV[2] then
      return -1
    end
    if existing_event == ARGV[2] then
      return 0
    end
  end
end

redis.call('HSET', KEYS[1],
  'vehicle_id', ARGV[1],
  'event_id', ARGV[2],
  'latitude', ARGV[4],
  'longitude', ARGV[5],
  'speed_kmh', ARGV[6],
  'heading', ARGV[7],
  'recorded_at', ARGV[8],
  'recorded_at_ms', ARGV[3],
  'received_at', ARGV[9],
  'received_at_ms', ARGV[10],
  'shipment_id', ARGV[11])
redis.call('GEOADD', KEYS[2], ARGV[5], ARGV[4], ARGV[1])
return 1
"""

_OUTCOME_BY_CODE = {
    1: LocationUpdateOutcome.APPLIED,
    0: LocationUpdateOutcome.DUPLICATE,
    -1: LocationUpdateOutcome.STALE,
}


def _normalise_utc(value: datetime) -> datetime:
    """Reject naive timestamps and normalise aware values to UTC."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("live-location timestamps must include a timezone offset")
    return value.astimezone(UTC)


def _to_epoch_milliseconds(value: datetime) -> int:
    """Convert an aware timestamp to integer epoch milliseconds."""
    return round(_normalise_utc(value).timestamp() * 1000)


def serialize_location(location: LiveLocationWrite) -> dict[str, str]:
    """Serialise a write model into Redis hash fields (all strings).

    ``heading`` and ``shipment_id`` use an empty string for ``None`` because
    Redis hashes cannot store null values.
    """
    recorded_at = _normalise_utc(location.recorded_at)
    received_at = _normalise_utc(location.received_at)
    return {
        "vehicle_id": str(location.vehicle_id),
        "event_id": str(location.event_id),
        "latitude": repr(float(location.latitude)),
        "longitude": repr(float(location.longitude)),
        "speed_kmh": repr(float(location.speed_kmh)),
        "heading": "" if location.heading is None else repr(float(location.heading)),
        "recorded_at": recorded_at.isoformat(),
        "recorded_at_ms": str(_to_epoch_milliseconds(recorded_at)),
        "received_at": received_at.isoformat(),
        "received_at_ms": str(_to_epoch_milliseconds(received_at)),
        "shipment_id": "" if location.shipment_id is None else str(location.shipment_id),
    }


def deserialize_location(fields: dict[str, Any]) -> LiveLocationRecord | None:
    """Deserialise Redis hash fields into a validated record.

    Returns ``None`` for an empty hash. A partially written or corrupt hash
    raises :class:`ValueError`/``pydantic.ValidationError`` rather than
    returning a half-valid location.
    """
    if not fields:
        return None

    def optional_text(name: str) -> str | None:
        value = fields.get(name)
        if value is None or value == "":
            return None
        return str(value)

    heading = optional_text("heading")
    shipment_id = optional_text("shipment_id")
    return LiveLocationRecord.model_validate(
        {
            "vehicle_id": fields["vehicle_id"],
            "event_id": fields["event_id"],
            "latitude": float(fields["latitude"]),
            "longitude": float(fields["longitude"]),
            "speed_kmh": float(fields["speed_kmh"]),
            "heading": None if heading is None else float(heading),
            "recorded_at": fields["recorded_at"],
            "received_at": fields["received_at"],
            "shipment_id": shipment_id,
        }
    )


class LiveLocationRepository:
    """Atomic Redis reads and writes for live vehicle state."""

    def __init__(self, client: Redis, *, prefix: str = "fleetpulse") -> None:
        self._client = client
        self._keys = LiveLocationKeys(prefix)

    @property
    def keys(self) -> LiveLocationKeys:
        """Key generator for this repository's namespace."""
        return self._keys

    async def apply(self, location: LiveLocationWrite) -> LocationUpdateOutcome:
        """Atomically write a newer observation and refresh the GEO index.

        Repeated delivery of the same event returns
        :attr:`LocationUpdateOutcome.DUPLICATE`; an older observation (or an
        equal timestamp whose ``event_id`` loses the deterministic tie-break)
        returns :attr:`LocationUpdateOutcome.STALE`. Neither mutates Redis.
        """
        fields = serialize_location(location)
        arguments = [
            fields["vehicle_id"],
            fields["event_id"],
            fields["recorded_at_ms"],
            fields["latitude"],
            fields["longitude"],
            fields["speed_kmh"],
            fields["heading"],
            fields["recorded_at"],
            fields["received_at"],
            fields["received_at_ms"],
            fields["shipment_id"],
        ]
        try:
            result = await cast(
                Awaitable[Any],
                self._client.eval(
                    UPDATE_SCRIPT,
                    2,
                    self._keys.location(location.vehicle_id),
                    self._keys.geo,
                    *arguments,
                ),
            )
        except (RedisError, OSError, TimeoutError) as exc:
            logger.warning(
                "Live-location update failed for vehicle %s (%s)",
                location.vehicle_id,
                type(exc).__name__,
            )
            raise LiveLocationStoreError("live-location update failed") from exc

        try:
            outcome = _OUTCOME_BY_CODE[int(result)]
        except (KeyError, TypeError, ValueError) as exc:
            raise LiveLocationStoreError("live-location update returned an unknown result") from exc
        return outcome

    async def latest(self, vehicle_id: UUID) -> LiveLocationRecord | None:
        """Return the stored latest location or ``None`` when never seen."""
        try:
            fields = await cast(
                Awaitable[dict[str, Any]],
                self._client.hgetall(self._keys.location(vehicle_id)),
            )
        except (RedisError, OSError, TimeoutError) as exc:
            logger.warning(
                "Live-location read failed for vehicle %s (%s)",
                vehicle_id,
                type(exc).__name__,
            )
            raise LiveLocationStoreError("live-location read failed") from exc
        return deserialize_location(fields)

    async def coordinates(self, vehicle_id: UUID) -> tuple[float, float] | None:
        """Return ``(latitude, longitude)`` from the GEO index, if present."""
        try:
            positions = await self._client.geopos(self._keys.geo, str(vehicle_id))
        except (RedisError, OSError, TimeoutError) as exc:
            logger.warning(
                "Geo coordinate read failed for vehicle %s (%s)",
                vehicle_id,
                type(exc).__name__,
            )
            raise LiveLocationStoreError("geo coordinate read failed") from exc
        if not positions or positions[0] is None:
            return None
        longitude, latitude = positions[0]
        return float(latitude), float(longitude)

    async def nearby(
        self,
        latitude: float,
        longitude: float,
        radius_km: float,
        *,
        limit: int = 50,
    ) -> list[NearbyVehicle]:
        """Return vehicles within ``radius_km``, nearest first."""
        if radius_km <= 0:
            raise ValueError("radius_km must be positive")
        if limit < 1:
            raise ValueError("limit must be at least 1")
        try:
            results = await self._client.geosearch(
                self._keys.geo,
                longitude=longitude,
                latitude=latitude,
                radius=radius_km,
                unit="km",
                sort="ASC",
                count=limit,
                withdist=True,
                withcoord=True,
            )
        except (RedisError, OSError, TimeoutError) as exc:
            logger.warning("Geo radius query failed (%s)", type(exc).__name__)
            raise LiveLocationStoreError("geo radius query failed") from exc

        vehicles: list[NearbyVehicle] = []
        for entry in results:
            member, distance, position = entry
            entry_longitude, entry_latitude = position
            vehicles.append(
                NearbyVehicle(
                    vehicle_id=UUID(str(member)),
                    latitude=float(entry_latitude),
                    longitude=float(entry_longitude),
                    distance_km=float(distance),
                )
            )
        return vehicles

    async def delete(self, vehicle_id: UUID) -> None:
        """Remove one vehicle from both the hash and the GEO index."""
        try:
            async with self._client.pipeline(transaction=True) as pipeline:
                pipeline.delete(self._keys.location(vehicle_id))
                pipeline.zrem(self._keys.geo, str(vehicle_id))
                await pipeline.execute()
        except (RedisError, OSError, TimeoutError) as exc:
            raise LiveLocationStoreError("live-location delete failed") from exc
