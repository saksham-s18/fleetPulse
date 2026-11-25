"""Business-facing operations for the live vehicle-location projection.

The service maps the shared :class:`~app.schemas.gps.GpsEvent` contract onto
the Redis write model, validates caller arguments and calculates freshness. It
never touches Redis keys or Lua directly; that belongs to
:mod:`app.repositories.live_location`.

Kafka consumer integration is intentionally **not** implemented in this
milestone. A future handler can call :meth:`LiveLocationService.update_from_event`
without changing the repository contract.
"""

import logging
from datetime import UTC, datetime
from uuid import UUID

from app.repositories.live_location import LiveLocationRepository, LiveLocationStoreError
from app.schemas.gps import GpsEvent
from app.schemas.live_location import (
    LiveLocation,
    LiveLocationWrite,
    LocationUpdateResult,
    NearbyVehicle,
)

logger = logging.getLogger(__name__)

MAX_NEARBY_LIMIT = 500


class LiveLocationUnavailableError(Exception):
    """Raised when the live-location projection cannot be read or written.

    Messages stay safe for logs and API responses: no Redis URL, credentials
    or raw command output.
    """


class LiveLocationService:
    """Validate live-location operations and expose business-facing results."""

    def __init__(
        self,
        repository: LiveLocationRepository,
        *,
        stale_after_seconds: int = 60,
    ) -> None:
        if stale_after_seconds < 1:
            raise ValueError("stale_after_seconds must be at least 1")
        self._repository = repository
        self._stale_after_seconds = stale_after_seconds

    @property
    def stale_after_seconds(self) -> int:
        """Age threshold after which a location is reported as stale."""
        return self._stale_after_seconds

    async def update_from_event(
        self,
        event: GpsEvent,
        *,
        received_at: datetime | None = None,
    ) -> LocationUpdateResult:
        """Apply one validated GPS event to the live projection.

        ``recorded_at`` is the ordering field and ``event_id`` the tie-breaker.
        Older events and repeated deliveries are reported without mutating
        Redis, so callers can treat the operation as idempotent per event.
        """
        received = received_at if received_at is not None else datetime.now(UTC)
        if received.tzinfo is None or received.utcoffset() is None:
            raise ValueError("received_at must include a timezone offset")

        location = LiveLocationWrite(
            vehicle_id=event.vehicle_id,
            event_id=event.event_id,
            latitude=event.latitude,
            longitude=event.longitude,
            speed_kmh=event.speed_kmph,
            heading=event.heading,
            recorded_at=event.recorded_at,
            received_at=received.astimezone(UTC),
            shipment_id=event.shipment_id,
        )
        try:
            outcome = await self._repository.apply(location)
        except LiveLocationStoreError as exc:
            logger.warning(
                "Live-location update unavailable for vehicle %s (%s)",
                event.vehicle_id,
                type(exc).__name__,
            )
            raise LiveLocationUnavailableError("live-location update is unavailable") from exc

        logger.debug(
            "Live-location event %s for vehicle %s: %s",
            event.event_id,
            event.vehicle_id,
            outcome,
        )
        return LocationUpdateResult(
            outcome=outcome,
            vehicle_id=event.vehicle_id,
            event_id=event.event_id,
        )

    async def get_location(
        self,
        vehicle_id: UUID,
        *,
        now: datetime | None = None,
    ) -> LiveLocation | None:
        """Return the latest location with an explicit stale indicator.

        The hash has no TTL: a vehicle that stops reporting keeps its last
        known position and becomes stale once ``now - recorded_at`` exceeds
        the configured threshold. Future timestamps are treated as fresh with
        an age of zero.
        """
        try:
            record = await self._repository.latest(vehicle_id)
        except LiveLocationStoreError as exc:
            logger.warning(
                "Live-location read unavailable for vehicle %s (%s)",
                vehicle_id,
                type(exc).__name__,
            )
            raise LiveLocationUnavailableError("live-location read is unavailable") from exc

        if record is None:
            return None

        current = now if now is not None else datetime.now(UTC)
        if current.tzinfo is None or current.utcoffset() is None:
            raise ValueError("now must include a timezone offset")
        age_seconds = max(0.0, (current.astimezone(UTC) - record.recorded_at).total_seconds())
        return LiveLocation(
            **record.model_dump(),
            stale=age_seconds > self._stale_after_seconds,
            age_seconds=age_seconds,
        )

    async def find_nearby(
        self,
        latitude: float,
        longitude: float,
        radius_km: float,
        *,
        limit: int = 50,
    ) -> list[NearbyVehicle]:
        """Return indexed vehicles within ``radius_km``, nearest first."""
        if not -90 <= latitude <= 90:
            raise ValueError("latitude must be between -90 and 90")
        if not -180 <= longitude <= 180:
            raise ValueError("longitude must be between -180 and 180")
        if radius_km <= 0:
            raise ValueError("radius_km must be positive")
        if not 1 <= limit <= MAX_NEARBY_LIMIT:
            raise ValueError(f"limit must be between 1 and {MAX_NEARBY_LIMIT}")

        try:
            return await self._repository.nearby(
                latitude,
                longitude,
                radius_km,
                limit=limit,
            )
        except LiveLocationStoreError as exc:
            logger.warning("Live-location radius query unavailable (%s)", type(exc).__name__)
            raise LiveLocationUnavailableError("live-location query is unavailable") from exc

    async def get_coordinates(self, vehicle_id: UUID) -> tuple[float, float] | None:
        """Return ``(latitude, longitude)`` from the GEO index, if present."""
        try:
            return await self._repository.coordinates(vehicle_id)
        except LiveLocationStoreError as exc:
            logger.warning(
                "Live-location coordinate read unavailable for vehicle %s (%s)",
                vehicle_id,
                type(exc).__name__,
            )
            raise LiveLocationUnavailableError("live-location query is unavailable") from exc
