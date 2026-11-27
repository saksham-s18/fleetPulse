"""Business-facing operations for the live vehicle-location projection.

The service maps the shared :class:`~app.schemas.gps.GpsEvent` contract onto
the Redis write model, validates caller arguments and calculates freshness. It
never touches Redis keys or Lua directly; that belongs to
:mod:`app.repositories.live_location`.

The standalone GPS worker reaches this service through
:class:`~app.services.live_location_handler.LiveLocationGpsEventHandler`, which
keeps Kafka retry/DLQ decisions in the consumer pipeline.
"""

import logging
from datetime import UTC, datetime
from uuid import UUID

from app.core.exceptions import InvalidQueryError, ServiceUnavailableError
from app.repositories.live_location import LiveLocationRepository, LiveLocationStoreError
from app.schemas.gps import GpsEvent
from app.schemas.live_location import (
    DistanceUnit,
    GeoCenter,
    LiveLocation,
    LiveLocationPage,
    LiveLocationRead,
    LiveLocationRecord,
    LiveLocationWrite,
    LocationUpdateResult,
    NearbySearchResponse,
    NearbyVehicle,
    NearbyVehicleRead,
)

logger = logging.getLogger(__name__)

MAX_NEARBY_LIMIT = 500
MAX_PAGE_SIZE = 500
MAX_NEARBY_RADIUS_KM = 100.0
KM_PER_MILE = 1.609344


class LiveLocationUnavailableError(ServiceUnavailableError):
    """Raised when the live-location projection cannot be read or written.

    Messages stay safe for logs and API responses: no Redis URL, credentials
    or raw command output. Subclassing the shared service-unavailable error
    lets the API error handlers return the standard 503 envelope.
    """


class LiveLocationService:
    """Validate live-location operations and expose business-facing results."""

    def __init__(
        self,
        repository: LiveLocationRepository,
        *,
        stale_after_seconds: int = 60,
        max_page_size: int = 100,
        max_nearby_radius_km: float = MAX_NEARBY_RADIUS_KM,
        max_nearby_limit: int = MAX_NEARBY_LIMIT,
    ) -> None:
        if stale_after_seconds < 1:
            raise ValueError("stale_after_seconds must be at least 1")
        if not 1 <= max_page_size <= MAX_PAGE_SIZE:
            raise ValueError(f"max_page_size must be between 1 and {MAX_PAGE_SIZE}")
        if not 0 < max_nearby_radius_km <= MAX_NEARBY_RADIUS_KM:
            raise ValueError(f"max_nearby_radius_km must be between 0 and {MAX_NEARBY_RADIUS_KM}")
        if not 1 <= max_nearby_limit <= MAX_NEARBY_LIMIT:
            raise ValueError(f"max_nearby_limit must be between 1 and {MAX_NEARBY_LIMIT}")
        self._repository = repository
        self._stale_after_seconds = stale_after_seconds
        self._max_page_size = max_page_size
        self._max_nearby_radius_km = max_nearby_radius_km
        self._max_nearby_limit = max_nearby_limit

    @property
    def stale_after_seconds(self) -> int:
        """Age threshold after which a location is reported as stale."""
        return self._stale_after_seconds

    @property
    def max_page_size(self) -> int:
        """Largest page size this service accepts."""
        return self._max_page_size

    @property
    def max_nearby_radius_km(self) -> float:
        """Largest nearby search radius this service accepts, in kilometres."""
        return self._max_nearby_radius_km

    @property
    def max_nearby_limit(self) -> int:
        """Largest nearby result limit this service accepts."""
        return self._max_nearby_limit

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
        return self._decorate(record, self._current_time(now))

    async def get_vehicle_location(
        self,
        vehicle_id: UUID,
        *,
        now: datetime | None = None,
    ) -> LiveLocationRead | None:
        """Return the public read model for one vehicle, or ``None``."""
        location = await self.get_location(vehicle_id, now=now)
        if location is None:
            return None
        return LiveLocationRead.from_location(location)

    async def list_vehicle_locations(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        is_stale: bool | None = None,
        vehicle_id: UUID | None = None,
        now: datetime | None = None,
    ) -> LiveLocationPage:
        """Return one page of live locations in deterministic GEO rank order.

        Without a staleness filter the total is the instantaneous GEO index
        cardinality and only one page of hashes is read. With a filter the
        whole index is streamed in bounded batches because freshness lives in
        the hash, not in an index; that path is ``O(N)`` but never loads the
        dataset at once. Neither path is a snapshot across requests.
        """
        if page < 1:
            raise InvalidQueryError("page must be at least 1.")
        if not 1 <= page_size <= self._max_page_size:
            raise InvalidQueryError(f"page_size must be between 1 and {self._max_page_size}.")

        current = self._current_time(now)
        offset = (page - 1) * page_size

        if vehicle_id is not None:
            try:
                record = await self._repository.latest(vehicle_id)
            except LiveLocationStoreError as exc:
                logger.warning(
                    "Live-location list read unavailable for vehicle %s (%s)",
                    vehicle_id,
                    type(exc).__name__,
                )
                raise LiveLocationUnavailableError("live-location read is unavailable") from exc
            matched = [] if record is None else [self._decorate(record, current)]
            if is_stale is not None:
                matched = [item for item in matched if item.stale is is_stale]
            vehicle_items = matched if page == 1 else []
            total = len(matched)
            return LiveLocationPage(
                items=[LiveLocationRead.from_location(item) for item in vehicle_items],
                page=page,
                page_size=page_size,
                total=total,
                total_pages=self._total_pages(total, page_size),
            )

        if is_stale is None:
            try:
                total = await self._repository.count_indexed()
                records = await self._repository.page_indexed(
                    offset=offset,
                    limit=page_size,
                )
            except LiveLocationStoreError as exc:
                logger.warning(
                    "Live-location list read unavailable (%s)",
                    type(exc).__name__,
                )
                raise LiveLocationUnavailableError("live-location read is unavailable") from exc
            page_items = [
                LiveLocationRead.from_location(self._decorate(record, current))
                for record in records
            ]
            return LiveLocationPage(
                items=page_items,
                page=page,
                page_size=page_size,
                total=total,
                total_pages=self._total_pages(total, page_size),
            )

        try:
            filtered_items, filtered_total = await self._filtered_page(
                current=current,
                offset=offset,
                page_size=page_size,
                is_stale=is_stale,
            )
        except LiveLocationStoreError as exc:
            logger.warning(
                "Live-location filtered list unavailable (%s)",
                type(exc).__name__,
            )
            raise LiveLocationUnavailableError("live-location read is unavailable") from exc
        return LiveLocationPage(
            items=filtered_items,
            page=page,
            page_size=page_size,
            total=filtered_total,
            total_pages=self._total_pages(filtered_total, page_size),
        )

    async def _filtered_page(
        self,
        *,
        current: datetime,
        offset: int,
        page_size: int,
        is_stale: bool,
    ) -> tuple[list[LiveLocationRead], int]:
        """Stream the index once, counting matches and collecting one page."""
        items: list[LiveLocationRead] = []
        matched = 0
        async for batch in self._repository.scan_indexed():
            for record in batch:
                location = self._decorate(record, current)
                if location.stale is not is_stale:
                    continue
                if offset <= matched < offset + page_size:
                    items.append(LiveLocationRead.from_location(location))
                matched += 1
        return items, matched

    async def find_nearby_vehicles(
        self,
        latitude: float,
        longitude: float,
        radius: float,
        *,
        unit: DistanceUnit = DistanceUnit.KILOMETERS,
        limit: int = 50,
        exclude_stale: bool = False,
        now: datetime | None = None,
    ) -> NearbySearchResponse:
        """Return vehicles within ``radius`` in the caller's unit.

        Staleness exclusion is applied to a bounded candidate window: when it
        is enabled, up to three times ``limit`` nearest vehicles are read so
        stale entries can be dropped without scanning the whole GEO index.
        """
        if not -90 <= latitude <= 90:
            raise InvalidQueryError("latitude must be between -90 and 90.")
        if not -180 <= longitude <= 180:
            raise InvalidQueryError("longitude must be between -180 and 180.")
        if radius <= 0:
            raise InvalidQueryError("radius must be positive.")
        radius_km = self._radius_to_km(radius, unit)
        if radius_km > self._max_nearby_radius_km:
            raise InvalidQueryError(
                f"radius must not exceed {self._max_nearby_radius_km:g} km "
                f"(requested {radius_km:g} km)."
            )
        if not 1 <= limit <= self._max_nearby_limit:
            raise InvalidQueryError(f"limit must be between 1 and {self._max_nearby_limit}.")

        candidate_limit = limit
        if exclude_stale:
            candidate_limit = min(self._max_nearby_limit, limit * 3)

        try:
            records = await self._repository.nearby_records(
                latitude,
                longitude,
                radius_km,
                limit=candidate_limit,
            )
        except LiveLocationStoreError as exc:
            logger.warning("Live-location radius query unavailable (%s)", type(exc).__name__)
            raise LiveLocationUnavailableError("live-location query is unavailable") from exc

        current = self._current_time(now)
        items: list[NearbyVehicleRead] = []
        for entry in records:
            location = self._decorate(entry.location, current)
            if exclude_stale and location.stale:
                continue
            public = LiveLocationRead.from_location(location)
            items.append(
                NearbyVehicleRead(
                    **public.model_dump(),
                    distance=self._distance_from_km(entry.distance_km, unit),
                )
            )
            if len(items) >= limit:
                break

        return NearbySearchResponse(
            center=GeoCenter(latitude=latitude, longitude=longitude),
            radius=radius,
            unit=unit,
            items=items,
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

    @staticmethod
    def _current_time(now: datetime | None) -> datetime:
        current = now if now is not None else datetime.now(UTC)
        if current.tzinfo is None or current.utcoffset() is None:
            raise ValueError("now must include a timezone offset")
        return current.astimezone(UTC)

    def _decorate(self, record: LiveLocationRecord, current: datetime) -> LiveLocation:
        """Attach the single shared freshness calculation to one record."""
        age_seconds = max(0.0, (current - record.recorded_at).total_seconds())
        return LiveLocation(
            **record.model_dump(),
            stale=age_seconds > self._stale_after_seconds,
            age_seconds=age_seconds,
        )

    @staticmethod
    def _total_pages(total: int, page_size: int) -> int:
        return (total + page_size - 1) // page_size if total else 0

    @staticmethod
    def _radius_to_km(radius: float, unit: DistanceUnit) -> float:
        if unit is DistanceUnit.METERS:
            return radius / 1000.0
        if unit is DistanceUnit.MILES:
            return radius * KM_PER_MILE
        return radius

    @staticmethod
    def _distance_from_km(distance_km: float, unit: DistanceUnit) -> float:
        if unit is DistanceUnit.METERS:
            return distance_km * 1000.0
        if unit is DistanceUnit.MILES:
            return distance_km / KM_PER_MILE
        return distance_km
