"""GPS event ingestion business rules.

Events are validated against the fleet domain — the vehicle must exist and
be active, and an optional shipment must sit on the vehicle's current active
assignment — and are then published to Kafka through the
:class:`~app.services.gps_publisher.GpsEventPublisher` protocol.

Ordering matters: publication happens only after every validation rule
passes, and ``202 Accepted`` is returned only once the broker acknowledges
the event. A delivery failure maps to ``503`` so a rejected publish can
never look successful.

GPS events are still not persisted in PostgreSQL, so the acknowledgement
keeps ``persisted: false``.
"""

import logging
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import ConflictError, NotFoundError, ServiceUnavailableError
from app.models.enums import VehicleStatus
from app.repositories.assignment import AssignmentRepository
from app.repositories.shipment import ShipmentRepository
from app.repositories.vehicle import VehicleRepository
from app.schemas.gps import GpsEvent, GpsIngestAck
from app.services.gps_publisher import GpsEventPublisher, GpsPublishError

logger = logging.getLogger(__name__)


class GpsIngestService:
    """Validate GPS events and publish them to the ingestion pipeline."""

    def __init__(self, session: AsyncSession, publisher: GpsEventPublisher) -> None:
        self._session = session
        self._publisher = publisher
        self._vehicles = VehicleRepository(session)
        self._shipments = ShipmentRepository(session)
        self._assignments = AssignmentRepository(session)

    async def ingest(self, event: GpsEvent) -> GpsIngestAck:
        vehicle = await self._vehicles.get(event.vehicle_id)
        if vehicle is None:
            raise NotFoundError("Vehicle not found.")
        if vehicle.status != VehicleStatus.ACTIVE:
            raise ConflictError("Vehicle is not active and cannot report GPS events.")

        if event.shipment_id is not None:
            await self._validate_shipment(event.vehicle_id, event.shipment_id)

        await self._publish(event)

        logger.info(
            "Accepted GPS event %s for vehicle %s (published, not persisted)",
            event.event_id,
            event.vehicle_id,
        )
        return GpsIngestAck(
            event_id=event.event_id,
            vehicle_id=event.vehicle_id,
            shipment_id=event.shipment_id,
        )

    async def _validate_shipment(self, vehicle_id: UUID, shipment_id: UUID) -> None:
        shipment = await self._shipments.get(shipment_id)
        if shipment is None:
            raise NotFoundError("Shipment not found.")

        assignment = await self._assignments.get_active_for_vehicle(vehicle_id)
        if assignment is None or assignment.shipment_id != shipment_id:
            raise ConflictError("Shipment is not on the vehicle's active assignment.")

    async def _publish(self, event: GpsEvent) -> None:
        """Hand the validated event to the publisher; never hide failures."""
        try:
            await self._publisher.publish(event)
        except GpsPublishError as exc:
            # Log the event id and exception type only: broker addresses and
            # raw client errors must not reach API clients.
            logger.warning(
                "GPS event %s was not accepted (%s)",
                event.event_id,
                type(exc).__name__,
            )
            raise ServiceUnavailableError(
                "GPS event delivery is unavailable; the event was not accepted."
            ) from exc
