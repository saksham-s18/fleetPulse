"""GPS event ingestion business rules.

Milestone 5B exposes a temporary HTTP acceptance path. Events are validated
against the fleet domain — the vehicle must exist and be active, and an
optional shipment must sit on the vehicle's current active assignment — but
nothing is persisted: there is no GPS table, broker or outbox yet.

Validation and acceptance are deliberately separate steps. The next milestone
can publish to Kafka inside :meth:`GpsIngestService._accept` without touching
the rules below or the API contract.
"""

import logging
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import ConflictError, NotFoundError
from app.models.enums import VehicleStatus
from app.repositories.assignment import AssignmentRepository
from app.repositories.shipment import ShipmentRepository
from app.repositories.vehicle import VehicleRepository
from app.schemas.gps import GpsEvent, GpsIngestAck

logger = logging.getLogger(__name__)


class GpsIngestService:
    """Validate GPS events and hand them to the temporary acceptance path."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
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

        return self._accept(event)

    async def _validate_shipment(self, vehicle_id: UUID, shipment_id: UUID) -> None:
        shipment = await self._shipments.get(shipment_id)
        if shipment is None:
            raise NotFoundError("Shipment not found.")

        assignment = await self._assignments.get_active_for_vehicle(vehicle_id)
        if assignment is None or assignment.shipment_id != shipment_id:
            raise ConflictError("Shipment is not on the vehicle's active assignment.")

    def _accept(self, event: GpsEvent) -> GpsIngestAck:
        """Temporary acceptance path: the event is validated but not stored.

        Replacing this log line with durable delivery (Kafka publishing) is
        the work of the next milestone; validation above stays unchanged.
        """
        logger.info(
            "Accepted GPS event %s for vehicle %s (not persisted)",
            event.event_id,
            event.vehicle_id,
        )
        return GpsIngestAck(
            event_id=event.event_id,
            vehicle_id=event.vehicle_id,
            shipment_id=event.shipment_id,
        )
