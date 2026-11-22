"""Vehicle assignment business logic."""

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import ConflictError, NotFoundError
from app.models.driver import Driver
from app.models.enums import DriverStatus, ShipmentStatus, VehicleStatus
from app.models.shipment import Shipment
from app.models.vehicle import Vehicle
from app.models.vehicle_assignment import VehicleAssignment
from app.repositories.assignment import AssignmentRepository
from app.repositories.driver import DriverRepository
from app.repositories.shipment import ShipmentRepository
from app.repositories.vehicle import VehicleRepository
from app.schemas.assignment import AssignmentCreate, AssignmentRead
from app.schemas.common import Page
from app.services.pagination import build_page
from app.services.transactions import commit_session

_RELATION_ATTRIBUTES = ["assigned_at", "ended_at", "vehicle", "driver", "shipment"]


class AssignmentService:
    """Create, query and end vehicle assignments."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._assignments = AssignmentRepository(session)
        self._vehicles = VehicleRepository(session)
        self._drivers = DriverRepository(session)
        self._shipments = ShipmentRepository(session)

    async def create(self, data: AssignmentCreate) -> AssignmentRead:
        vehicle = await self._get_vehicle(data.vehicle_id)
        driver = await self._get_driver(data.driver_id)
        shipment = await self._get_shipment_for_update(data.shipment_id)

        if vehicle.status != VehicleStatus.ACTIVE:
            raise ConflictError("Vehicle is not active and cannot be assigned.")
        if driver.status != DriverStatus.ACTIVE:
            raise ConflictError("Driver is not active and cannot be assigned.")
        if shipment.status != ShipmentStatus.CREATED:
            raise ConflictError(
                "Shipment is not eligible for assignment. "
                "Only shipments in 'created' status can be assigned."
            )

        if await self._assignments.has_active_for_vehicle(vehicle.id):
            raise ConflictError("The vehicle already has an active assignment.")
        if await self._assignments.has_active_for_driver(driver.id):
            raise ConflictError("The driver already has an active assignment.")
        if await self._assignments.has_active_for_shipment(shipment.id):
            raise ConflictError("The shipment already has an active assignment.")

        assignment = VehicleAssignment(
            vehicle=vehicle,
            driver=driver,
            shipment=shipment,
        )
        self._assignments.add(assignment)

        # The status change and the new assignment are committed atomically.
        shipment.status = ShipmentStatus.ASSIGNED
        await commit_session(self._session)
        await self._session.refresh(assignment, attribute_names=_RELATION_ATTRIBUTES)
        return AssignmentRead.model_validate(assignment)

    async def list(
        self,
        *,
        page: int,
        page_size: int,
        vehicle_id: UUID | None = None,
        driver_id: UUID | None = None,
        shipment_id: UUID | None = None,
        active: bool | None = None,
    ) -> Page[AssignmentRead]:
        offset = (page - 1) * page_size
        assignments, total = await self._assignments.list(
            vehicle_id=vehicle_id,
            driver_id=driver_id,
            shipment_id=shipment_id,
            active=active,
            offset=offset,
            limit=page_size,
        )
        items = [AssignmentRead.model_validate(item) for item in assignments]
        return build_page(items, total, page, page_size)

    async def get(self, assignment_id: UUID) -> AssignmentRead:
        assignment = await self._assignments.get(assignment_id)
        if assignment is None:
            raise NotFoundError("Assignment not found.")
        return AssignmentRead.model_validate(assignment)

    async def end(self, assignment_id: UUID) -> AssignmentRead:
        assignment = await self._assignments.get(
            assignment_id,
            with_relations=False,
            for_update=True,
        )
        if assignment is None:
            raise NotFoundError("Assignment not found.")
        if assignment.ended_at is not None:
            raise ConflictError("Assignment has already ended.")

        # Preserve history: the row is updated in place and never deleted.
        ended_at = max(datetime.now(UTC), assignment.assigned_at)
        assignment.ended_at = ended_at

        # Release the shipment so it can be assigned again.
        shipment = await self._shipments.get_for_update(assignment.shipment_id)
        if shipment is not None and shipment.status == ShipmentStatus.ASSIGNED:
            shipment.status = ShipmentStatus.CREATED

        await commit_session(self._session)
        await self._session.refresh(assignment, attribute_names=_RELATION_ATTRIBUTES)
        return AssignmentRead.model_validate(assignment)

    async def list_for_vehicle(
        self,
        vehicle_id: UUID,
        *,
        page: int,
        page_size: int,
        active: bool | None = None,
    ) -> Page[AssignmentRead]:
        if await self._vehicles.get(vehicle_id) is None:
            raise NotFoundError("Vehicle not found.")
        return await self.list(
            page=page,
            page_size=page_size,
            vehicle_id=vehicle_id,
            active=active,
        )

    async def list_for_driver(
        self,
        driver_id: UUID,
        *,
        page: int,
        page_size: int,
        active: bool | None = None,
    ) -> Page[AssignmentRead]:
        if await self._drivers.get(driver_id) is None:
            raise NotFoundError("Driver not found.")
        return await self.list(
            page=page,
            page_size=page_size,
            driver_id=driver_id,
            active=active,
        )

    async def list_for_shipment(
        self,
        shipment_id: UUID,
        *,
        page: int,
        page_size: int,
        active: bool | None = None,
    ) -> Page[AssignmentRead]:
        if await self._shipments.get(shipment_id) is None:
            raise NotFoundError("Shipment not found.")
        return await self.list(
            page=page,
            page_size=page_size,
            shipment_id=shipment_id,
            active=active,
        )

    async def _get_vehicle(self, vehicle_id: UUID) -> Vehicle:
        vehicle = await self._vehicles.get(vehicle_id)
        if vehicle is None:
            raise NotFoundError("Vehicle not found.")
        return vehicle

    async def _get_driver(self, driver_id: UUID) -> Driver:
        driver = await self._drivers.get(driver_id)
        if driver is None:
            raise NotFoundError("Driver not found.")
        return driver

    async def _get_shipment_for_update(self, shipment_id: UUID) -> Shipment:
        shipment = await self._shipments.get_for_update(shipment_id)
        if shipment is None:
            raise NotFoundError("Shipment not found.")
        return shipment
