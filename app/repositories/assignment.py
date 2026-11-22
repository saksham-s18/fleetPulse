"""Vehicle assignment persistence queries."""

from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute, selectinload
from sqlalchemy.sql.elements import ColumnElement

from app.models.vehicle_assignment import VehicleAssignment


class AssignmentRepository:
    """Reusable database queries for assignment history."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(
        self,
        assignment_id: UUID,
        *,
        with_relations: bool = True,
        for_update: bool = False,
    ) -> VehicleAssignment | None:
        query = select(VehicleAssignment).where(VehicleAssignment.id == assignment_id)
        if with_relations:
            query = query.options(
                selectinload(VehicleAssignment.vehicle),
                selectinload(VehicleAssignment.driver),
                selectinload(VehicleAssignment.shipment),
            ).execution_options(populate_existing=True)
        if for_update:
            query = query.with_for_update()
        result = await self._session.execute(query)
        return result.scalar_one_or_none()

    async def list(
        self,
        *,
        vehicle_id: UUID | None,
        driver_id: UUID | None,
        shipment_id: UUID | None,
        active: bool | None,
        offset: int,
        limit: int,
    ) -> tuple[list[VehicleAssignment], int]:
        filters: list[ColumnElement[bool]] = []
        if vehicle_id is not None:
            filters.append(VehicleAssignment.vehicle_id == vehicle_id)
        if driver_id is not None:
            filters.append(VehicleAssignment.driver_id == driver_id)
        if shipment_id is not None:
            filters.append(VehicleAssignment.shipment_id == shipment_id)
        if active is True:
            filters.append(VehicleAssignment.ended_at.is_(None))
        elif active is False:
            filters.append(VehicleAssignment.ended_at.is_not(None))

        total = await self._session.scalar(
            select(func.count()).select_from(VehicleAssignment).where(*filters)
        )
        result = await self._session.execute(
            select(VehicleAssignment)
            .where(*filters)
            .options(
                selectinload(VehicleAssignment.vehicle),
                selectinload(VehicleAssignment.driver),
                selectinload(VehicleAssignment.shipment),
            )
            .order_by(VehicleAssignment.assigned_at.desc(), VehicleAssignment.id)
            .offset(offset)
            .limit(limit)
        )
        return list(result.scalars().all()), int(total or 0)

    async def has_active_for_vehicle(self, vehicle_id: UUID) -> bool:
        return await self._has_active(VehicleAssignment.vehicle_id, vehicle_id)

    async def has_active_for_driver(self, driver_id: UUID) -> bool:
        return await self._has_active(VehicleAssignment.driver_id, driver_id)

    async def has_active_for_shipment(self, shipment_id: UUID) -> bool:
        return await self._has_active(VehicleAssignment.shipment_id, shipment_id)

    async def _has_active(
        self,
        column: InstrumentedAttribute[UUID],
        value: UUID,
    ) -> bool:
        result = await self._session.scalar(
            select(VehicleAssignment.id)
            .where(column == value, VehicleAssignment.ended_at.is_(None))
            .limit(1)
        )
        return result is not None

    def add(self, assignment: VehicleAssignment) -> None:
        self._session.add(assignment)
