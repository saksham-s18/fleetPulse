"""Vehicle persistence queries."""

from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from app.models.enums import VehicleStatus
from app.models.vehicle import Vehicle


class VehicleRepository:
    """Reusable database queries for vehicles."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, vehicle_id: UUID) -> Vehicle | None:
        return await self._session.get(Vehicle, vehicle_id)

    async def get_by_registration_number(self, registration_number: str) -> Vehicle | None:
        result = await self._session.execute(
            select(Vehicle).where(Vehicle.registration_number == registration_number)
        )
        return result.scalar_one_or_none()

    async def get_by_device_id(self, device_id: str) -> Vehicle | None:
        result = await self._session.execute(select(Vehicle).where(Vehicle.device_id == device_id))
        return result.scalar_one_or_none()

    async def list(
        self,
        *,
        status: VehicleStatus | None,
        vehicle_type: str | None,
        offset: int,
        limit: int,
    ) -> tuple[list[Vehicle], int]:
        filters: list[ColumnElement[bool]] = []
        if status is not None:
            filters.append(Vehicle.status == status)
        if vehicle_type is not None:
            filters.append(func.lower(Vehicle.vehicle_type) == vehicle_type.lower())

        total = await self._session.scalar(
            select(func.count()).select_from(Vehicle).where(*filters)
        )
        result = await self._session.execute(
            select(Vehicle)
            .where(*filters)
            .order_by(Vehicle.created_at.desc(), Vehicle.id)
            .offset(offset)
            .limit(limit)
        )
        return list(result.scalars().all()), int(total or 0)

    def add(self, vehicle: Vehicle) -> None:
        self._session.add(vehicle)
