"""Driver persistence queries."""

from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from app.models.driver import Driver
from app.models.enums import DriverStatus


class DriverRepository:
    """Reusable database queries for drivers."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, driver_id: UUID) -> Driver | None:
        return await self._session.get(Driver, driver_id)

    async def get_by_license_number(self, license_number: str) -> Driver | None:
        result = await self._session.execute(
            select(Driver).where(Driver.license_number == license_number)
        )
        return result.scalar_one_or_none()

    async def get_by_phone(self, phone: str) -> Driver | None:
        result = await self._session.execute(select(Driver).where(Driver.phone == phone))
        return result.scalar_one_or_none()

    async def list(
        self,
        *,
        status: DriverStatus | None,
        offset: int,
        limit: int,
    ) -> tuple[list[Driver], int]:
        filters: list[ColumnElement[bool]] = []
        if status is not None:
            filters.append(Driver.status == status)

        total = await self._session.scalar(select(func.count()).select_from(Driver).where(*filters))
        result = await self._session.execute(
            select(Driver)
            .where(*filters)
            .order_by(Driver.created_at.desc(), Driver.id)
            .offset(offset)
            .limit(limit)
        )
        return list(result.scalars().all()), int(total or 0)

    def add(self, driver: Driver) -> None:
        self._session.add(driver)
