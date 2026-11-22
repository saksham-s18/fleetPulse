"""Shipment persistence queries."""

from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from app.models.enums import ShipmentStatus
from app.models.shipment import Shipment


class ShipmentRepository:
    """Reusable database queries for shipments."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, shipment_id: UUID) -> Shipment | None:
        return await self._session.get(Shipment, shipment_id)

    async def get_for_update(self, shipment_id: UUID) -> Shipment | None:
        result = await self._session.execute(
            select(Shipment).where(Shipment.id == shipment_id).with_for_update()
        )
        return result.scalar_one_or_none()

    async def get_by_reference(self, reference: str) -> Shipment | None:
        result = await self._session.execute(
            select(Shipment).where(Shipment.reference == reference)
        )
        return result.scalar_one_or_none()

    async def list(
        self,
        *,
        status: ShipmentStatus | None,
        offset: int,
        limit: int,
    ) -> tuple[list[Shipment], int]:
        filters: list[ColumnElement[bool]] = []
        if status is not None:
            filters.append(Shipment.status == status)

        total = await self._session.scalar(
            select(func.count()).select_from(Shipment).where(*filters)
        )
        result = await self._session.execute(
            select(Shipment)
            .where(*filters)
            .order_by(Shipment.created_at.desc(), Shipment.id)
            .offset(offset)
            .limit(limit)
        )
        return list(result.scalars().all()), int(total or 0)

    def add(self, shipment: Shipment) -> None:
        self._session.add(shipment)
