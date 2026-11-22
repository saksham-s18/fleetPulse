"""Shipment business logic and status transitions."""

from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import BadRequestError, ConflictError, NotFoundError
from app.models.enums import ShipmentStatus
from app.models.shipment import Shipment
from app.repositories.shipment import ShipmentRepository
from app.schemas.common import Location, Page
from app.schemas.shipment import ShipmentCreate, ShipmentRead, ShipmentUpdate
from app.services.pagination import build_page
from app.services.shipment_transitions import validate_transition
from app.services.transactions import commit_session


class ShipmentService:
    """Create, query and update shipments."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._shipments = ShipmentRepository(session)

    async def create(self, data: ShipmentCreate) -> ShipmentRead:
        await self._ensure_reference_available(data.reference)
        shipment = Shipment(
            reference=data.reference,
            origin=_location_dict(data.origin),
            destination=_location_dict(data.destination),
            status=ShipmentStatus.CREATED,
            expected_delivery_at=data.expected_delivery_at,
        )
        self._shipments.add(shipment)
        await commit_session(self._session)
        await self._session.refresh(shipment)
        return ShipmentRead.model_validate(shipment)

    async def list(
        self,
        *,
        page: int,
        page_size: int,
        status: ShipmentStatus | None = None,
    ) -> Page[ShipmentRead]:
        offset = (page - 1) * page_size
        shipments, total = await self._shipments.list(
            status=status,
            offset=offset,
            limit=page_size,
        )
        items = [ShipmentRead.model_validate(shipment) for shipment in shipments]
        return build_page(items, total, page, page_size)

    async def get(self, shipment_id: UUID) -> ShipmentRead:
        shipment = await self._get_entity(shipment_id)
        return ShipmentRead.model_validate(shipment)

    async def update(self, shipment_id: UUID, data: ShipmentUpdate) -> ShipmentRead:
        shipment = await self._get_entity(shipment_id)
        changes: dict[str, Any] = data.model_dump(exclude_unset=True)
        if not changes:
            raise BadRequestError("No fields were provided for update.")

        for field in ("reference", "origin", "destination"):
            if field in changes and changes[field] is None:
                raise BadRequestError(f"'{field}' cannot be null.")

        if "reference" in changes:
            await self._ensure_reference_available(str(changes["reference"]), shipment=shipment)
        if "origin" in changes and data.origin is not None:
            changes["origin"] = _location_dict(data.origin)
        if "destination" in changes and data.destination is not None:
            changes["destination"] = _location_dict(data.destination)

        for field, value in changes.items():
            setattr(shipment, field, value)

        await commit_session(self._session)
        await self._session.refresh(shipment)
        return ShipmentRead.model_validate(shipment)

    async def update_status(
        self,
        shipment_id: UUID,
        status: ShipmentStatus,
    ) -> ShipmentRead:
        shipment = await self._shipments.get_for_update(shipment_id)
        if shipment is None:
            raise NotFoundError("Shipment not found.")

        validate_transition(shipment.status, status)
        shipment.status = status
        await commit_session(self._session)
        await self._session.refresh(shipment)
        return ShipmentRead.model_validate(shipment)

    async def _get_entity(self, shipment_id: UUID) -> Shipment:
        shipment = await self._shipments.get(shipment_id)
        if shipment is None:
            raise NotFoundError("Shipment not found.")
        return shipment

    async def _ensure_reference_available(
        self,
        reference: str,
        *,
        shipment: Shipment | None = None,
    ) -> None:
        existing = await self._shipments.get_by_reference(reference)
        if existing is not None and (shipment is None or existing.id != shipment.id):
            raise ConflictError("A shipment with this reference already exists.")


def _location_dict(location: Location) -> dict[str, Any]:
    return location.model_dump()
