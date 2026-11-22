"""Vehicle business logic."""

from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import BadRequestError, ConflictError, NotFoundError
from app.models.enums import VehicleStatus
from app.models.vehicle import Vehicle
from app.repositories.vehicle import VehicleRepository
from app.schemas.common import Page
from app.schemas.vehicle import VehicleCreate, VehicleRead, VehicleUpdate
from app.services.pagination import build_page
from app.services.transactions import commit_session


class VehicleService:
    """Create, query and update vehicles."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._vehicles = VehicleRepository(session)

    async def create(self, data: VehicleCreate) -> VehicleRead:
        await self._ensure_unique(data.registration_number, data.device_id)
        vehicle = Vehicle(
            registration_number=data.registration_number,
            vehicle_type=data.vehicle_type,
            status=data.status,
            device_id=data.device_id,
        )
        self._vehicles.add(vehicle)
        await commit_session(self._session)
        await self._session.refresh(vehicle)
        return VehicleRead.model_validate(vehicle)

    async def list(
        self,
        *,
        page: int,
        page_size: int,
        status: VehicleStatus | None = None,
        vehicle_type: str | None = None,
    ) -> Page[VehicleRead]:
        offset = (page - 1) * page_size
        vehicles, total = await self._vehicles.list(
            status=status,
            vehicle_type=vehicle_type,
            offset=offset,
            limit=page_size,
        )
        items = [VehicleRead.model_validate(vehicle) for vehicle in vehicles]
        return build_page(items, total, page, page_size)

    async def get(self, vehicle_id: UUID) -> VehicleRead:
        vehicle = await self._get_entity(vehicle_id)
        return VehicleRead.model_validate(vehicle)

    async def update(self, vehicle_id: UUID, data: VehicleUpdate) -> VehicleRead:
        vehicle = await self._get_entity(vehicle_id)
        changes: dict[str, Any] = data.model_dump(exclude_unset=True)
        if not changes:
            raise BadRequestError("No fields were provided for update.")

        for field in ("registration_number", "vehicle_type", "status"):
            if field in changes and changes[field] is None:
                raise BadRequestError(f"'{field}' cannot be null.")

        if "registration_number" in changes:
            await self._ensure_registration_available(str(changes["registration_number"]), vehicle)
        if "device_id" in changes and changes["device_id"] is not None:
            await self._ensure_device_available(str(changes["device_id"]), vehicle)

        for field, value in changes.items():
            setattr(vehicle, field, value)

        await commit_session(self._session)
        await self._session.refresh(vehicle)
        return VehicleRead.model_validate(vehicle)

    async def update_status(self, vehicle_id: UUID, status: VehicleStatus) -> VehicleRead:
        vehicle = await self._get_entity(vehicle_id)
        vehicle.status = status
        await commit_session(self._session)
        await self._session.refresh(vehicle)
        return VehicleRead.model_validate(vehicle)

    async def _get_entity(self, vehicle_id: UUID) -> Vehicle:
        vehicle = await self._vehicles.get(vehicle_id)
        if vehicle is None:
            raise NotFoundError("Vehicle not found.")
        return vehicle

    async def _ensure_unique(
        self,
        registration_number: str,
        device_id: str | None,
    ) -> None:
        if await self._vehicles.get_by_registration_number(registration_number) is not None:
            raise ConflictError("A vehicle with this registration number already exists.")
        if device_id is not None and await self._vehicles.get_by_device_id(device_id) is not None:
            raise ConflictError("A vehicle with this device id already exists.")

    async def _ensure_registration_available(
        self,
        registration_number: str,
        vehicle: Vehicle,
    ) -> None:
        existing = await self._vehicles.get_by_registration_number(registration_number)
        if existing is not None and existing.id != vehicle.id:
            raise ConflictError("A vehicle with this registration number already exists.")

    async def _ensure_device_available(self, device_id: str, vehicle: Vehicle) -> None:
        existing = await self._vehicles.get_by_device_id(device_id)
        if existing is not None and existing.id != vehicle.id:
            raise ConflictError("A vehicle with this device id already exists.")
