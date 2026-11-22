"""Driver business logic."""

from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import BadRequestError, ConflictError, NotFoundError
from app.models.driver import Driver
from app.models.enums import DriverStatus
from app.repositories.driver import DriverRepository
from app.schemas.common import Page
from app.schemas.driver import DriverCreate, DriverRead, DriverUpdate
from app.services.pagination import build_page
from app.services.transactions import commit_session


class DriverService:
    """Create, query and update drivers."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._drivers = DriverRepository(session)

    async def create(self, data: DriverCreate) -> DriverRead:
        await self._ensure_unique(data.license_number, data.phone)
        driver = Driver(
            full_name=data.full_name,
            phone=data.phone,
            license_number=data.license_number,
            status=data.status,
        )
        self._drivers.add(driver)
        await commit_session(self._session)
        await self._session.refresh(driver)
        return DriverRead.model_validate(driver)

    async def list(
        self,
        *,
        page: int,
        page_size: int,
        status: DriverStatus | None = None,
    ) -> Page[DriverRead]:
        offset = (page - 1) * page_size
        drivers, total = await self._drivers.list(
            status=status,
            offset=offset,
            limit=page_size,
        )
        items = [DriverRead.model_validate(driver) for driver in drivers]
        return build_page(items, total, page, page_size)

    async def get(self, driver_id: UUID) -> DriverRead:
        driver = await self._get_entity(driver_id)
        return DriverRead.model_validate(driver)

    async def update(self, driver_id: UUID, data: DriverUpdate) -> DriverRead:
        driver = await self._get_entity(driver_id)
        changes: dict[str, Any] = data.model_dump(exclude_unset=True)
        if not changes:
            raise BadRequestError("No fields were provided for update.")

        for field in ("full_name", "license_number", "status"):
            if field in changes and changes[field] is None:
                raise BadRequestError(f"'{field}' cannot be null.")

        if "license_number" in changes:
            await self._ensure_license_available(str(changes["license_number"]), driver)
        if "phone" in changes and changes["phone"] is not None:
            await self._ensure_phone_available(str(changes["phone"]), driver)

        for field, value in changes.items():
            setattr(driver, field, value)

        await commit_session(self._session)
        await self._session.refresh(driver)
        return DriverRead.model_validate(driver)

    async def update_status(self, driver_id: UUID, status: DriverStatus) -> DriverRead:
        driver = await self._get_entity(driver_id)
        driver.status = status
        await commit_session(self._session)
        await self._session.refresh(driver)
        return DriverRead.model_validate(driver)

    async def _get_entity(self, driver_id: UUID) -> Driver:
        driver = await self._drivers.get(driver_id)
        if driver is None:
            raise NotFoundError("Driver not found.")
        return driver

    async def _ensure_unique(self, license_number: str, phone: str | None) -> None:
        if await self._drivers.get_by_license_number(license_number) is not None:
            raise ConflictError("A driver with this license number already exists.")
        if phone is not None and await self._drivers.get_by_phone(phone) is not None:
            raise ConflictError("A driver with this phone number already exists.")

    async def _ensure_license_available(self, license_number: str, driver: Driver) -> None:
        existing = await self._drivers.get_by_license_number(license_number)
        if existing is not None and existing.id != driver.id:
            raise ConflictError("A driver with this license number already exists.")

    async def _ensure_phone_available(self, phone: str, driver: Driver) -> None:
        existing = await self._drivers.get_by_phone(phone)
        if existing is not None and existing.id != driver.id:
            raise ConflictError("A driver with this phone number already exists.")
