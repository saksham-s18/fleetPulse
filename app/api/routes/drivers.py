"""Driver REST endpoints."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query, status

from app.api.dependencies import DriverServiceDep
from app.models.enums import DriverStatus
from app.schemas.common import Page
from app.schemas.driver import (
    DriverCreate,
    DriverRead,
    DriverStatusUpdate,
    DriverUpdate,
)

router = APIRouter(prefix="/drivers", tags=["drivers"])


@router.post(
    "",
    response_model=DriverRead,
    status_code=status.HTTP_201_CREATED,
    summary="Create a driver",
)
async def create_driver(
    payload: DriverCreate,
    service: DriverServiceDep,
) -> DriverRead:
    return await service.create(payload)


@router.get(
    "",
    response_model=Page[DriverRead],
    summary="List drivers",
)
async def list_drivers(
    service: DriverServiceDep,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    status_filter: Annotated[DriverStatus | None, Query(alias="status")] = None,
) -> Page[DriverRead]:
    return await service.list(
        page=page,
        page_size=page_size,
        status=status_filter,
    )


@router.get(
    "/{driver_id}",
    response_model=DriverRead,
    summary="Get a driver",
)
async def get_driver(
    driver_id: UUID,
    service: DriverServiceDep,
) -> DriverRead:
    return await service.get(driver_id)


@router.patch(
    "/{driver_id}",
    response_model=DriverRead,
    summary="Update a driver",
)
async def update_driver(
    driver_id: UUID,
    payload: DriverUpdate,
    service: DriverServiceDep,
) -> DriverRead:
    return await service.update(driver_id, payload)


@router.patch(
    "/{driver_id}/status",
    response_model=DriverRead,
    summary="Change a driver's status",
)
async def update_driver_status(
    driver_id: UUID,
    payload: DriverStatusUpdate,
    service: DriverServiceDep,
) -> DriverRead:
    return await service.update_status(driver_id, payload.status)
