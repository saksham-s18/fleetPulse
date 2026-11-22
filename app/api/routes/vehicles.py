"""Vehicle REST endpoints."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query, status

from app.api.dependencies import VehicleServiceDep
from app.models.enums import VehicleStatus
from app.schemas.common import Page
from app.schemas.vehicle import (
    VehicleCreate,
    VehicleRead,
    VehicleStatusUpdate,
    VehicleUpdate,
)

router = APIRouter(prefix="/vehicles", tags=["vehicles"])


@router.post(
    "",
    response_model=VehicleRead,
    status_code=status.HTTP_201_CREATED,
    summary="Create a vehicle",
)
async def create_vehicle(
    payload: VehicleCreate,
    service: VehicleServiceDep,
) -> VehicleRead:
    return await service.create(payload)


@router.get(
    "",
    response_model=Page[VehicleRead],
    summary="List vehicles",
)
async def list_vehicles(
    service: VehicleServiceDep,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    status_filter: Annotated[VehicleStatus | None, Query(alias="status")] = None,
    vehicle_type: Annotated[str | None, Query(min_length=1, max_length=50)] = None,
) -> Page[VehicleRead]:
    return await service.list(
        page=page,
        page_size=page_size,
        status=status_filter,
        vehicle_type=vehicle_type,
    )


@router.get(
    "/{vehicle_id}",
    response_model=VehicleRead,
    summary="Get a vehicle",
)
async def get_vehicle(
    vehicle_id: UUID,
    service: VehicleServiceDep,
) -> VehicleRead:
    return await service.get(vehicle_id)


@router.patch(
    "/{vehicle_id}",
    response_model=VehicleRead,
    summary="Update a vehicle",
)
async def update_vehicle(
    vehicle_id: UUID,
    payload: VehicleUpdate,
    service: VehicleServiceDep,
) -> VehicleRead:
    return await service.update(vehicle_id, payload)


@router.patch(
    "/{vehicle_id}/status",
    response_model=VehicleRead,
    summary="Change a vehicle's status",
)
async def update_vehicle_status(
    vehicle_id: UUID,
    payload: VehicleStatusUpdate,
    service: VehicleServiceDep,
) -> VehicleRead:
    return await service.update_status(vehicle_id, payload.status)
