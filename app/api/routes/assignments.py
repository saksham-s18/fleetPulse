"""Vehicle assignment REST endpoints."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query, status

from app.api.dependencies import AssignmentServiceDep
from app.schemas.assignment import AssignmentCreate, AssignmentRead
from app.schemas.common import Page

router = APIRouter(tags=["assignments"])


@router.post(
    "/assignments",
    response_model=AssignmentRead,
    status_code=status.HTTP_201_CREATED,
    summary="Assign a vehicle and driver to a shipment",
)
async def create_assignment(
    payload: AssignmentCreate,
    service: AssignmentServiceDep,
) -> AssignmentRead:
    return await service.create(payload)


@router.get(
    "/assignments",
    response_model=Page[AssignmentRead],
    summary="List assignments",
)
async def list_assignments(
    service: AssignmentServiceDep,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    vehicle_id: UUID | None = None,
    driver_id: UUID | None = None,
    shipment_id: UUID | None = None,
    active: bool | None = None,
) -> Page[AssignmentRead]:
    return await service.list(
        page=page,
        page_size=page_size,
        vehicle_id=vehicle_id,
        driver_id=driver_id,
        shipment_id=shipment_id,
        active=active,
    )


@router.get(
    "/assignments/{assignment_id}",
    response_model=AssignmentRead,
    summary="Get an assignment",
)
async def get_assignment(
    assignment_id: UUID,
    service: AssignmentServiceDep,
) -> AssignmentRead:
    return await service.get(assignment_id)


@router.post(
    "/assignments/{assignment_id}/end",
    response_model=AssignmentRead,
    summary="End an assignment",
)
async def end_assignment(
    assignment_id: UUID,
    service: AssignmentServiceDep,
) -> AssignmentRead:
    return await service.end(assignment_id)


@router.get(
    "/vehicles/{vehicle_id}/assignments",
    response_model=Page[AssignmentRead],
    summary="List a vehicle's assignment history",
)
async def list_vehicle_assignments(
    vehicle_id: UUID,
    service: AssignmentServiceDep,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    active: bool | None = None,
) -> Page[AssignmentRead]:
    return await service.list_for_vehicle(
        vehicle_id,
        page=page,
        page_size=page_size,
        active=active,
    )


@router.get(
    "/drivers/{driver_id}/assignments",
    response_model=Page[AssignmentRead],
    summary="List a driver's assignment history",
)
async def list_driver_assignments(
    driver_id: UUID,
    service: AssignmentServiceDep,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    active: bool | None = None,
) -> Page[AssignmentRead]:
    return await service.list_for_driver(
        driver_id,
        page=page,
        page_size=page_size,
        active=active,
    )


@router.get(
    "/shipments/{shipment_id}/assignments",
    response_model=Page[AssignmentRead],
    summary="List a shipment's assignment history",
)
async def list_shipment_assignments(
    shipment_id: UUID,
    service: AssignmentServiceDep,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    active: bool | None = None,
) -> Page[AssignmentRead]:
    return await service.list_for_shipment(
        shipment_id,
        page=page,
        page_size=page_size,
        active=active,
    )
