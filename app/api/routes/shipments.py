"""Shipment REST endpoints."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query, status

from app.api.dependencies import ShipmentServiceDep
from app.models.enums import ShipmentStatus
from app.schemas.common import Page
from app.schemas.shipment import (
    ShipmentCreate,
    ShipmentRead,
    ShipmentStatusUpdate,
    ShipmentUpdate,
)

router = APIRouter(prefix="/shipments", tags=["shipments"])


@router.post(
    "",
    response_model=ShipmentRead,
    status_code=status.HTTP_201_CREATED,
    summary="Create a shipment",
)
async def create_shipment(
    payload: ShipmentCreate,
    service: ShipmentServiceDep,
) -> ShipmentRead:
    return await service.create(payload)


@router.get(
    "",
    response_model=Page[ShipmentRead],
    summary="List shipments",
)
async def list_shipments(
    service: ShipmentServiceDep,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    status_filter: Annotated[ShipmentStatus | None, Query(alias="status")] = None,
) -> Page[ShipmentRead]:
    return await service.list(
        page=page,
        page_size=page_size,
        status=status_filter,
    )


@router.get(
    "/{shipment_id}",
    response_model=ShipmentRead,
    summary="Get a shipment",
)
async def get_shipment(
    shipment_id: UUID,
    service: ShipmentServiceDep,
) -> ShipmentRead:
    return await service.get(shipment_id)


@router.patch(
    "/{shipment_id}",
    response_model=ShipmentRead,
    summary="Update a shipment",
)
async def update_shipment(
    shipment_id: UUID,
    payload: ShipmentUpdate,
    service: ShipmentServiceDep,
) -> ShipmentRead:
    return await service.update(shipment_id, payload)


@router.patch(
    "/{shipment_id}/status",
    response_model=ShipmentRead,
    summary="Change a shipment's status",
)
async def update_shipment_status(
    shipment_id: UUID,
    payload: ShipmentStatusUpdate,
    service: ShipmentServiceDep,
) -> ShipmentRead:
    return await service.update_status(shipment_id, payload.status)
