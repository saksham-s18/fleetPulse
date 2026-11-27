"""GPS ingestion and Redis live-location read endpoints."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query, status

from app.api.dependencies import GpsIngestServiceDep, LiveLocationServiceDep
from app.core.exceptions import NotFoundError
from app.schemas.errors import ErrorResponse
from app.schemas.gps import GpsEvent, GpsIngestAck
from app.schemas.live_location import (
    DistanceUnit,
    LiveLocationPage,
    LiveLocationRead,
    NearbySearchResponse,
)
from app.services.live_location import MAX_NEARBY_LIMIT, MAX_PAGE_SIZE

router = APIRouter(prefix="/locations", tags=["locations"])

_READ_ERROR_RESPONSES: dict[int | str, dict[str, object]] = {
    404: {"model": ErrorResponse, "description": "No live location exists for the vehicle."},
    422: {"model": ErrorResponse, "description": "A query parameter failed validation."},
    503: {
        "model": ErrorResponse,
        "description": "The Redis live-location projection is unavailable.",
    },
}


@router.post(
    "",
    response_model=GpsIngestAck,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest a GPS event",
)
async def ingest_location(
    payload: GpsEvent,
    service: GpsIngestServiceDep,
) -> GpsIngestAck:
    """Validate a GPS event and accept it through the temporary HTTP path.

    A ``202`` response means the event passed validation and was acknowledged
    by Kafka; it is not durably stored in PostgreSQL yet.
    """
    return await service.ingest(payload)


@router.get(
    "/nearby",
    response_model=NearbySearchResponse,
    summary="Find nearby vehicles",
    description=(
        "Search the Redis GEO index for vehicles within a radius of a centre "
        "point. Results are ordered nearest first. Distances and the echoed "
        "radius use the requested unit. Stale locations are included by "
        "default; set `exclude_stale=true` to drop them from a bounded "
        "candidate window."
    ),
    responses=_READ_ERROR_RESPONSES,
)
async def find_nearby_locations(
    service: LiveLocationServiceDep,
    latitude: Annotated[
        float,
        Query(
            ge=-90,
            le=90,
            description="Centre latitude in decimal degrees (-90 to 90).",
        ),
    ],
    longitude: Annotated[
        float,
        Query(
            ge=-180,
            le=180,
            description="Centre longitude in decimal degrees (-180 to 180).",
        ),
    ],
    radius: Annotated[
        float,
        Query(gt=0, description="Positive search radius expressed in `unit`."),
    ],
    unit: Annotated[
        DistanceUnit,
        Query(description="Distance unit: metres (`m`), kilometres (`km`) or miles (`mi`)."),
    ] = DistanceUnit.KILOMETERS,
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=MAX_NEARBY_LIMIT,
            description="Maximum number of vehicles to return (nearest first).",
        ),
    ] = 50,
    exclude_stale: Annotated[
        bool,
        Query(description="Exclude locations older than the configured stale threshold."),
    ] = False,
) -> NearbySearchResponse:
    """Return indexed vehicles within the radius, nearest first."""
    return await service.find_nearby_vehicles(
        latitude,
        longitude,
        radius,
        unit=unit,
        limit=limit,
        exclude_stale=exclude_stale,
    )


@router.get(
    "",
    response_model=LiveLocationPage,
    summary="List live vehicle locations",
    description=(
        "Return a page of latest locations from the Redis live-location "
        "projection. Ordering is deterministic for a fixed dataset (GEO index "
        "rank). The projection is live, so `total` is an instantaneous count "
        "and a vehicle may move between pages while a client paginates. "
        "Filtering by `is_stale` streams the whole GEO index in bounded "
        "batches; unfiltered paging reads only one page of hashes."
    ),
    responses=_READ_ERROR_RESPONSES,
)
async def list_locations(
    service: LiveLocationServiceDep,
    page: Annotated[int, Query(ge=1, description="1-based page number.")] = 1,
    page_size: Annotated[
        int,
        Query(
            ge=1,
            le=MAX_PAGE_SIZE,
            description="Items per page (default 20, capped by configuration).",
        ),
    ] = 20,
    is_stale: Annotated[
        bool | None,
        Query(description="Optional freshness filter; omit to include stale and fresh."),
    ] = None,
    vehicle_id: Annotated[
        UUID | None,
        Query(description="Optional vehicle UUID filter."),
    ] = None,
) -> LiveLocationPage:
    """Return a page of live locations with freshness metadata."""
    return await service.list_vehicle_locations(
        page=page,
        page_size=page_size,
        is_stale=is_stale,
        vehicle_id=vehicle_id,
    )


@router.get(
    "/{vehicle_id}",
    response_model=LiveLocationRead,
    summary="Get a vehicle's latest location",
    description=(
        "Read one vehicle's latest location from Redis, including freshness "
        "metadata. Returns 404 when the vehicle has never reported a location. "
        "The last known position is kept when a vehicle goes silent; it is "
        "reported as stale rather than removed."
    ),
    responses=_READ_ERROR_RESPONSES,
)
async def get_vehicle_location(
    vehicle_id: UUID,
    service: LiveLocationServiceDep,
) -> LiveLocationRead:
    """Return the latest live location for one vehicle."""
    location = await service.get_vehicle_location(vehicle_id)
    if location is None:
        raise NotFoundError("No live location found for this vehicle.")
    return location
