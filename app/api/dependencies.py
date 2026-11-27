"""FastAPI dependencies for request-scoped sessions and services."""

from typing import Annotated

from fastapi import Depends, Request
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.infrastructure.database import get_db_session
from app.repositories.live_location import LiveLocationRepository
from app.services.assignment import AssignmentService
from app.services.driver import DriverService
from app.services.gps import GpsIngestService
from app.services.gps_publisher import GpsEventPublisher
from app.services.live_location import LiveLocationService
from app.services.shipment import ShipmentService
from app.services.vehicle import VehicleService

SessionDep = Annotated[AsyncSession, Depends(get_db_session)]


def get_vehicle_service(session: SessionDep) -> VehicleService:
    return VehicleService(session)


def get_driver_service(session: SessionDep) -> DriverService:
    return DriverService(session)


def get_shipment_service(session: SessionDep) -> ShipmentService:
    return ShipmentService(session)


def get_assignment_service(session: SessionDep) -> AssignmentService:
    return AssignmentService(session)


def get_gps_publisher(request: Request) -> GpsEventPublisher:
    """Return the application-scoped publisher created during lifespan startup."""
    publisher: GpsEventPublisher = request.app.state.gps_publisher
    return publisher


def get_gps_ingest_service(
    session: SessionDep,
    publisher: Annotated[GpsEventPublisher, Depends(get_gps_publisher)],
) -> GpsIngestService:
    return GpsIngestService(session, publisher)


def get_redis_client(request: Request) -> Redis:
    """Return the application-scoped Redis client created during lifespan.

    The client owns one connection pool for the whole process; dependencies
    must never create a new Redis connection per request.
    """
    client: Redis = request.app.state.redis_client
    return client


def get_live_location_service(
    client: Annotated[Redis, Depends(get_redis_client)],
) -> LiveLocationService:
    """Build the live-location service around the shared Redis client."""
    settings = get_settings()
    repository = LiveLocationRepository(client, prefix=settings.redis_key_prefix)
    return LiveLocationService(
        repository,
        stale_after_seconds=settings.redis_location_stale_after_seconds,
        max_page_size=settings.redis_location_max_page_size,
        max_nearby_radius_km=settings.redis_location_nearby_max_radius_km,
        max_nearby_limit=settings.redis_location_nearby_max_limit,
    )


VehicleServiceDep = Annotated[VehicleService, Depends(get_vehicle_service)]
DriverServiceDep = Annotated[DriverService, Depends(get_driver_service)]
ShipmentServiceDep = Annotated[ShipmentService, Depends(get_shipment_service)]
AssignmentServiceDep = Annotated[AssignmentService, Depends(get_assignment_service)]
GpsEventPublisherDep = Annotated[GpsEventPublisher, Depends(get_gps_publisher)]
GpsIngestServiceDep = Annotated[GpsIngestService, Depends(get_gps_ingest_service)]
RedisClientDep = Annotated[Redis, Depends(get_redis_client)]
LiveLocationServiceDep = Annotated[LiveLocationService, Depends(get_live_location_service)]
