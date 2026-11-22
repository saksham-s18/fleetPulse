"""FastAPI dependencies for request-scoped sessions and services."""

from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.database import get_db_session
from app.services.assignment import AssignmentService
from app.services.driver import DriverService
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


VehicleServiceDep = Annotated[VehicleService, Depends(get_vehicle_service)]
DriverServiceDep = Annotated[DriverService, Depends(get_driver_service)]
ShipmentServiceDep = Annotated[ShipmentService, Depends(get_shipment_service)]
AssignmentServiceDep = Annotated[AssignmentService, Depends(get_assignment_service)]
