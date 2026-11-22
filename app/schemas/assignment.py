"""Vehicle assignment request and response schemas."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from app.models.enums import DriverStatus, ShipmentStatus, VehicleStatus
from app.schemas.common import RequestModel
from app.schemas.shipment import LocationRead


class AssignmentCreate(RequestModel):
    """Payload for assigning a vehicle and driver to a shipment."""

    vehicle_id: UUID
    driver_id: UUID
    shipment_id: UUID


class AssignmentVehicleSummary(BaseModel):
    """Compact vehicle data embedded in an assignment response."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    registration_number: str
    vehicle_type: str
    status: VehicleStatus


class AssignmentDriverSummary(BaseModel):
    """Compact driver data embedded in an assignment response."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    full_name: str
    license_number: str
    status: DriverStatus


class AssignmentShipmentSummary(BaseModel):
    """Compact shipment data embedded in an assignment response."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    reference: str
    origin: LocationRead
    destination: LocationRead
    status: ShipmentStatus


class AssignmentRead(BaseModel):
    """Public assignment representation with its related entities."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    vehicle_id: UUID
    driver_id: UUID
    shipment_id: UUID
    assigned_at: datetime
    ended_at: datetime | None
    vehicle: AssignmentVehicleSummary
    driver: AssignmentDriverSummary
    shipment: AssignmentShipmentSummary
