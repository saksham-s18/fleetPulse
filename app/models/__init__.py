"""Central registration point for all SQLAlchemy ORM models.

Importing this package registers every model on ``Base.metadata`` so Alembic
can autogenerate migrations. Add new model modules to the imports below.
"""

from app.infrastructure.database import Base
from app.models.driver import Driver
from app.models.enums import DriverStatus, ShipmentStatus, VehicleStatus
from app.models.shipment import Shipment
from app.models.vehicle import Vehicle
from app.models.vehicle_assignment import VehicleAssignment

__all__ = [
    "Base",
    "Driver",
    "DriverStatus",
    "Shipment",
    "ShipmentStatus",
    "Vehicle",
    "VehicleAssignment",
    "VehicleStatus",
]
