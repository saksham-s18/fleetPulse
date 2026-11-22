"""Domain enumerations persisted as constrained strings in PostgreSQL."""

from enum import Enum, StrEnum


class VehicleStatus(StrEnum):
    """Lifecycle states for a vehicle."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    MAINTENANCE = "maintenance"


class DriverStatus(StrEnum):
    """Availability states for a driver."""

    ACTIVE = "active"
    INACTIVE = "inactive"


class ShipmentStatus(StrEnum):
    """Lifecycle states for a shipment."""

    CREATED = "created"
    ASSIGNED = "assigned"
    IN_TRANSIT = "in_transit"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


def enum_values(enum_type: type[Enum]) -> list[str]:
    """Return the persisted string values for an enum type."""
    return [str(member.value) for member in enum_type]
