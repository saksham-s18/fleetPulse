"""Explicit shipment status transition rules."""

from app.core.exceptions import ConflictError
from app.models.enums import ShipmentStatus

ALLOWED_TRANSITIONS: dict[ShipmentStatus, frozenset[ShipmentStatus]] = {
    ShipmentStatus.CREATED: frozenset({ShipmentStatus.ASSIGNED, ShipmentStatus.CANCELLED}),
    ShipmentStatus.ASSIGNED: frozenset({ShipmentStatus.IN_TRANSIT, ShipmentStatus.CANCELLED}),
    ShipmentStatus.IN_TRANSIT: frozenset({ShipmentStatus.DELIVERED, ShipmentStatus.CANCELLED}),
    ShipmentStatus.DELIVERED: frozenset(),
    ShipmentStatus.CANCELLED: frozenset(),
}


def allowed_transitions(status: ShipmentStatus) -> frozenset[ShipmentStatus]:
    """Return the statuses reachable from ``status``."""
    return ALLOWED_TRANSITIONS[status]


def validate_transition(current: ShipmentStatus, target: ShipmentStatus) -> None:
    """Raise a conflict when a shipment cannot move from current to target."""
    allowed = ALLOWED_TRANSITIONS[current]
    if target in allowed:
        return

    allowed_text = ", ".join(sorted(item.value for item in allowed)) or "none"
    raise ConflictError(
        f"Invalid shipment status transition from '{current.value}' to "
        f"'{target.value}'. Allowed target statuses: {allowed_text}."
    )
