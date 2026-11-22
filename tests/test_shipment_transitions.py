"""Unit tests for explicit shipment status transitions."""

import pytest

from app.core.exceptions import ConflictError
from app.models.enums import ShipmentStatus
from app.services.shipment_transitions import (
    ALLOWED_TRANSITIONS,
    allowed_transitions,
    validate_transition,
)


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (ShipmentStatus.CREATED, ShipmentStatus.ASSIGNED),
        (ShipmentStatus.CREATED, ShipmentStatus.CANCELLED),
        (ShipmentStatus.ASSIGNED, ShipmentStatus.IN_TRANSIT),
        (ShipmentStatus.ASSIGNED, ShipmentStatus.CANCELLED),
        (ShipmentStatus.IN_TRANSIT, ShipmentStatus.DELIVERED),
        (ShipmentStatus.IN_TRANSIT, ShipmentStatus.CANCELLED),
    ],
)
def test_allowed_transitions_do_not_raise(
    current: ShipmentStatus,
    target: ShipmentStatus,
) -> None:
    validate_transition(current, target)


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (ShipmentStatus.CREATED, ShipmentStatus.CREATED),
        (ShipmentStatus.CREATED, ShipmentStatus.IN_TRANSIT),
        (ShipmentStatus.CREATED, ShipmentStatus.DELIVERED),
        (ShipmentStatus.ASSIGNED, ShipmentStatus.CREATED),
        (ShipmentStatus.ASSIGNED, ShipmentStatus.DELIVERED),
        (ShipmentStatus.IN_TRANSIT, ShipmentStatus.CREATED),
        (ShipmentStatus.IN_TRANSIT, ShipmentStatus.ASSIGNED),
        (ShipmentStatus.DELIVERED, ShipmentStatus.CANCELLED),
        (ShipmentStatus.CANCELLED, ShipmentStatus.ASSIGNED),
    ],
)
def test_invalid_transitions_raise_conflict(
    current: ShipmentStatus,
    target: ShipmentStatus,
) -> None:
    with pytest.raises(ConflictError) as exc_info:
        validate_transition(current, target)

    message = str(exc_info.value)
    assert current.value in message
    assert target.value in message


def test_terminal_statuses_have_no_outgoing_transitions() -> None:
    assert allowed_transitions(ShipmentStatus.DELIVERED) == frozenset()
    assert allowed_transitions(ShipmentStatus.CANCELLED) == frozenset()


def test_transition_map_covers_every_status() -> None:
    assert set(ALLOWED_TRANSITIONS) == set(ShipmentStatus)
