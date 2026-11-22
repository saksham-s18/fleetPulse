"""Translate database integrity failures into safe domain errors."""

from sqlalchemy.exc import IntegrityError

from app.core.exceptions import BadRequestError, ConflictError, FleetPulseError

_CONSTRAINT_MESSAGES: dict[str, str] = {
    "uq_vehicles_registration_number": "A vehicle with this registration number already exists.",
    "uq_vehicles_device_id": "A vehicle with this device id already exists.",
    "uq_drivers_license_number": "A driver with this license number already exists.",
    "uq_drivers_phone": "A driver with this phone number already exists.",
    "uq_shipments_reference": "A shipment with this reference already exists.",
    "uq_vehicle_assignments_active_vehicle": "The vehicle already has an active assignment.",
    "uq_vehicle_assignments_active_driver": "The driver already has an active assignment.",
    "uq_vehicle_assignments_active_shipment": "The shipment already has an active assignment.",
}


def _constraint_name(exc: IntegrityError) -> str | None:
    """Extract a PostgreSQL constraint name without exposing driver internals."""
    name = getattr(exc.orig, "constraint_name", None)
    if isinstance(name, str) and name:
        return name

    # Fallback for drivers that do not expose constraint_name as an attribute.
    message = str(exc.orig)
    for candidate in _CONSTRAINT_MESSAGES:
        if candidate in message:
            return candidate
    return None


def translate_integrity_error(exc: IntegrityError) -> FleetPulseError:
    """Return a client-safe error for a database integrity violation."""
    name = _constraint_name(exc)
    if name is not None and name in _CONSTRAINT_MESSAGES:
        return ConflictError(_CONSTRAINT_MESSAGES[name])
    if name is not None and name.startswith("ck_"):
        return BadRequestError("The request violates a data constraint.")
    if name is not None and name.startswith("fk_"):
        return ConflictError("A referenced record no longer exists.")
    return ConflictError("The request conflicts with existing data.")
