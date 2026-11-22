"""Unit tests for safe translation of database integrity errors."""

from sqlalchemy.exc import IntegrityError

from app.core.exceptions import BadRequestError, ConflictError
from app.repositories.errors import translate_integrity_error


class _FakeOrig(Exception):
    def __init__(self, message: str, constraint_name: str | None = None) -> None:
        super().__init__(message)
        if constraint_name is not None:
            self.constraint_name = constraint_name


def _integrity_error(message: str, constraint_name: str | None = None) -> IntegrityError:
    return IntegrityError("INSERT ...", {}, _FakeOrig(message, constraint_name))


def test_known_unique_constraint_returns_safe_conflict() -> None:
    error = translate_integrity_error(
        _integrity_error("duplicate key", "uq_vehicles_registration_number")
    )

    assert isinstance(error, ConflictError)
    assert error.message == "A vehicle with this registration number already exists."
    assert "INSERT" not in error.message


def test_active_assignment_constraint_returns_specific_conflict() -> None:
    error = translate_integrity_error(
        _integrity_error("duplicate key", "uq_vehicle_assignments_active_driver")
    )

    assert isinstance(error, ConflictError)
    assert error.message == "The driver already has an active assignment."


def test_constraint_name_can_be_recovered_from_message() -> None:
    error = translate_integrity_error(
        _integrity_error(
            'duplicate key value violates unique constraint "uq_drivers_license_number"'
        )
    )

    assert isinstance(error, ConflictError)
    assert error.message == "A driver with this license number already exists."


def test_check_constraint_returns_bad_request() -> None:
    error = translate_integrity_error(
        _integrity_error("check violation", "ck_vehicles_vehicle_status")
    )

    assert isinstance(error, BadRequestError)
    assert error.message == "The request violates a data constraint."


def test_foreign_key_constraint_returns_conflict() -> None:
    error = translate_integrity_error(
        _integrity_error("foreign key violation", "fk_vehicle_assignments_vehicle_id_vehicles")
    )

    assert isinstance(error, ConflictError)
    assert error.message == "A referenced record no longer exists."


def test_unknown_integrity_error_returns_generic_conflict() -> None:
    error = translate_integrity_error(_integrity_error("something unexpected"))

    assert isinstance(error, ConflictError)
    assert error.message == "The request conflicts with existing data."
