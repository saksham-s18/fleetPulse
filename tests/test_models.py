"""Unit tests for ORM metadata. These tests do not require PostgreSQL."""

from sqlalchemy import CheckConstraint

from app.models import Base
from app.models.enums import DriverStatus, ShipmentStatus, VehicleStatus


def test_all_domain_tables_are_registered() -> None:
    assert set(Base.metadata.tables) == {
        "drivers",
        "shipments",
        "vehicle_assignments",
        "vehicles",
    }


def test_assignment_uses_partial_unique_indexes() -> None:
    table = Base.metadata.tables["vehicle_assignments"]
    indexes = {str(index.name): index for index in table.indexes}

    for name in (
        "uq_vehicle_assignments_active_vehicle",
        "uq_vehicle_assignments_active_driver",
        "uq_vehicle_assignments_active_shipment",
    ):
        index = indexes[name]
        assert index.unique is True
        where_clause = index.dialect_options["postgresql"]["where"]
        assert where_clause is not None
        assert str(where_clause) == "ended_at IS NULL"


def test_assignment_foreign_keys_restrict_deletion() -> None:
    table = Base.metadata.tables["vehicle_assignments"]
    foreign_keys = {fk.parent.name: fk for fk in table.foreign_keys}

    assert set(foreign_keys) == {"vehicle_id", "driver_id", "shipment_id"}
    assert all(fk.ondelete == "RESTRICT" for fk in foreign_keys.values())


def test_assignment_has_time_range_check() -> None:
    table = Base.metadata.tables["vehicle_assignments"]
    checks = {
        constraint.name: constraint
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint)
    }
    constraint = checks["ck_vehicle_assignments_assignment_time_range"]

    assert "ended_at" in str(constraint.sqltext)
    assert "assigned_at" in str(constraint.sqltext)


def test_status_columns_have_database_checks() -> None:
    expected_checks = {
        "vehicles": "ck_vehicles_vehicle_status",
        "drivers": "ck_drivers_driver_status",
        "shipments": "ck_shipments_shipment_status",
    }

    for table_name, expected_name in expected_checks.items():
        table = Base.metadata.tables[table_name]
        check_names = {
            constraint.name
            for constraint in table.constraints
            if isinstance(constraint, CheckConstraint)
        }
        assert expected_name in check_names


def test_status_enums_expose_expected_values() -> None:
    assert [status.value for status in VehicleStatus] == [
        "active",
        "inactive",
        "maintenance",
    ]
    assert [status.value for status in DriverStatus] == ["active", "inactive"]
    assert [status.value for status in ShipmentStatus] == [
        "created",
        "assigned",
        "in_transit",
        "delivered",
        "cancelled",
    ]
