"""Unit tests for ORM metadata. These tests do not require PostgreSQL."""

from sqlalchemy import CheckConstraint, DateTime

from app.models import Base
from app.models.enums import DriverStatus, ShipmentStatus, VehicleStatus


def test_all_domain_tables_are_registered() -> None:
    assert set(Base.metadata.tables) == {
        "drivers",
        "gps_telemetry",
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


def test_gps_telemetry_uses_event_id_as_primary_key() -> None:
    table = Base.metadata.tables["gps_telemetry"]

    assert [column.name for column in table.primary_key.columns] == ["event_id"]


def test_gps_telemetry_foreign_keys_restrict_deletion() -> None:
    table = Base.metadata.tables["gps_telemetry"]
    foreign_keys = {fk.parent.name: fk for fk in table.foreign_keys}

    assert set(foreign_keys) == {"vehicle_id", "shipment_id"}
    assert all(fk.ondelete == "RESTRICT" for fk in foreign_keys.values())
    assert foreign_keys["vehicle_id"].target_fullname == "vehicles.id"
    assert foreign_keys["shipment_id"].target_fullname == "shipments.id"
    assert foreign_keys["vehicle_id"].parent.nullable is False
    assert foreign_keys["shipment_id"].parent.nullable is True


def test_gps_telemetry_has_range_checks() -> None:
    table = Base.metadata.tables["gps_telemetry"]
    checks = {
        constraint.name: str(constraint.sqltext)
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint)
    }

    assert checks == {
        "ck_gps_telemetry_heading_range": "heading IS NULL OR (heading >= 0 AND heading <= 360)",
        "ck_gps_telemetry_latitude_range": "latitude >= -90 AND latitude <= 90",
        "ck_gps_telemetry_longitude_range": "longitude >= -180 AND longitude <= 180",
        "ck_gps_telemetry_speed_kmh_non_negative": "speed_kmh >= 0",
    }


def test_gps_telemetry_indexes_support_newest_first_history() -> None:
    table = Base.metadata.tables["gps_telemetry"]
    indexes = {str(index.name): index for index in table.indexes}

    vehicle_index = indexes["ix_gps_telemetry_vehicle_recorded_at"]
    assert [str(expression) for expression in vehicle_index.expressions] == [
        "gps_telemetry.vehicle_id",
        "recorded_at DESC",
    ]
    assert vehicle_index.unique is False
    assert vehicle_index.dialect_options["postgresql"].get("where") is None

    shipment_index = indexes["ix_gps_telemetry_shipment_recorded_at"]
    assert [str(expression) for expression in shipment_index.expressions] == [
        "gps_telemetry.shipment_id",
        "recorded_at DESC",
    ]
    assert shipment_index.unique is False
    where_clause = shipment_index.dialect_options["postgresql"]["where"]
    assert str(where_clause) == "shipment_id IS NOT NULL"


def test_gps_telemetry_timestamp_columns_are_timezone_aware() -> None:
    table = Base.metadata.tables["gps_telemetry"]
    recorded_at_type = table.c.recorded_at.type
    received_at_type = table.c.received_at.type

    assert isinstance(recorded_at_type, DateTime)
    assert recorded_at_type.timezone is True
    assert isinstance(received_at_type, DateTime)
    assert received_at_type.timezone is True
    assert table.c.received_at.server_default is not None


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
