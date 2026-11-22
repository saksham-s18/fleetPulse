"""Integration tests for domain models against real PostgreSQL.

Run with a migrated database:

    docker compose up -d postgres
    alembic upgrade head
    pytest -m integration
"""

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.config import get_settings
from app.infrastructure.database import create_engine, create_session_factory
from app.models import Driver, Shipment, Vehicle, VehicleAssignment
from app.models.enums import DriverStatus, ShipmentStatus, VehicleStatus

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def session() -> AsyncIterator[AsyncSession]:
    """Yield an async session bound to the configured PostgreSQL database."""
    engine = create_engine(get_settings())
    session_factory = create_session_factory(engine)
    try:
        async with session_factory() as database_session:
            yield database_session
    finally:
        await engine.dispose()


def unique(prefix: str) -> str:
    """Return a unique value so tests do not depend on an empty database."""
    return f"{prefix}-{uuid4().hex[:12]}"


async def create_vehicle(
    session: AsyncSession,
    registration_number: str | None = None,
) -> Vehicle:
    vehicle = Vehicle(
        registration_number=registration_number or unique("REG"),
        vehicle_type="van",
        status=VehicleStatus.ACTIVE,
        device_id=unique("DEV"),
    )
    session.add(vehicle)
    await session.commit()
    await session.refresh(vehicle)
    return vehicle


async def create_driver(
    session: AsyncSession,
    license_number: str | None = None,
) -> Driver:
    driver = Driver(
        full_name="Test Driver",
        phone=unique("PHONE"),
        license_number=license_number or unique("LIC"),
        status=DriverStatus.ACTIVE,
    )
    session.add(driver)
    await session.commit()
    await session.refresh(driver)
    return driver


async def create_shipment(
    session: AsyncSession,
    reference: str | None = None,
) -> Shipment:
    shipment = Shipment(
        reference=reference or unique("SHP"),
        origin={"city": "Berlin", "latitude": 52.52, "longitude": 13.405},
        destination={"city": "Hamburg", "latitude": 53.551, "longitude": 9.993},
        status=ShipmentStatus.CREATED,
        expected_delivery_at=datetime.now(UTC) + timedelta(hours=4),
    )
    session.add(shipment)
    await session.commit()
    await session.refresh(shipment)
    return shipment


async def create_assignment(
    session: AsyncSession,
    vehicle: Vehicle,
    driver: Driver,
    shipment: Shipment,
    *,
    assigned_at: datetime | None = None,
    ended_at: datetime | None = None,
) -> VehicleAssignment:
    assignment = VehicleAssignment(
        vehicle_id=vehicle.id,
        driver_id=driver.id,
        shipment_id=shipment.id,
        assigned_at=assigned_at or datetime.now(UTC),
        ended_at=ended_at,
    )
    session.add(assignment)
    await session.commit()
    await session.refresh(assignment)
    return assignment


@pytest.mark.asyncio
async def test_create_vehicle(session: AsyncSession) -> None:
    vehicle = await create_vehicle(session)

    assert isinstance(vehicle.id, UUID)
    assert vehicle.registration_number.startswith("REG-")
    assert vehicle.status == VehicleStatus.ACTIVE
    assert vehicle.created_at.tzinfo is not None
    assert vehicle.updated_at.tzinfo is not None


@pytest.mark.asyncio
async def test_create_driver(session: AsyncSession) -> None:
    driver = await create_driver(session)

    assert isinstance(driver.id, UUID)
    assert driver.status == DriverStatus.ACTIVE
    assert driver.created_at.tzinfo is not None
    assert driver.updated_at.tzinfo is not None


@pytest.mark.asyncio
async def test_create_shipment(session: AsyncSession) -> None:
    shipment = await create_shipment(session)

    assert isinstance(shipment.id, UUID)
    assert shipment.origin["city"] == "Berlin"
    assert shipment.destination["city"] == "Hamburg"
    assert shipment.status == ShipmentStatus.CREATED
    assert shipment.expected_delivery_at is not None
    assert shipment.expected_delivery_at.tzinfo is not None


@pytest.mark.asyncio
async def test_create_assignment_and_load_relationships(session: AsyncSession) -> None:
    vehicle = await create_vehicle(session)
    driver = await create_driver(session)
    shipment = await create_shipment(session)

    assignment = await create_assignment(session, vehicle, driver, shipment)

    result = await session.execute(
        select(VehicleAssignment)
        .where(VehicleAssignment.id == assignment.id)
        .options(
            selectinload(VehicleAssignment.vehicle),
            selectinload(VehicleAssignment.driver),
            selectinload(VehicleAssignment.shipment),
        )
    )
    loaded = result.scalar_one()

    assert loaded.ended_at is None
    assert loaded.vehicle.id == vehicle.id
    assert loaded.driver.id == driver.id
    assert loaded.shipment.id == shipment.id


@pytest.mark.asyncio
async def test_registration_number_must_be_unique(session: AsyncSession) -> None:
    existing = await create_vehicle(session)
    duplicate = Vehicle(
        registration_number=existing.registration_number,
        vehicle_type="truck",
    )
    session.add(duplicate)

    with pytest.raises(IntegrityError):
        await session.commit()

    await session.rollback()


@pytest.mark.asyncio
async def test_driver_license_number_must_be_unique(session: AsyncSession) -> None:
    existing = await create_driver(session)
    duplicate = Driver(
        full_name="Another Driver",
        license_number=existing.license_number,
    )
    session.add(duplicate)

    with pytest.raises(IntegrityError):
        await session.commit()

    await session.rollback()


@pytest.mark.asyncio
async def test_shipment_reference_must_be_unique(session: AsyncSession) -> None:
    existing = await create_shipment(session)
    duplicate = Shipment(
        reference=existing.reference,
        origin={"city": "Berlin"},
        destination={"city": "Munich"},
    )
    session.add(duplicate)

    with pytest.raises(IntegrityError):
        await session.commit()

    await session.rollback()


@pytest.mark.asyncio
async def test_device_id_must_be_unique_when_present(session: AsyncSession) -> None:
    device_id = unique("DEV")
    session.add_all(
        [
            Vehicle(
                registration_number=unique("REG"),
                vehicle_type="van",
                device_id=device_id,
            ),
            Vehicle(
                registration_number=unique("REG"),
                vehicle_type="van",
                device_id=device_id,
            ),
        ]
    )

    with pytest.raises(IntegrityError):
        await session.commit()

    await session.rollback()


@pytest.mark.asyncio
async def test_phone_must_be_unique_when_present(session: AsyncSession) -> None:
    phone = unique("PHONE")
    session.add_all(
        [
            Driver(
                full_name="First Driver",
                phone=phone,
                license_number=unique("LIC"),
            ),
            Driver(
                full_name="Second Driver",
                phone=phone,
                license_number=unique("LIC"),
            ),
        ]
    )

    with pytest.raises(IntegrityError):
        await session.commit()

    await session.rollback()


@pytest.mark.asyncio
async def test_null_optional_unique_values_are_allowed(session: AsyncSession) -> None:
    session.add_all(
        [
            Vehicle(
                registration_number=unique("REG"),
                vehicle_type="van",
                device_id=None,
            ),
            Vehicle(
                registration_number=unique("REG"),
                vehicle_type="truck",
                device_id=None,
            ),
            Driver(
                full_name="First Driver",
                phone=None,
                license_number=unique("LIC"),
            ),
            Driver(
                full_name="Second Driver",
                phone=None,
                license_number=unique("LIC"),
            ),
        ]
    )

    await session.commit()


@pytest.mark.parametrize("status", list(VehicleStatus), ids=lambda value: value.value)
@pytest.mark.asyncio
async def test_valid_vehicle_statuses(session: AsyncSession, status: VehicleStatus) -> None:
    vehicle = Vehicle(
        registration_number=unique("REG"),
        vehicle_type="van",
        status=status,
    )
    session.add(vehicle)
    await session.commit()
    await session.refresh(vehicle)

    assert vehicle.status == status


@pytest.mark.parametrize("status", list(DriverStatus), ids=lambda value: value.value)
@pytest.mark.asyncio
async def test_valid_driver_statuses(session: AsyncSession, status: DriverStatus) -> None:
    driver = Driver(
        full_name="Test Driver",
        license_number=unique("LIC"),
        status=status,
    )
    session.add(driver)
    await session.commit()
    await session.refresh(driver)

    assert driver.status == status


@pytest.mark.parametrize("status", list(ShipmentStatus), ids=lambda value: value.value)
@pytest.mark.asyncio
async def test_valid_shipment_statuses(
    session: AsyncSession,
    status: ShipmentStatus,
) -> None:
    shipment = Shipment(
        reference=unique("SHP"),
        origin={"city": "Berlin"},
        destination={"city": "Hamburg"},
        status=status,
    )
    session.add(shipment)
    await session.commit()
    await session.refresh(shipment)

    assert shipment.status == status


@pytest.mark.asyncio
async def test_invalid_vehicle_status_is_rejected_by_database(
    session: AsyncSession,
) -> None:
    with pytest.raises(IntegrityError):
        await session.execute(
            text(
                """
                INSERT INTO vehicles (id, registration_number, vehicle_type, status)
                VALUES (CAST(:id AS uuid), :registration_number, 'van', 'flying')
                """
            ),
            {"id": str(uuid4()), "registration_number": unique("REG")},
        )
        await session.commit()

    await session.rollback()


@pytest.mark.asyncio
async def test_invalid_driver_status_is_rejected_by_database(
    session: AsyncSession,
) -> None:
    with pytest.raises(IntegrityError):
        await session.execute(
            text(
                """
                INSERT INTO drivers (id, full_name, license_number, status)
                VALUES (CAST(:id AS uuid), 'Test Driver', :license_number, 'suspended')
                """
            ),
            {"id": str(uuid4()), "license_number": unique("LIC")},
        )
        await session.commit()

    await session.rollback()


@pytest.mark.asyncio
async def test_invalid_shipment_status_is_rejected_by_database(
    session: AsyncSession,
) -> None:
    with pytest.raises(IntegrityError):
        await session.execute(
            text(
                """
                INSERT INTO shipments (id, reference, origin, destination, status)
                VALUES (
                    CAST(:id AS uuid),
                    :reference,
                    '{}'::jsonb,
                    '{}'::jsonb,
                    'lost'
                )
                """
            ),
            {"id": str(uuid4()), "reference": unique("SHP")},
        )
        await session.commit()

    await session.rollback()


@pytest.mark.parametrize("invalid_field", ["vehicle_id", "driver_id", "shipment_id"])
@pytest.mark.asyncio
async def test_assignment_foreign_keys_are_enforced(
    session: AsyncSession,
    invalid_field: str,
) -> None:
    vehicle = await create_vehicle(session)
    driver = await create_driver(session)
    shipment = await create_shipment(session)

    values = {
        "vehicle_id": vehicle.id,
        "driver_id": driver.id,
        "shipment_id": shipment.id,
    }
    values[invalid_field] = uuid4()

    assignment = VehicleAssignment(
        vehicle_id=values["vehicle_id"],
        driver_id=values["driver_id"],
        shipment_id=values["shipment_id"],
        assigned_at=datetime.now(UTC),
    )
    session.add(assignment)

    with pytest.raises(IntegrityError):
        await session.commit()

    await session.rollback()


@pytest.mark.asyncio
async def test_ending_an_assignment_sets_ended_at(session: AsyncSession) -> None:
    vehicle = await create_vehicle(session)
    driver = await create_driver(session)
    shipment = await create_shipment(session)
    assignment = await create_assignment(session, vehicle, driver, shipment)
    ended_at = assignment.assigned_at + timedelta(hours=1)

    assignment.ended_at = ended_at
    await session.commit()
    await session.refresh(assignment)

    assert assignment.ended_at == ended_at


@pytest.mark.asyncio
async def test_assignment_end_must_not_precede_start(session: AsyncSession) -> None:
    vehicle = await create_vehicle(session)
    driver = await create_driver(session)
    shipment = await create_shipment(session)
    assigned_at = datetime.now(UTC)

    assignment = VehicleAssignment(
        vehicle_id=vehicle.id,
        driver_id=driver.id,
        shipment_id=shipment.id,
        assigned_at=assigned_at,
        ended_at=assigned_at - timedelta(seconds=1),
    )
    session.add(assignment)

    with pytest.raises(IntegrityError):
        await session.commit()

    await session.rollback()


@pytest.mark.asyncio
async def test_only_one_active_assignment_per_vehicle(session: AsyncSession) -> None:
    vehicle = await create_vehicle(session)
    first_driver = await create_driver(session)
    second_driver = await create_driver(session)
    first_shipment = await create_shipment(session)
    second_shipment = await create_shipment(session)
    assigned_at = datetime.now(UTC)

    session.add_all(
        [
            VehicleAssignment(
                vehicle_id=vehicle.id,
                driver_id=first_driver.id,
                shipment_id=first_shipment.id,
                assigned_at=assigned_at,
            ),
            VehicleAssignment(
                vehicle_id=vehicle.id,
                driver_id=second_driver.id,
                shipment_id=second_shipment.id,
                assigned_at=assigned_at,
            ),
        ]
    )

    with pytest.raises(IntegrityError):
        await session.commit()

    await session.rollback()


@pytest.mark.asyncio
async def test_only_one_active_assignment_per_driver(session: AsyncSession) -> None:
    driver = await create_driver(session)
    first_vehicle = await create_vehicle(session)
    second_vehicle = await create_vehicle(session)
    first_shipment = await create_shipment(session)
    second_shipment = await create_shipment(session)
    assigned_at = datetime.now(UTC)

    session.add_all(
        [
            VehicleAssignment(
                vehicle_id=first_vehicle.id,
                driver_id=driver.id,
                shipment_id=first_shipment.id,
                assigned_at=assigned_at,
            ),
            VehicleAssignment(
                vehicle_id=second_vehicle.id,
                driver_id=driver.id,
                shipment_id=second_shipment.id,
                assigned_at=assigned_at,
            ),
        ]
    )

    with pytest.raises(IntegrityError):
        await session.commit()

    await session.rollback()


@pytest.mark.asyncio
async def test_only_one_active_assignment_per_shipment(session: AsyncSession) -> None:
    shipment = await create_shipment(session)
    first_vehicle = await create_vehicle(session)
    second_vehicle = await create_vehicle(session)
    first_driver = await create_driver(session)
    second_driver = await create_driver(session)
    assigned_at = datetime.now(UTC)

    session.add_all(
        [
            VehicleAssignment(
                vehicle_id=first_vehicle.id,
                driver_id=first_driver.id,
                shipment_id=shipment.id,
                assigned_at=assigned_at,
            ),
            VehicleAssignment(
                vehicle_id=second_vehicle.id,
                driver_id=second_driver.id,
                shipment_id=shipment.id,
                assigned_at=assigned_at,
            ),
        ]
    )

    with pytest.raises(IntegrityError):
        await session.commit()

    await session.rollback()


@pytest.mark.asyncio
async def test_reassignment_is_allowed_after_previous_assignment_ends(
    session: AsyncSession,
) -> None:
    vehicle = await create_vehicle(session)
    driver = await create_driver(session)
    shipment = await create_shipment(session)
    now = datetime.now(UTC)

    first = await create_assignment(
        session,
        vehicle,
        driver,
        shipment,
        assigned_at=now - timedelta(hours=2),
        ended_at=now - timedelta(hours=1),
    )
    second = await create_assignment(
        session,
        vehicle,
        driver,
        shipment,
        assigned_at=now,
    )

    assert first.ended_at is not None
    assert second.ended_at is None


@pytest.mark.asyncio
async def test_historical_assignments_are_preserved(session: AsyncSession) -> None:
    vehicle = await create_vehicle(session)
    driver = await create_driver(session)
    shipment = await create_shipment(session)
    now = datetime.now(UTC)

    first = await create_assignment(
        session,
        vehicle,
        driver,
        shipment,
        assigned_at=now - timedelta(hours=2),
        ended_at=now - timedelta(hours=1),
    )
    second = await create_assignment(
        session,
        vehicle,
        driver,
        shipment,
        assigned_at=now,
    )

    result = await session.execute(
        select(VehicleAssignment)
        .where(VehicleAssignment.vehicle_id == vehicle.id)
        .order_by(VehicleAssignment.assigned_at)
    )
    rows = result.scalars().all()

    assert [row.id for row in rows] == [first.id, second.id]
    assert rows[0].ended_at is not None
    assert rows[1].ended_at is None


@pytest.mark.parametrize("parent_kind", ["vehicle", "driver", "shipment"])
@pytest.mark.asyncio
async def test_deleting_parent_with_assignment_is_restricted(
    session: AsyncSession,
    parent_kind: str,
) -> None:
    vehicle = await create_vehicle(session)
    driver = await create_driver(session)
    shipment = await create_shipment(session)
    await create_assignment(session, vehicle, driver, shipment)

    parents: dict[str, Vehicle | Driver | Shipment] = {
        "vehicle": vehicle,
        "driver": driver,
        "shipment": shipment,
    }
    await session.delete(parents[parent_kind])

    with pytest.raises(IntegrityError):
        await session.commit()

    await session.rollback()
