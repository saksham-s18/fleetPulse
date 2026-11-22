"""Integration tests for assignment concurrency and transactional rollback."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID

import pytest
import pytest_asyncio
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import app.services.assignment as assignment_service_module
from app.core.config import get_settings
from app.core.exceptions import ConflictError
from app.infrastructure.database import create_engine, create_session_factory
from app.models import Driver, Shipment, Vehicle, VehicleAssignment
from app.models.enums import DriverStatus, ShipmentStatus, VehicleStatus
from app.repositories.assignment import AssignmentRepository
from app.schemas.assignment import AssignmentCreate
from app.services.assignment import AssignmentService
from app.services.transactions import commit_session as original_commit
from tests.integration.helpers import unique

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Yield a session factory bound to the configured PostgreSQL database."""
    engine = create_engine(get_settings())
    factory = create_session_factory(engine)
    try:
        yield factory
    finally:
        await engine.dispose()


async def create_resources(
    factory: async_sessionmaker[AsyncSession],
    *,
    shipment_count: int,
) -> tuple[UUID, UUID, list[UUID]]:
    """Create an active vehicle, active driver and the requested shipments."""
    async with factory() as session:
        vehicle = Vehicle(
            registration_number=unique("REG"),
            vehicle_type="van",
            status=VehicleStatus.ACTIVE,
        )
        driver = Driver(
            full_name="Concurrency Driver",
            license_number=unique("LIC"),
            status=DriverStatus.ACTIVE,
        )
        shipments = [
            Shipment(
                reference=unique("SHP"),
                origin={"name": "Berlin", "latitude": 52.52, "longitude": 13.405},
                destination={"name": "Hamburg", "latitude": 53.551, "longitude": 9.993},
                status=ShipmentStatus.CREATED,
            )
            for _ in range(shipment_count)
        ]
        session.add_all([vehicle, driver, *shipments])
        await session.commit()
        return vehicle.id, driver.id, [shipment.id for shipment in shipments]


async def test_concurrent_assignment_attempts_are_serialized_by_database(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vehicle_id, driver_id, shipment_ids = await create_resources(
        session_factory,
        shipment_count=2,
    )

    barrier = asyncio.Barrier(2)

    async def synchronized_commit(session: AsyncSession) -> None:
        # Let both requests pass their business-rule checks before committing.
        await barrier.wait()
        await original_commit(session)

    monkeypatch.setattr(
        assignment_service_module,
        "commit_session",
        synchronized_commit,
    )

    async def attempt(shipment_id: UUID) -> tuple[str, str]:
        async with session_factory() as session:
            service = AssignmentService(session)
            try:
                assignment = await service.create(
                    AssignmentCreate(
                        vehicle_id=vehicle_id,
                        driver_id=driver_id,
                        shipment_id=shipment_id,
                    )
                )
                return "created", str(assignment.id)
            except ConflictError as exc:
                return "conflict", exc.message

    results = await asyncio.gather(*(attempt(shipment_id) for shipment_id in shipment_ids))

    assert sorted(result[0] for result in results) == ["conflict", "created"]
    conflict_message = next(message for outcome, message in results if outcome == "conflict")
    assert "active assignment" in conflict_message

    async with session_factory() as session:
        active_count = await session.scalar(
            select(func.count())
            .select_from(VehicleAssignment)
            .where(
                VehicleAssignment.vehicle_id == vehicle_id,
                VehicleAssignment.ended_at.is_(None),
            )
        )
        assert active_count == 1

        result = await session.execute(select(Shipment.status).where(Shipment.id.in_(shipment_ids)))
        statuses = set(result.scalars().all())
        assert statuses == {ShipmentStatus.ASSIGNED, ShipmentStatus.CREATED}


async def test_assignment_failure_rolls_back_shipment_status(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vehicle_id, driver_id, shipment_ids = await create_resources(
        session_factory,
        shipment_count=2,
    )

    async with session_factory() as session:
        service = AssignmentService(session)
        await service.create(
            AssignmentCreate(
                vehicle_id=vehicle_id,
                driver_id=driver_id,
                shipment_id=shipment_ids[0],
            )
        )

    async with session_factory() as session:
        second_driver = Driver(
            full_name="Second Driver",
            license_number=unique("LIC"),
            status=DriverStatus.ACTIVE,
        )
        session.add(second_driver)
        await session.commit()
        second_driver_id = second_driver.id

    async def no_active_vehicle(_self: AssignmentRepository, _vehicle_id: UUID) -> bool:
        return False

    monkeypatch.setattr(
        AssignmentRepository,
        "has_active_for_vehicle",
        no_active_vehicle,
    )

    async with session_factory() as session:
        service = AssignmentService(session)
        with pytest.raises(ConflictError) as exc_info:
            await service.create(
                AssignmentCreate(
                    vehicle_id=vehicle_id,
                    driver_id=second_driver_id,
                    shipment_id=shipment_ids[1],
                )
            )

    assert "active assignment" in exc_info.value.message

    async with session_factory() as session:
        shipment = await session.get(Shipment, shipment_ids[1])
        assert shipment is not None
        assert shipment.status == ShipmentStatus.CREATED

        assignment_count = await session.scalar(
            select(func.count())
            .select_from(VehicleAssignment)
            .where(VehicleAssignment.shipment_id == shipment_ids[1])
        )
        assert assignment_count == 0


async def test_assignment_rejects_shipment_with_existing_active_assignment(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    vehicle_id, driver_id, shipment_ids = await create_resources(
        session_factory,
        shipment_count=1,
    )
    shipment_id = shipment_ids[0]

    # Simulate an active assignment created outside the service, leaving the
    # shipment in "created" status so eligibility alone does not reject it.
    async with session_factory() as session:
        session.add(
            VehicleAssignment(
                vehicle_id=vehicle_id,
                driver_id=driver_id,
                shipment_id=shipment_id,
                assigned_at=datetime.now(UTC),
            )
        )
        await session.commit()

    async with session_factory() as session:
        other_vehicle = Vehicle(
            registration_number=unique("REG"),
            vehicle_type="truck",
            status=VehicleStatus.ACTIVE,
        )
        other_driver = Driver(
            full_name="Other Driver",
            license_number=unique("LIC"),
            status=DriverStatus.ACTIVE,
        )
        session.add_all([other_vehicle, other_driver])
        await session.commit()
        other_vehicle_id = other_vehicle.id
        other_driver_id = other_driver.id

    async with session_factory() as session:
        service = AssignmentService(session)
        with pytest.raises(ConflictError) as exc_info:
            await service.create(
                AssignmentCreate(
                    vehicle_id=other_vehicle_id,
                    driver_id=other_driver_id,
                    shipment_id=shipment_id,
                )
            )

    assert "shipment already has an active assignment" in exc_info.value.message
