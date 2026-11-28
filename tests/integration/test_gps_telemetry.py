"""Integration tests for durable GPS telemetry against real PostgreSQL.

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

from app.core.config import get_settings
from app.infrastructure.database import create_engine, create_session_factory
from app.models import GpsTelemetry, Shipment, Vehicle
from app.models.enums import ShipmentStatus, VehicleStatus

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


async def create_vehicle(session: AsyncSession) -> Vehicle:
    vehicle = Vehicle(
        registration_number=unique("REG"),
        vehicle_type="van",
        status=VehicleStatus.ACTIVE,
        device_id=unique("DEV"),
    )
    session.add(vehicle)
    await session.commit()
    await session.refresh(vehicle)
    return vehicle


async def create_shipment(session: AsyncSession) -> Shipment:
    shipment = Shipment(
        reference=unique("SHP"),
        origin={"city": "Berlin", "latitude": 52.52, "longitude": 13.405},
        destination={"city": "Hamburg", "latitude": 53.551, "longitude": 9.993},
        status=ShipmentStatus.CREATED,
        expected_delivery_at=datetime.now(UTC) + timedelta(hours=4),
    )
    session.add(shipment)
    await session.commit()
    await session.refresh(shipment)
    return shipment


async def create_telemetry(
    session: AsyncSession,
    vehicle: Vehicle,
    shipment: Shipment | None = None,
    *,
    event_id: UUID | None = None,
    latitude: float = 52.520008,
    longitude: float = 13.404954,
    speed_kmh: float = 48.125,
    heading: float | None = 359.9,
    recorded_at: datetime | None = None,
) -> GpsTelemetry:
    telemetry = GpsTelemetry(
        event_id=event_id or uuid4(),
        vehicle_id=vehicle.id,
        shipment_id=shipment.id if shipment is not None else None,
        latitude=latitude,
        longitude=longitude,
        speed_kmh=speed_kmh,
        heading=heading,
        recorded_at=recorded_at or datetime.now(UTC) - timedelta(minutes=1),
    )
    session.add(telemetry)
    await session.commit()
    await session.refresh(telemetry)
    return telemetry


@pytest.mark.asyncio
async def test_create_telemetry_persists_contract_fields(session: AsyncSession) -> None:
    vehicle = await create_vehicle(session)
    recorded_at = datetime.now(UTC) - timedelta(minutes=1)

    telemetry = await create_telemetry(session, vehicle, recorded_at=recorded_at)

    assert isinstance(telemetry.event_id, UUID)
    assert telemetry.vehicle_id == vehicle.id
    assert telemetry.shipment_id is None
    assert telemetry.schema_version == "1.0"
    assert telemetry.recorded_at == recorded_at
    assert telemetry.recorded_at.tzinfo is not None
    assert telemetry.received_at.tzinfo is not None
    assert telemetry.received_at >= telemetry.recorded_at


@pytest.mark.asyncio
async def test_duplicate_event_id_is_rejected(session: AsyncSession) -> None:
    vehicle = await create_vehicle(session)
    existing = await create_telemetry(session, vehicle)
    # Remove the committed row from the identity map so the duplicate is sent
    # to PostgreSQL instead of being treated as an in-memory conflict.
    session.expunge(existing)

    duplicate = GpsTelemetry(
        event_id=existing.event_id,
        vehicle_id=vehicle.id,
        latitude=52.52,
        longitude=13.405,
        speed_kmh=10.0,
        recorded_at=datetime.now(UTC),
    )
    session.add(duplicate)

    with pytest.raises(IntegrityError):
        await session.commit()

    await session.rollback()


@pytest.mark.asyncio
async def test_vehicle_foreign_key_is_enforced(session: AsyncSession) -> None:
    telemetry = GpsTelemetry(
        event_id=uuid4(),
        vehicle_id=uuid4(),
        latitude=52.52,
        longitude=13.405,
        speed_kmh=10.0,
        recorded_at=datetime.now(UTC),
    )
    session.add(telemetry)

    with pytest.raises(IntegrityError):
        await session.commit()

    await session.rollback()


@pytest.mark.asyncio
async def test_shipment_reference_is_optional(session: AsyncSession) -> None:
    vehicle = await create_vehicle(session)

    telemetry = await create_telemetry(session, vehicle, shipment=None)

    assert telemetry.shipment_id is None


@pytest.mark.asyncio
async def test_valid_shipment_reference_is_persisted(session: AsyncSession) -> None:
    vehicle = await create_vehicle(session)
    shipment = await create_shipment(session)

    telemetry = await create_telemetry(session, vehicle, shipment)

    assert telemetry.shipment_id == shipment.id

    result = await session.execute(
        select(GpsTelemetry).where(GpsTelemetry.event_id == telemetry.event_id)
    )
    assert result.scalar_one().shipment_id == shipment.id


@pytest.mark.asyncio
async def test_invalid_shipment_reference_is_rejected(session: AsyncSession) -> None:
    vehicle = await create_vehicle(session)

    telemetry = GpsTelemetry(
        event_id=uuid4(),
        vehicle_id=vehicle.id,
        shipment_id=uuid4(),
        latitude=52.52,
        longitude=13.405,
        speed_kmh=10.0,
        recorded_at=datetime.now(UTC),
    )
    session.add(telemetry)

    with pytest.raises(IntegrityError):
        await session.commit()

    await session.rollback()


@pytest.mark.asyncio
async def test_numeric_and_timestamp_values_round_trip(session: AsyncSession) -> None:
    vehicle = await create_vehicle(session)
    recorded_at = datetime(2026, 11, 27, 10, 30, 15, 123456, tzinfo=UTC)

    telemetry = await create_telemetry(
        session,
        vehicle,
        latitude=-33.86882,
        longitude=151.20929,
        speed_kmh=73.456,
        heading=182.25,
        recorded_at=recorded_at,
    )

    assert telemetry.latitude == pytest.approx(-33.86882, abs=1e-12)
    assert telemetry.longitude == pytest.approx(151.20929, abs=1e-12)
    assert telemetry.speed_kmh == pytest.approx(73.456, abs=1e-12)
    assert telemetry.heading == pytest.approx(182.25, abs=1e-12)
    assert telemetry.recorded_at == recorded_at
    assert telemetry.recorded_at.tzinfo is not None


@pytest.mark.asyncio
async def test_null_heading_is_allowed(session: AsyncSession) -> None:
    vehicle = await create_vehicle(session)

    telemetry = await create_telemetry(session, vehicle, heading=None)

    assert telemetry.heading is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("latitude", 90.1),
        ("latitude", -90.1),
        ("longitude", 180.1),
        ("longitude", -180.1),
        ("speed_kmh", -0.1),
        ("heading", 360.1),
        ("heading", -0.1),
    ],
)
@pytest.mark.asyncio
async def test_range_constraints_are_enforced(
    session: AsyncSession,
    field: str,
    value: float,
) -> None:
    vehicle = await create_vehicle(session)
    values: dict[str, object] = {
        "event_id": uuid4(),
        "vehicle_id": vehicle.id,
        "latitude": 52.52,
        "longitude": 13.405,
        "speed_kmh": 10.0,
        "heading": 90.0,
        "recorded_at": datetime.now(UTC),
    }
    values[field] = value

    telemetry = GpsTelemetry(**values)
    session.add(telemetry)

    with pytest.raises(IntegrityError):
        await session.commit()

    await session.rollback()


@pytest.mark.asyncio
async def test_telemetry_constraints_and_indexes_exist(session: AsyncSession) -> None:
    constraints = await session.execute(
        text(
            """
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'gps_telemetry'::regclass
            """
        )
    )
    constraint_names: set[str] = set(map(str, constraints.scalars()))

    assert {
        "pk_gps_telemetry",
        "fk_gps_telemetry_vehicle_id_vehicles",
        "fk_gps_telemetry_shipment_id_shipments",
        "ck_gps_telemetry_latitude_range",
        "ck_gps_telemetry_longitude_range",
        "ck_gps_telemetry_speed_kmh_non_negative",
        "ck_gps_telemetry_heading_range",
    } <= constraint_names

    indexes = await session.execute(
        text(
            """
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'public' AND tablename = 'gps_telemetry'
            """
        )
    )
    index_definitions = {row.indexname: row.indexdef for row in indexes}

    assert "recorded_at DESC" in index_definitions["ix_gps_telemetry_vehicle_recorded_at"]
    shipment_index = index_definitions["ix_gps_telemetry_shipment_recorded_at"]
    assert "recorded_at DESC" in shipment_index
    assert "WHERE (shipment_id IS NOT NULL)" in shipment_index


@pytest.mark.parametrize("parent_kind", ["vehicle", "shipment"])
@pytest.mark.asyncio
async def test_deleting_parent_with_telemetry_is_restricted(
    session: AsyncSession,
    parent_kind: str,
) -> None:
    vehicle = await create_vehicle(session)
    shipment = await create_shipment(session)
    await create_telemetry(session, vehicle, shipment)

    parents: dict[str, Vehicle | Shipment] = {
        "vehicle": vehicle,
        "shipment": shipment,
    }
    await session.delete(parents[parent_kind])

    with pytest.raises(IntegrityError):
        await session.commit()

    await session.rollback()
