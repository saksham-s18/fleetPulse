"""Unit tests for GPS ingestion publication ordering (no PostgreSQL/Kafka).

The repositories are replaced with lightweight fakes so the tests focus on
one thing: validation runs first, publication runs only after validation,
and ``202`` semantics (ack) or ``503`` mapping depend on the publisher.
"""

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import ConflictError, NotFoundError, ServiceUnavailableError
from app.models.enums import VehicleStatus
from app.schemas.gps import GpsEvent
from app.services.gps import GpsIngestService
from app.services.gps_publisher import GpsPublishError
from tests.gps_publisher_fakes import RecordingGpsEventPublisher


def make_event(**overrides: object) -> GpsEvent:
    payload: dict[str, object] = {
        "event_id": uuid4(),
        "vehicle_id": uuid4(),
        "latitude": 52.52,
        "longitude": 13.405,
        "speed_kmph": 31.5,
        "recorded_at": datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
    }
    payload.update(overrides)
    return GpsEvent.model_validate(payload)


class FakeVehicleRepository:
    def __init__(self, trace: list[str], vehicle: SimpleNamespace | None) -> None:
        self._trace = trace
        self._vehicle = vehicle

    async def get(self, vehicle_id: object) -> SimpleNamespace | None:
        del vehicle_id
        self._trace.append("vehicle_lookup")
        return self._vehicle


class FakeShipmentRepository:
    def __init__(self, trace: list[str], shipment: SimpleNamespace | None) -> None:
        self._trace = trace
        self._shipment = shipment

    async def get(self, shipment_id: object) -> SimpleNamespace | None:
        del shipment_id
        self._trace.append("shipment_lookup")
        return self._shipment


class FakeAssignmentRepository:
    def __init__(self, trace: list[str], assignment: SimpleNamespace | None) -> None:
        self._trace = trace
        self._assignment = assignment

    async def get_active_for_vehicle(self, vehicle_id: object) -> SimpleNamespace | None:
        del vehicle_id
        self._trace.append("assignment_lookup")
        return self._assignment


class TracingPublisher(RecordingGpsEventPublisher):
    """Recording publisher that also appends to the shared trace."""

    def __init__(self, trace: list[str], *, fail_with: GpsPublishError | None = None) -> None:
        super().__init__(fail_with=fail_with)
        self._trace = trace

    async def publish(self, event: GpsEvent) -> None:
        self._trace.append("publish")
        await super().publish(event)


def build_service(
    trace: list[str],
    *,
    vehicle: SimpleNamespace | None,
    shipment: SimpleNamespace | None = None,
    assignment: SimpleNamespace | None = None,
    publisher: TracingPublisher | None = None,
) -> tuple[GpsIngestService, TracingPublisher]:
    service = GpsIngestService(
        session=cast(AsyncSession, SimpleNamespace()),
        publisher=publisher or TracingPublisher(trace),
    )
    service._vehicles = FakeVehicleRepository(trace, vehicle)  # type: ignore[assignment]
    service._shipments = FakeShipmentRepository(trace, shipment)  # type: ignore[assignment]
    service._assignments = FakeAssignmentRepository(trace, assignment)  # type: ignore[assignment]
    resolved_publisher = publisher or service._publisher
    assert isinstance(resolved_publisher, TracingPublisher)
    return service, resolved_publisher


def active_vehicle() -> SimpleNamespace:
    return SimpleNamespace(id=uuid4(), status=VehicleStatus.ACTIVE)


@pytest.mark.asyncio
async def test_valid_event_is_published_before_ack() -> None:
    trace: list[str] = []
    service, publisher = build_service(trace, vehicle=active_vehicle())
    event = make_event()

    ack = await service.ingest(event)

    # Publication happens after validation and completes before the ack.
    assert trace == ["vehicle_lookup", "publish"]
    assert publisher.events == [event]
    assert ack.event_id == event.event_id
    assert ack.vehicle_id == event.vehicle_id
    assert ack.shipment_id is None
    assert ack.status == "accepted"
    assert ack.persisted is False


@pytest.mark.asyncio
async def test_unknown_vehicle_is_never_published() -> None:
    trace: list[str] = []
    service, publisher = build_service(trace, vehicle=None)

    with pytest.raises(NotFoundError, match="Vehicle not found"):
        await service.ingest(make_event())

    assert trace == ["vehicle_lookup"]
    assert publisher.events == []


@pytest.mark.asyncio
async def test_inactive_vehicle_is_never_published() -> None:
    trace: list[str] = []
    vehicle = SimpleNamespace(id=uuid4(), status=VehicleStatus.INACTIVE)
    service, publisher = build_service(trace, vehicle=vehicle)

    with pytest.raises(ConflictError, match="not active"):
        await service.ingest(make_event())

    assert publisher.events == []


@pytest.mark.asyncio
async def test_shipment_off_active_assignment_is_never_published() -> None:
    trace: list[str] = []
    shipment = SimpleNamespace(id=uuid4())
    service, publisher = build_service(
        trace,
        vehicle=active_vehicle(),
        shipment=shipment,
        assignment=None,
    )

    with pytest.raises(ConflictError, match="active assignment"):
        await service.ingest(make_event(shipment_id=shipment.id))

    assert trace == ["vehicle_lookup", "shipment_lookup", "assignment_lookup"]
    assert publisher.events == []


@pytest.mark.asyncio
async def test_publish_failure_maps_to_service_unavailable() -> None:
    trace: list[str] = []
    publisher = TracingPublisher(trace, fail_with=GpsPublishError("broker down"))
    service, _ = build_service(trace, vehicle=active_vehicle(), publisher=publisher)

    with pytest.raises(ServiceUnavailableError) as exc_info:
        await service.ingest(make_event())

    assert exc_info.value.status_code == 503
    assert exc_info.value.code == "service_unavailable"
    assert publisher.events == []


@pytest.mark.asyncio
async def test_publish_failure_message_hides_broker_details() -> None:
    trace: list[str] = []
    publisher = TracingPublisher(
        trace, fail_with=GpsPublishError("connect to localhost:9092 refused")
    )
    service, _ = build_service(trace, vehicle=active_vehicle(), publisher=publisher)

    with pytest.raises(ServiceUnavailableError) as exc_info:
        await service.ingest(make_event())

    # The safe API message is fixed; broker addresses stay in server-side logs.
    assert "localhost" not in exc_info.value.message
    assert "9092" not in exc_info.value.message
