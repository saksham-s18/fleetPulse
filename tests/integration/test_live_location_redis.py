"""Real Redis integration tests for the live-location projection.

These tests require the Compose Redis service::

    docker compose up -d redis
    pytest -m redis

Every test uses an isolated key namespace and deletes its keys afterwards, so
it never touches another environment or another test run.
"""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from redis.asyncio import Redis

from app.core.config import get_settings
from app.infrastructure.redis import check_redis, create_redis_client, ensure_redis_ready
from app.repositories.live_location import LiveLocationRepository
from app.schemas.gps import GpsEvent
from app.schemas.live_location import DistanceUnit, LocationUpdateOutcome
from app.services.live_location import LiveLocationService

pytestmark = pytest.mark.redis

RECORDED_AT = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def make_event(**overrides: object) -> GpsEvent:
    payload: dict[str, object] = {
        "event_id": uuid4(),
        "vehicle_id": uuid4(),
        "latitude": 52.52,
        "longitude": 13.405,
        "speed_kmph": 31.5,
        "heading": 87.5,
        "recorded_at": RECORDED_AT,
    }
    payload.update(overrides)
    return GpsEvent.model_validate(payload)


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[Redis]:
    client = create_redis_client(get_settings())
    await ensure_redis_ready(client)
    try:
        yield client
    finally:
        await client.aclose()


@pytest_asyncio.fixture
async def namespace(redis_client: Redis) -> AsyncIterator[str]:
    prefix = f"fleetpulse-test-{uuid4().hex[:12]}"
    try:
        yield prefix
    finally:
        keys = [key async for key in redis_client.scan_iter(match=f"{prefix}:*")]
        if keys:
            await redis_client.delete(*keys)


def build_service(redis_client: Redis, namespace: str) -> LiveLocationService:
    return LiveLocationService(
        LiveLocationRepository(redis_client, prefix=namespace),
        stale_after_seconds=60,
    )


@pytest.mark.asyncio
async def test_connection_and_ping(redis_client: Redis) -> None:
    assert await redis_client.ping() is True
    assert await check_redis(redis_client) is True


@pytest.mark.asyncio
async def test_location_write_and_read(redis_client: Redis, namespace: str) -> None:
    service = build_service(redis_client, namespace)
    shipment_id = uuid4()
    event = make_event(shipment_id=shipment_id)

    result = await service.update_from_event(
        event,
        received_at=RECORDED_AT + timedelta(seconds=1),
    )
    location = await service.get_location(
        event.vehicle_id,
        now=RECORDED_AT + timedelta(seconds=10),
    )

    assert result.outcome is LocationUpdateOutcome.APPLIED
    assert location is not None
    assert location.event_id == event.event_id
    assert location.shipment_id == shipment_id
    assert location.stale is False
    assert location.age_seconds == pytest.approx(10.0)


@pytest.mark.asyncio
async def test_geo_lookup(redis_client: Redis, namespace: str) -> None:
    service = build_service(redis_client, namespace)
    vehicle_a = make_event(latitude=52.5200, longitude=13.4050)
    vehicle_b = make_event(latitude=52.5300, longitude=13.4150)
    vehicle_far = make_event(latitude=48.1372, longitude=11.5756)
    for event in (vehicle_a, vehicle_b, vehicle_far):
        await service.update_from_event(event)

    nearby = await service.find_nearby(52.5200, 13.4050, radius_km=5, limit=10)
    assert {vehicle.vehicle_id for vehicle in nearby} == {
        vehicle_a.vehicle_id,
        vehicle_b.vehicle_id,
    }
    assert nearby[0].vehicle_id == vehicle_a.vehicle_id

    coordinates = await service.get_coordinates(vehicle_far.vehicle_id)
    assert coordinates is not None
    assert coordinates == pytest.approx((48.1372, 11.5756))


@pytest.mark.asyncio
async def test_duplicate_and_out_of_order_events(
    redis_client: Redis,
    namespace: str,
) -> None:
    service = build_service(redis_client, namespace)
    vehicle_id = uuid4()
    newer = make_event(
        vehicle_id=vehicle_id,
        recorded_at=RECORDED_AT + timedelta(seconds=10),
        latitude=52.6,
    )
    older = make_event(
        vehicle_id=vehicle_id,
        recorded_at=RECORDED_AT,
        latitude=52.4,
    )

    assert (await service.update_from_event(newer)).outcome is LocationUpdateOutcome.APPLIED
    assert (await service.update_from_event(older)).outcome is LocationUpdateOutcome.STALE
    assert (await service.update_from_event(newer)).outcome is LocationUpdateOutcome.DUPLICATE

    location = await service.get_location(vehicle_id)
    coordinates = await service.get_coordinates(vehicle_id)
    assert location is not None
    assert location.event_id == newer.event_id
    assert location.latitude == pytest.approx(52.6)
    assert coordinates is not None
    assert coordinates == pytest.approx((52.6, newer.longitude))


@pytest.mark.asyncio
async def test_equal_timestamp_tie_break(redis_client: Redis, namespace: str) -> None:
    service = build_service(redis_client, namespace)
    vehicle_id = uuid4()
    lower = make_event(vehicle_id=vehicle_id, event_id=UUID(int=1), latitude=52.4)
    higher = make_event(vehicle_id=vehicle_id, event_id=UUID(int=2), latitude=52.6)

    assert (await service.update_from_event(lower)).outcome is LocationUpdateOutcome.APPLIED
    assert (await service.update_from_event(higher)).outcome is LocationUpdateOutcome.APPLIED
    assert (await service.update_from_event(lower)).outcome is LocationUpdateOutcome.STALE

    location = await service.get_location(vehicle_id)
    assert location is not None
    assert location.event_id == UUID(int=2)
    assert location.latitude == pytest.approx(52.6)


@pytest.mark.asyncio
async def test_concurrent_updates_leave_consistent_hash_and_geo(
    redis_client: Redis,
    namespace: str,
) -> None:
    service = build_service(redis_client, namespace)
    vehicle_id = uuid4()
    events = [
        make_event(
            vehicle_id=vehicle_id,
            recorded_at=RECORDED_AT + timedelta(seconds=step),
            latitude=52.5 + step / 100,
            longitude=13.4 + step / 100,
        )
        for step in range(20)
    ]
    # Deliver in reverse order; the Lua compare-and-set must converge on the
    # newest recorded_at regardless of arrival order. Which intermediate
    # events win depends on scheduling, so only convergence is asserted.
    results = await asyncio.gather(
        *(service.update_from_event(event) for event in reversed(events))
    )
    assert any(result.outcome is LocationUpdateOutcome.APPLIED for result in results)

    location = await service.get_location(vehicle_id)
    coordinates = await service.get_coordinates(vehicle_id)
    assert location is not None
    assert location.event_id == events[-1].event_id
    assert location.recorded_at == events[-1].recorded_at
    assert coordinates is not None
    assert coordinates == pytest.approx((location.latitude, location.longitude))


@pytest.mark.asyncio
async def test_repeated_updates_do_not_desynchronise_hash_and_geo(
    redis_client: Redis,
    namespace: str,
) -> None:
    service = build_service(redis_client, namespace)
    vehicle_id = uuid4()
    for step in range(10):
        event = make_event(
            vehicle_id=vehicle_id,
            recorded_at=RECORDED_AT + timedelta(seconds=step),
            latitude=52.5 + step / 100,
            longitude=13.4 + step / 100,
        )
        assert (await service.update_from_event(event)).outcome is LocationUpdateOutcome.APPLIED
        # Redeliver the same event; neither structure may change.
        assert (await service.update_from_event(event)).outcome is LocationUpdateOutcome.DUPLICATE

    location = await service.get_location(vehicle_id)
    coordinates = await service.get_coordinates(vehicle_id)
    assert location is not None
    assert coordinates is not None
    assert coordinates == pytest.approx((location.latitude, location.longitude))


@pytest.mark.asyncio
async def test_paginated_locations_and_stale_filter(
    redis_client: Redis,
    namespace: str,
) -> None:
    service = build_service(redis_client, namespace)
    fresh_events = [
        make_event(latitude=52.50 + step / 100, longitude=13.40 + step / 100) for step in range(3)
    ]
    stale_event = make_event(
        latitude=52.60,
        longitude=13.50,
        recorded_at=RECORDED_AT - timedelta(seconds=120),
    )
    for event in (*fresh_events, stale_event):
        await service.update_from_event(event)

    current = RECORDED_AT + timedelta(seconds=30)
    first = await service.list_vehicle_locations(page=1, page_size=2, now=current)
    second = await service.list_vehicle_locations(page=2, page_size=2, now=current)
    assert first.total == 4
    assert first.total_pages == 2
    assert len(first.items) == 2
    assert len(second.items) == 2
    assert {item.vehicle_id for item in (*first.items, *second.items)} == {
        event.vehicle_id for event in (*fresh_events, stale_event)
    }

    stale_page = await service.list_vehicle_locations(is_stale=True, now=current)
    assert stale_page.total == 1
    assert [item.vehicle_id for item in stale_page.items] == [stale_event.vehicle_id]

    fresh_page = await service.list_vehicle_locations(is_stale=False, now=current)
    assert fresh_page.total == 3
    assert {item.vehicle_id for item in fresh_page.items} == {
        event.vehicle_id for event in fresh_events
    }


@pytest.mark.asyncio
async def test_nearby_records_and_empty_results(
    redis_client: Redis,
    namespace: str,
) -> None:
    service = build_service(redis_client, namespace)
    repository = LiveLocationRepository(redis_client, prefix=namespace)
    near = make_event(latitude=52.5200, longitude=13.4050)
    farther = make_event(latitude=52.5300, longitude=13.4150)
    for event in (near, farther):
        await service.update_from_event(event)

    entries = await repository.nearby_records(52.5200, 13.4050, radius_km=5, limit=10)
    assert [entry.location.vehicle_id for entry in entries] == [
        near.vehicle_id,
        farther.vehicle_id,
    ]
    assert entries[0].distance_km <= entries[1].distance_km

    result = await service.find_nearby_vehicles(
        52.5200,
        13.4050,
        5000,
        unit=DistanceUnit.METERS,
    )
    assert len(result.items) == 2
    assert result.items[0].distance == pytest.approx(0.0, abs=1.0)
    assert result.items[1].distance > result.items[0].distance

    empty = await service.find_nearby_vehicles(48.1372, 11.5756, 1)
    assert empty.items == []

    empty_page = await service.list_vehicle_locations(
        vehicle_id=uuid4(),
        now=RECORDED_AT,
    )
    assert empty_page.total == 0
    assert empty_page.items == []


@pytest.mark.asyncio
async def test_location_hash_retrieval_missing_vehicle(
    redis_client: Redis,
    namespace: str,
) -> None:
    repository = LiveLocationRepository(redis_client, prefix=namespace)

    assert await repository.latest(uuid4()) is None
    assert await repository.count_indexed() == 0
    assert await repository.page_indexed(offset=0, limit=10) == []
