"""Unit tests for the Redis live-location projection (no Redis server).

``fakeredis`` provides the async client, including Lua evaluation and GEO
commands, so the atomic update contract, key design, serialisation, staleness
and error translation are exercised deterministically.
"""

from collections.abc import AsyncIterator, Awaitable
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID, uuid4

import fakeredis.aioredis
import pytest
import pytest_asyncio
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from app.repositories.live_location import (
    LiveLocationKeys,
    LiveLocationRepository,
    LiveLocationStoreError,
    deserialize_location,
    serialize_location,
)
from app.schemas.gps import GpsEvent
from app.schemas.live_location import (
    LiveLocationWrite,
    LocationUpdateOutcome,
)
from app.services.live_location import (
    LiveLocationService,
    LiveLocationUnavailableError,
)

PREFIX = "fleetpulse-test"
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


def make_write(event: GpsEvent, *, received_at: datetime | None = None) -> LiveLocationWrite:
    return LiveLocationWrite(
        vehicle_id=event.vehicle_id,
        event_id=event.event_id,
        latitude=event.latitude,
        longitude=event.longitude,
        speed_kmh=event.speed_kmph,
        heading=event.heading,
        recorded_at=event.recorded_at,
        received_at=received_at or RECORDED_AT + timedelta(seconds=1),
        shipment_id=event.shipment_id,
    )


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[fakeredis.aioredis.FakeRedis]:
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
def repository(redis_client: fakeredis.aioredis.FakeRedis) -> LiveLocationRepository:
    return LiveLocationRepository(cast(Redis, redis_client), prefix=PREFIX)


@pytest.fixture
def service(repository: LiveLocationRepository) -> LiveLocationService:
    return LiveLocationService(repository, stale_after_seconds=60)


def test_key_generation_and_namespacing() -> None:
    keys = LiveLocationKeys(PREFIX)
    vehicle_id = UUID("0b6f2c9a-1d2e-4f3a-8b7c-9d0e1f2a3b4c")

    assert keys.geo == f"{PREFIX}:live:geo"
    assert keys.location(vehicle_id) == f"{PREFIX}:live:location:{vehicle_id}"
    assert LiveLocationKeys().geo == "fleetpulse:live:geo"


@pytest.mark.parametrize("prefix", ["", " padded", "has space", " padded "])
def test_invalid_prefix_is_rejected(prefix: str) -> None:
    with pytest.raises(ValueError):
        LiveLocationKeys(prefix)


def test_serialization_round_trip() -> None:
    event = make_event(heading=None)
    write = make_write(event)
    fields = serialize_location(write)

    assert fields["vehicle_id"] == str(event.vehicle_id)
    assert fields["event_id"] == str(event.event_id)
    assert fields["speed_kmh"] == "31.5"
    assert fields["heading"] == ""
    assert fields["shipment_id"] == ""
    assert fields["recorded_at_ms"] == str(int(RECORDED_AT.timestamp() * 1000))

    record = deserialize_location(fields)
    assert record is not None
    assert record.vehicle_id == event.vehicle_id
    assert record.event_id == event.event_id
    assert record.latitude == pytest.approx(event.latitude)
    assert record.longitude == pytest.approx(event.longitude)
    assert record.speed_kmh == pytest.approx(event.speed_kmph)
    assert record.heading is None
    assert record.shipment_id is None
    assert record.recorded_at == RECORDED_AT
    assert deserialize_location({}) is None


def test_serialization_rejects_naive_timestamps() -> None:
    write = LiveLocationWrite(
        vehicle_id=uuid4(),
        event_id=uuid4(),
        latitude=52.52,
        longitude=13.405,
        speed_kmh=31.5,
        heading=None,
        recorded_at=datetime(2026, 1, 1, 12, 0),
        received_at=RECORDED_AT,
    )
    with pytest.raises(ValueError, match="timezone"):
        serialize_location(write)


@pytest.mark.asyncio
async def test_latest_location_update(
    repository: LiveLocationRepository,
    service: LiveLocationService,
) -> None:
    shipment_id = uuid4()
    event = make_event(shipment_id=shipment_id)
    received_at = RECORDED_AT + timedelta(seconds=2)

    result = await service.update_from_event(event, received_at=received_at)

    assert result.outcome is LocationUpdateOutcome.APPLIED
    assert result.vehicle_id == event.vehicle_id
    record = await repository.latest(event.vehicle_id)
    assert record is not None
    assert record.event_id == event.event_id
    assert record.shipment_id == shipment_id
    assert record.speed_kmh == pytest.approx(31.5)
    assert record.recorded_at == RECORDED_AT
    assert record.received_at == received_at


@pytest.mark.asyncio
async def test_duplicate_event_is_idempotent(
    repository: LiveLocationRepository,
    redis_client: fakeredis.aioredis.FakeRedis,
) -> None:
    event = make_event()
    write = make_write(event)

    first = await repository.apply(write)
    fields_after_first = await cast(
        Awaitable[dict[str, str]],
        redis_client.hgetall(repository.keys.location(event.vehicle_id)),
    )
    second = await repository.apply(write)
    fields_after_second = await cast(
        Awaitable[dict[str, str]],
        redis_client.hgetall(repository.keys.location(event.vehicle_id)),
    )

    assert first is LocationUpdateOutcome.APPLIED
    assert second is LocationUpdateOutcome.DUPLICATE
    assert fields_after_second == fields_after_first
    assert await redis_client.zcard(repository.keys.geo) == 1


@pytest.mark.asyncio
async def test_older_event_is_rejected(
    repository: LiveLocationRepository,
    redis_client: fakeredis.aioredis.FakeRedis,
) -> None:
    newer = make_event(recorded_at=RECORDED_AT + timedelta(seconds=10), latitude=52.6)
    older = make_event(
        vehicle_id=newer.vehicle_id,
        recorded_at=RECORDED_AT,
        latitude=52.4,
        longitude=13.2,
    )

    assert await repository.apply(make_write(newer)) is LocationUpdateOutcome.APPLIED
    assert await repository.apply(make_write(older)) is LocationUpdateOutcome.STALE

    record = await repository.latest(newer.vehicle_id)
    assert record is not None
    assert record.event_id == newer.event_id
    assert record.latitude == pytest.approx(52.6)
    coordinates = await repository.coordinates(newer.vehicle_id)
    assert coordinates is not None
    assert coordinates[0] == pytest.approx(52.6)


@pytest.mark.asyncio
async def test_equal_timestamp_uses_event_id_tie_breaker(
    repository: LiveLocationRepository,
) -> None:
    vehicle_id = uuid4()
    lower_id = UUID(int=1)
    higher_id = UUID(int=2)
    lower = make_event(
        vehicle_id=vehicle_id,
        event_id=lower_id,
        recorded_at=RECORDED_AT,
        latitude=52.4,
    )
    higher = make_event(
        vehicle_id=vehicle_id,
        event_id=higher_id,
        recorded_at=RECORDED_AT,
        latitude=52.6,
    )

    assert await repository.apply(make_write(lower)) is LocationUpdateOutcome.APPLIED
    assert await repository.apply(make_write(higher)) is LocationUpdateOutcome.APPLIED
    # The losing event must not overwrite the winning one.
    assert await repository.apply(make_write(lower)) is LocationUpdateOutcome.STALE
    assert await repository.apply(make_write(higher)) is LocationUpdateOutcome.DUPLICATE

    record = await repository.latest(vehicle_id)
    assert record is not None
    assert record.event_id == higher_id
    assert record.latitude == pytest.approx(52.6)


@pytest.mark.asyncio
async def test_geo_index_updates_and_radius_query(
    repository: LiveLocationRepository,
    redis_client: fakeredis.aioredis.FakeRedis,
) -> None:
    vehicle_a = make_event(latitude=52.5200, longitude=13.4050)
    vehicle_b = make_event(latitude=52.5300, longitude=13.4150)
    await repository.apply(make_write(vehicle_a))
    await repository.apply(make_write(vehicle_b))

    nearby = await repository.nearby(52.5200, 13.4050, radius_km=5, limit=10)
    assert {vehicle.vehicle_id for vehicle in nearby} == {
        vehicle_a.vehicle_id,
        vehicle_b.vehicle_id,
    }
    assert nearby[0].vehicle_id == vehicle_a.vehicle_id
    assert nearby[0].distance_km <= nearby[1].distance_km

    moved = make_event(
        vehicle_id=vehicle_a.vehicle_id,
        recorded_at=RECORDED_AT + timedelta(seconds=5),
        latitude=52.6000,
        longitude=13.5000,
    )
    assert await repository.apply(make_write(moved)) is LocationUpdateOutcome.APPLIED

    coordinates = await repository.coordinates(vehicle_a.vehicle_id)
    assert coordinates is not None
    assert coordinates[0] == pytest.approx(52.6)
    assert coordinates[1] == pytest.approx(13.5)

    near_old_position = await repository.nearby(52.5200, 13.4050, radius_km=2, limit=10)
    assert {vehicle.vehicle_id for vehicle in near_old_position} == {vehicle_b.vehicle_id}

    await repository.delete(vehicle_a.vehicle_id)
    assert await repository.coordinates(vehicle_a.vehicle_id) is None
    assert await repository.latest(vehicle_a.vehicle_id) is None
    assert await redis_client.zcard(repository.keys.geo) == 1


@pytest.mark.asyncio
async def test_repeated_updates_keep_hash_and_geo_consistent(
    repository: LiveLocationRepository,
) -> None:
    vehicle_id = uuid4()
    for step in range(5):
        event = make_event(
            vehicle_id=vehicle_id,
            recorded_at=RECORDED_AT + timedelta(seconds=step),
            latitude=52.5 + step / 100,
            longitude=13.4 + step / 100,
        )
        assert await repository.apply(make_write(event)) is LocationUpdateOutcome.APPLIED

    record = await repository.latest(vehicle_id)
    coordinates = await repository.coordinates(vehicle_id)
    assert record is not None
    assert coordinates is not None
    assert coordinates == pytest.approx((record.latitude, record.longitude))


@pytest.mark.asyncio
async def test_stale_location_calculation(service: LiveLocationService) -> None:
    event = make_event()
    await service.update_from_event(event, received_at=RECORDED_AT + timedelta(seconds=1))

    fresh = await service.get_location(event.vehicle_id, now=RECORDED_AT + timedelta(seconds=30))
    assert fresh is not None
    assert fresh.stale is False
    assert fresh.age_seconds == pytest.approx(30.0)

    stale = await service.get_location(event.vehicle_id, now=RECORDED_AT + timedelta(seconds=61))
    assert stale is not None
    assert stale.stale is True
    assert stale.age_seconds == pytest.approx(61.0)

    future = await service.get_location(event.vehicle_id, now=RECORDED_AT - timedelta(seconds=10))
    assert future is not None
    assert future.stale is False
    assert future.age_seconds == 0.0


@pytest.mark.asyncio
async def test_unknown_vehicle_returns_none(service: LiveLocationService) -> None:
    assert await service.get_location(uuid4()) is None
    assert await service.get_coordinates(uuid4()) is None


class BrokenRedis:
    """Minimal client whose commands always fail with a Redis error."""

    async def eval(self, *args: object, **kwargs: object) -> object:
        del args, kwargs
        raise RedisConnectionError("connection lost")

    async def hgetall(self, *args: object, **kwargs: object) -> object:
        del args, kwargs
        raise RedisConnectionError("connection lost")

    async def geopos(self, *args: object, **kwargs: object) -> object:
        del args, kwargs
        raise RedisConnectionError("connection lost")

    async def geosearch(self, *args: object, **kwargs: object) -> object:
        del args, kwargs
        raise RedisConnectionError("connection lost")


@pytest.mark.asyncio
async def test_repository_translates_redis_errors() -> None:
    repository = LiveLocationRepository(cast(Redis, BrokenRedis()), prefix=PREFIX)
    event = make_event()

    with pytest.raises(LiveLocationStoreError):
        await repository.apply(make_write(event))
    with pytest.raises(LiveLocationStoreError):
        await repository.latest(event.vehicle_id)
    with pytest.raises(LiveLocationStoreError):
        await repository.coordinates(event.vehicle_id)


@pytest.mark.asyncio
async def test_service_translates_redis_errors() -> None:
    repository = LiveLocationRepository(cast(Redis, BrokenRedis()), prefix=PREFIX)
    service = LiveLocationService(repository)
    event = make_event()

    with pytest.raises(LiveLocationUnavailableError):
        await service.update_from_event(event)
    with pytest.raises(LiveLocationUnavailableError):
        await service.get_location(event.vehicle_id)
    with pytest.raises(LiveLocationUnavailableError):
        await service.find_nearby(52.52, 13.405, 5)


@pytest.mark.asyncio
async def test_service_validates_query_arguments(service: LiveLocationService) -> None:
    with pytest.raises(ValueError):
        await service.find_nearby(91, 13.405, 5)
    with pytest.raises(ValueError):
        await service.find_nearby(52.52, 181, 5)
    with pytest.raises(ValueError):
        await service.find_nearby(52.52, 13.405, 0)
    with pytest.raises(ValueError):
        await service.find_nearby(52.52, 13.405, 5, limit=0)
    with pytest.raises(ValueError):
        LiveLocationService(
            LiveLocationRepository(cast(Redis, BrokenRedis())), stale_after_seconds=0
        )


@pytest.mark.asyncio
async def test_repository_pages_and_scans_the_geo_index(
    repository: LiveLocationRepository,
) -> None:
    events = [
        make_event(latitude=52.50 + step / 100, longitude=13.40 + step / 100) for step in range(5)
    ]
    for event in events:
        await repository.apply(make_write(event))

    assert await repository.count_indexed() == 5
    first_page = await repository.page_indexed(offset=0, limit=2)
    second_page = await repository.page_indexed(offset=2, limit=2)
    assert len(first_page) == 2
    assert len(second_page) == 2
    assert {record.vehicle_id for record in first_page}.isdisjoint(
        {record.vehicle_id for record in second_page}
    )

    scanned = []
    async for batch in repository.scan_indexed(batch_size=2):
        assert len(batch) <= 2
        scanned.extend(batch)
    assert {record.vehicle_id for record in scanned} == {event.vehicle_id for event in events}


@pytest.mark.asyncio
async def test_repository_nearby_records_include_hashes_and_distances(
    repository: LiveLocationRepository,
) -> None:
    near = make_event(latitude=52.5200, longitude=13.4050)
    farther = make_event(latitude=52.5300, longitude=13.4150)
    await repository.apply(make_write(near))
    await repository.apply(make_write(farther))

    entries = await repository.nearby_records(52.5200, 13.4050, radius_km=5, limit=10)

    assert [entry.location.vehicle_id for entry in entries] == [
        near.vehicle_id,
        farther.vehicle_id,
    ]
    assert entries[0].distance_km <= entries[1].distance_km
    assert entries[0].location.event_id == near.event_id


@pytest.mark.asyncio
async def test_repository_skips_malformed_hashes_in_pages(
    repository: LiveLocationRepository,
    redis_client: fakeredis.aioredis.FakeRedis,
) -> None:
    valid = make_event()
    await repository.apply(make_write(valid))
    broken_vehicle = uuid4()
    await cast(
        Awaitable[int],
        redis_client.hset(
            repository.keys.location(broken_vehicle),
            mapping={"vehicle_id": str(broken_vehicle), "latitude": "not-a-number"},
        ),
    )
    await redis_client.geoadd(
        repository.keys.geo,
        (13.4, 52.5, str(broken_vehicle)),
    )

    assert await repository.count_indexed() == 2
    page = await repository.page_indexed(offset=0, limit=10)
    assert [record.vehicle_id for record in page] == [valid.vehicle_id]
