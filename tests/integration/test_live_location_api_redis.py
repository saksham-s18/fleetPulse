"""API smoke tests for live-location reads against a real Redis server.

Run with the Compose Redis service:

    docker compose up -d redis
    pytest -m redis

Each test uses an isolated namespace and removes its keys afterwards.
"""

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx2 as httpx
import pytest
import pytest_asyncio
from redis.asyncio import Redis

from app.api.dependencies import get_live_location_service
from app.core.config import get_settings
from app.infrastructure.redis import create_redis_client, ensure_redis_ready
from app.main import create_app
from app.repositories.live_location import LiveLocationRepository
from app.schemas.gps import GpsEvent
from app.services.live_location import LiveLocationService

pytestmark = pytest.mark.redis

RECORDED_AT = datetime.now(UTC) - timedelta(seconds=10)


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
    prefix = f"fleetpulse-api-test-{uuid4().hex[:12]}"
    try:
        yield prefix
    finally:
        keys = [key async for key in redis_client.scan_iter(match=f"{prefix}:*")]
        if keys:
            await redis_client.delete(*keys)


@pytest_asyncio.fixture
async def service(redis_client: Redis, namespace: str) -> LiveLocationService:
    return LiveLocationService(
        LiveLocationRepository(redis_client, prefix=namespace),
        stale_after_seconds=60,
        max_page_size=100,
        max_nearby_radius_km=100,
        max_nearby_limit=100,
    )


@pytest_asyncio.fixture
async def client(service: LiveLocationService) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app()
    app.dependency_overrides[get_live_location_service] = lambda: service
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    ) as test_client:
        yield test_client


@pytest.mark.asyncio
async def test_single_list_and_nearby_against_real_redis(
    client: httpx.AsyncClient,
    service: LiveLocationService,
) -> None:
    near = make_event(latitude=52.5200, longitude=13.4050)
    farther = make_event(latitude=52.5300, longitude=13.4150)
    stale = make_event(
        latitude=52.5400,
        longitude=13.4250,
        recorded_at=RECORDED_AT - timedelta(seconds=120),
    )
    for event in (near, farther, stale):
        await service.update_from_event(event)

    single = await client.get(f"/api/v1/locations/{near.vehicle_id}")
    assert single.status_code == 200
    assert single.json()["event_id"] == str(near.event_id)
    assert single.json()["is_stale"] is False

    listing = await client.get("/api/v1/locations", params={"page_size": 2})
    assert listing.status_code == 200
    assert listing.json()["total"] == 3
    assert listing.json()["total_pages"] == 2

    stale_listing = await client.get("/api/v1/locations", params={"is_stale": "true"})
    assert stale_listing.status_code == 200
    assert [item["vehicle_id"] for item in stale_listing.json()["items"]] == [str(stale.vehicle_id)]

    nearby = await client.get(
        "/api/v1/locations/nearby",
        params={"latitude": 52.52, "longitude": 13.405, "radius": 5, "unit": "km"},
    )
    assert nearby.status_code == 200
    ids = [item["vehicle_id"] for item in nearby.json()["items"]]
    assert ids == [str(near.vehicle_id), str(farther.vehicle_id), str(stale.vehicle_id)]
    distances = [item["distance"] for item in nearby.json()["items"]]
    assert distances == sorted(distances)


@pytest.mark.asyncio
async def test_missing_and_validation_errors_against_real_redis(
    client: httpx.AsyncClient,
) -> None:
    missing = await client.get(f"/api/v1/locations/{uuid4()}")
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "not_found"

    invalid = await client.get(
        "/api/v1/locations/nearby",
        params={"latitude": 91, "longitude": 13.405, "radius": 5},
    )
    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "validation_error"
