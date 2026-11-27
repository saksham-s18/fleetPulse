"""Unit tests for the live-location REST endpoints (fakeredis, no server)."""

from collections.abc import AsyncIterator, Awaitable
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID, uuid4

import fakeredis.aioredis
import httpx2 as httpx
import pytest
import pytest_asyncio
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from app.api.dependencies import get_live_location_service
from app.main import create_app
from app.repositories.live_location import LiveLocationRepository
from app.schemas.gps import GpsEvent
from app.services.live_location import LiveLocationService

PREFIX = "fleetpulse-api-test"
NOW = datetime.now(UTC)


def make_event(
    *,
    vehicle_id: UUID | None = None,
    latitude: float = 52.52,
    longitude: float = 13.405,
    recorded_at: datetime | None = None,
    shipment_id: UUID | None = None,
) -> GpsEvent:
    return GpsEvent.model_validate(
        {
            "event_id": uuid4(),
            "vehicle_id": vehicle_id or uuid4(),
            "shipment_id": shipment_id,
            "latitude": latitude,
            "longitude": longitude,
            "speed_kmph": 31.5,
            "heading": 87.5,
            "recorded_at": recorded_at or NOW,
        }
    )


async def seed(
    service: LiveLocationService,
    *,
    vehicle_id: UUID | None = None,
    latitude: float = 52.52,
    longitude: float = 13.405,
    recorded_at: datetime | None = None,
    shipment_id: UUID | None = None,
) -> GpsEvent:
    event = make_event(
        vehicle_id=vehicle_id,
        latitude=latitude,
        longitude=longitude,
        recorded_at=recorded_at,
        shipment_id=shipment_id,
    )
    await service.update_from_event(event, received_at=NOW)
    return event


class BrokenRedis:
    """Client whose read commands always fail like an unreachable Redis."""

    async def hgetall(self, *args: object, **kwargs: object) -> object:
        del args, kwargs
        raise RedisConnectionError("connection lost")

    async def zcard(self, *args: object, **kwargs: object) -> object:
        del args, kwargs
        raise RedisConnectionError("connection lost")

    async def zrange(self, *args: object, **kwargs: object) -> object:
        del args, kwargs
        raise RedisConnectionError("connection lost")

    async def geosearch(self, *args: object, **kwargs: object) -> object:
        del args, kwargs
        raise RedisConnectionError("connection lost")


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[fakeredis.aioredis.FakeRedis]:
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()


@pytest_asyncio.fixture
async def service(redis_client: fakeredis.aioredis.FakeRedis) -> LiveLocationService:
    return LiveLocationService(
        LiveLocationRepository(cast(Redis, redis_client), prefix=PREFIX),
        stale_after_seconds=60,
        max_page_size=100,
        max_nearby_radius_km=100,
        max_nearby_limit=100,
    )


def build_client(service: LiveLocationService) -> httpx.AsyncClient:
    app = create_app()
    app.dependency_overrides[get_live_location_service] = lambda: service
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    )


@pytest_asyncio.fixture
async def client(service: LiveLocationService) -> AsyncIterator[httpx.AsyncClient]:
    async with build_client(service) as test_client:
        yield test_client


@pytest.mark.asyncio
async def test_get_single_location_includes_freshness(
    client: httpx.AsyncClient,
    service: LiveLocationService,
) -> None:
    shipment_id = uuid4()
    event = await seed(
        service,
        latitude=52.52,
        longitude=13.405,
        recorded_at=NOW - timedelta(seconds=10),
        shipment_id=shipment_id,
    )

    response = await client.get(f"/api/v1/locations/{event.vehicle_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["vehicle_id"] == str(event.vehicle_id)
    assert body["event_id"] == str(event.event_id)
    assert body["latitude"] == pytest.approx(52.52)
    assert body["longitude"] == pytest.approx(13.405)
    assert body["speed_kmh"] == pytest.approx(31.5)
    assert body["heading"] == pytest.approx(87.5)
    assert body["shipment_id"] == str(shipment_id)
    assert body["is_stale"] is False
    assert body["age_seconds"] >= 10


@pytest.mark.asyncio
async def test_missing_location_returns_404_envelope(client: httpx.AsyncClient) -> None:
    response = await client.get(f"/api/v1/locations/{uuid4()}")

    assert response.status_code == 404
    body = response.json()
    assert body["error"]["code"] == "not_found"
    assert body["error"]["message"] == "No live location found for this vehicle."
    assert body["error"]["details"] is None


@pytest.mark.asyncio
async def test_invalid_vehicle_uuid_returns_422(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/locations/not-a-uuid")

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


@pytest.mark.asyncio
async def test_list_pagination_is_deterministic(
    client: httpx.AsyncClient,
    service: LiveLocationService,
) -> None:
    events = [
        await seed(
            service,
            latitude=52.50 + index / 100,
            longitude=13.40 + index / 100,
        )
        for index in range(5)
    ]

    seen: list[str] = []
    for page in (1, 2, 3):
        response = await client.get(
            "/api/v1/locations",
            params={"page": page, "page_size": 2},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["page"] == page
        assert body["page_size"] == 2
        assert body["total"] == 5
        assert body["total_pages"] == 3
        seen.extend(item["vehicle_id"] for item in body["items"])

    assert len(seen) == 5
    assert set(seen) == {str(event.vehicle_id) for event in events}


@pytest.mark.asyncio
async def test_empty_location_list(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/locations")

    assert response.status_code == 200
    assert response.json() == {
        "items": [],
        "page": 1,
        "page_size": 20,
        "total": 0,
        "total_pages": 0,
    }


@pytest.mark.asyncio
async def test_stale_filter_separates_fresh_and_stale(
    client: httpx.AsyncClient,
    service: LiveLocationService,
) -> None:
    fresh = await seed(service, recorded_at=NOW - timedelta(seconds=5))
    stale = await seed(service, recorded_at=NOW - timedelta(seconds=120))

    stale_response = await client.get("/api/v1/locations", params={"is_stale": "true"})
    assert stale_response.status_code == 200
    stale_body = stale_response.json()
    assert stale_body["total"] == 1
    assert [item["vehicle_id"] for item in stale_body["items"]] == [str(stale.vehicle_id)]
    assert stale_body["items"][0]["is_stale"] is True

    fresh_response = await client.get("/api/v1/locations", params={"is_stale": "false"})
    assert fresh_response.status_code == 200
    fresh_body = fresh_response.json()
    assert fresh_body["total"] == 1
    assert [item["vehicle_id"] for item in fresh_body["items"]] == [str(fresh.vehicle_id)]
    assert fresh_body["items"][0]["is_stale"] is False


@pytest.mark.asyncio
async def test_list_vehicle_id_filter(
    client: httpx.AsyncClient,
    service: LiveLocationService,
) -> None:
    target = await seed(service)
    await seed(service)

    response = await client.get(
        "/api/v1/locations",
        params={"vehicle_id": str(target.vehicle_id)},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert [item["vehicle_id"] for item in body["items"]] == [str(target.vehicle_id)]


@pytest.mark.asyncio
async def test_page_size_above_configured_maximum_returns_422(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/locations", params={"page_size": 101})

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


@pytest.mark.asyncio
async def test_nearby_search_is_distance_ordered(
    client: httpx.AsyncClient,
    service: LiveLocationService,
) -> None:
    near = await seed(service, latitude=52.5200, longitude=13.4050)
    middle = await seed(service, latitude=52.5300, longitude=13.4150)
    far = await seed(service, latitude=52.5500, longitude=13.4350)
    await seed(service, latitude=48.1372, longitude=11.5756)

    response = await client.get(
        "/api/v1/locations/nearby",
        params={"latitude": 52.5200, "longitude": 13.4050, "radius": 5, "unit": "km"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["unit"] == "km"
    assert body["radius"] == pytest.approx(5.0)
    assert body["center"] == {"latitude": 52.52, "longitude": 13.405}
    ids = [item["vehicle_id"] for item in body["items"]]
    assert ids == [str(near.vehicle_id), str(middle.vehicle_id), str(far.vehicle_id)]
    distances = [item["distance"] for item in body["items"]]
    assert distances == sorted(distances)
    assert distances[0] == pytest.approx(0.0, abs=0.01)
    assert body["items"][0]["is_stale"] is False


@pytest.mark.asyncio
async def test_nearby_distance_unit_conversion(
    client: httpx.AsyncClient,
    service: LiveLocationService,
) -> None:
    await seed(service, latitude=52.5200, longitude=13.4050)
    await seed(service, latitude=52.5300, longitude=13.4150)

    kilometres = await client.get(
        "/api/v1/locations/nearby",
        params={"latitude": 52.52, "longitude": 13.405, "radius": 5, "unit": "km"},
    )
    metres = await client.get(
        "/api/v1/locations/nearby",
        params={"latitude": 52.52, "longitude": 13.405, "radius": 5000, "unit": "m"},
    )
    miles = await client.get(
        "/api/v1/locations/nearby",
        params={"latitude": 52.52, "longitude": 13.405, "radius": 5, "unit": "mi"},
    )

    assert kilometres.status_code == metres.status_code == miles.status_code == 200
    km_distance = kilometres.json()["items"][1]["distance"]
    m_distance = metres.json()["items"][1]["distance"]
    mi_distance = miles.json()["items"][1]["distance"]
    assert m_distance == pytest.approx(km_distance * 1000, rel=1e-3)
    assert mi_distance == pytest.approx(km_distance / 1.609344, rel=1e-3)
    assert metres.json()["radius"] == pytest.approx(5000.0)


@pytest.mark.asyncio
async def test_nearby_empty_result_is_200(
    client: httpx.AsyncClient,
    service: LiveLocationService,
) -> None:
    await seed(service, latitude=48.1372, longitude=11.5756)

    response = await client.get(
        "/api/v1/locations/nearby",
        params={"latitude": 52.52, "longitude": 13.405, "radius": 1},
    )

    assert response.status_code == 200
    assert response.json()["items"] == []


@pytest.mark.asyncio
async def test_nearby_excludes_stale_when_requested(
    client: httpx.AsyncClient,
    service: LiveLocationService,
) -> None:
    fresh = await seed(service, latitude=52.5200, longitude=13.4050)
    stale = await seed(
        service,
        latitude=52.5201,
        longitude=13.4051,
        recorded_at=NOW - timedelta(seconds=120),
    )

    included = await client.get(
        "/api/v1/locations/nearby",
        params={"latitude": 52.52, "longitude": 13.405, "radius": 2},
    )
    excluded = await client.get(
        "/api/v1/locations/nearby",
        params={
            "latitude": 52.52,
            "longitude": 13.405,
            "radius": 2,
            "exclude_stale": "true",
        },
    )

    assert {item["vehicle_id"] for item in included.json()["items"]} == {
        str(fresh.vehicle_id),
        str(stale.vehicle_id),
    }
    assert [item["vehicle_id"] for item in excluded.json()["items"]] == [str(fresh.vehicle_id)]


@pytest.mark.parametrize(
    ("params", "expected"),
    [
        ({"latitude": 91, "longitude": 13.405, "radius": 5}, 422),
        ({"latitude": 52.52, "longitude": 181, "radius": 5}, 422),
        ({"latitude": 52.52, "longitude": 13.405, "radius": 0}, 422),
        ({"latitude": 52.52, "longitude": 13.405, "radius": 5, "limit": 0}, 422),
        ({"latitude": 52.52, "longitude": 13.405, "radius": 5, "unit": "yards"}, 422),
        ({"latitude": 52.52, "longitude": 13.405, "radius": 5, "limit": 101}, 422),
        ({"latitude": 52.52, "longitude": 13.405, "radius": 101}, 422),
    ],
)
@pytest.mark.asyncio
async def test_nearby_validation_errors(
    client: httpx.AsyncClient,
    params: dict[str, str | int | float],
    expected: int,
) -> None:
    response = await client.get("/api/v1/locations/nearby", params=params)

    assert response.status_code == expected
    assert response.json()["error"]["code"] == "validation_error"


@pytest.mark.asyncio
async def test_malformed_hash_is_safe_for_single_and_list(
    client: httpx.AsyncClient,
    service: LiveLocationService,
    redis_client: fakeredis.aioredis.FakeRedis,
) -> None:
    valid = await seed(service, latitude=52.52, longitude=13.405)
    broken_vehicle = uuid4()
    repository = LiveLocationRepository(cast(Redis, redis_client), prefix=PREFIX)
    await cast(
        Awaitable[int],
        redis_client.hset(
            repository.keys.location(broken_vehicle),
            mapping={"vehicle_id": str(broken_vehicle), "latitude": "not-a-number"},
        ),
    )
    await redis_client.geoadd(repository.keys.geo, (13.4, 52.5, str(broken_vehicle)))

    single = await client.get(f"/api/v1/locations/{broken_vehicle}")
    assert single.status_code == 503
    assert single.json()["error"]["code"] == "service_unavailable"

    listing = await client.get("/api/v1/locations")
    assert listing.status_code == 200
    body = listing.json()
    assert body["total"] == 2
    assert [item["vehicle_id"] for item in body["items"]] == [str(valid.vehicle_id)]


@pytest.mark.asyncio
async def test_redis_unavailable_returns_503() -> None:
    service = LiveLocationService(LiveLocationRepository(cast(Redis, BrokenRedis()), prefix=PREFIX))
    async with build_client(service) as client:
        single = await client.get(f"/api/v1/locations/{uuid4()}")
        listing = await client.get("/api/v1/locations")
        nearby = await client.get(
            "/api/v1/locations/nearby",
            params={"latitude": 52.52, "longitude": 13.405, "radius": 5},
        )

    for response in (single, listing, nearby):
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "service_unavailable"
        assert "Redis" not in response.text


@pytest.mark.asyncio
async def test_pagination_reflects_live_dataset_changes(
    client: httpx.AsyncClient,
    service: LiveLocationService,
) -> None:
    for _ in range(3):
        await seed(service)

    first = await client.get("/api/v1/locations", params={"page": 1, "page_size": 2})
    assert first.status_code == 200
    assert first.json()["total"] == 3

    await seed(service)

    second = await client.get("/api/v1/locations", params={"page": 2, "page_size": 2})
    assert second.status_code == 200
    assert second.json()["total"] == 4
    assert second.json()["total_pages"] == 2


@pytest.mark.asyncio
async def test_openapi_documents_location_read_endpoints(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/openapi.json")

    assert response.status_code == 200
    paths = response.json()["paths"]
    assert "get" in paths["/api/v1/locations"]
    assert "post" in paths["/api/v1/locations"]
    assert "get" in paths["/api/v1/locations/{vehicle_id}"]
    assert "get" in paths["/api/v1/locations/nearby"]
    assert "404" in paths["/api/v1/locations/{vehicle_id}"]["get"]["responses"]
    assert "503" in paths["/api/v1/locations"]["get"]["responses"]
