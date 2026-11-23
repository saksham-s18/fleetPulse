"""Unit tests for route movement, event generation, transport and shutdown."""

import asyncio
import io
import json
import logging
from datetime import UTC, datetime, timedelta
from email.message import Message
from random import Random
from typing import Any
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

import pytest

from app.schemas.gps import GpsEvent
from simulator.cli import build_parser, main
from simulator.generator import GpsEventGenerator
from simulator.geo import haversine_km, initial_bearing_degrees
from simulator.routes import DEFAULT_ROUTES, Route
from simulator.runner import SimulationRunner
from simulator.transport import DryRunPublisher, HttpEventPublisher, TransportError
from simulator.vehicles import VehicleDiscoveryError, fetch_active_vehicle_ids


def make_event() -> GpsEvent:
    return GpsEvent(
        event_id=uuid4(),
        vehicle_id=uuid4(),
        latitude=52.52,
        longitude=13.405,
        speed_kmph=30.0,
        heading=45.0,
        recorded_at=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
    )


def run_generator(generator: GpsEventGenerator, ticks: int = 5) -> list[dict[str, Any]]:
    start = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    snapshots: list[dict[str, Any]] = []
    for tick in range(ticks):
        events = generator.generate(start + timedelta(seconds=tick), 1.0)
        snapshots.extend(event.model_dump(exclude={"event_id"}) for event in events)
    return snapshots


class RecordingPublisher:
    def __init__(
        self,
        *,
        stop_after: int | None = None,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        self.events: list[GpsEvent] = []
        self.closed = False
        self._stop_after = stop_after
        self._stop_event = stop_event

    async def publish(self, event: GpsEvent) -> None:
        self.events.append(event)
        if (
            self._stop_after is not None
            and self._stop_event is not None
            and len(self.events) >= self._stop_after
        ):
            self._stop_event.set()

    async def close(self) -> None:
        self.closed = True


class FailingPublisher:
    def __init__(self, stop_event: asyncio.Event) -> None:
        self.attempts = 0
        self.closed = False
        self._stop_event = stop_event

    async def publish(self, event: GpsEvent) -> None:
        self.attempts += 1
        self._stop_event.set()
        raise TransportError("ingestion endpoint unavailable")

    async def close(self) -> None:
        self.closed = True


class FakeResponse:
    def __init__(self, status: int = 202) -> None:
        self.status = status

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        return None


def test_haversine_distance_and_bearing() -> None:
    assert haversine_km(52.52, 13.405, 52.52, 13.405) == pytest.approx(0.0)
    assert haversine_km(52.52, 13.405, 53.5511, 9.9937) == pytest.approx(255.0, abs=5.0)
    assert haversine_km(0.0, 0.0, 0.0, 180.0) == pytest.approx(20015.1, rel=0.001)

    assert initial_bearing_degrees(0.0, 0.0, 1.0, 0.0) == pytest.approx(0.0)
    assert initial_bearing_degrees(0.0, 0.0, 0.0, 1.0) == pytest.approx(90.0)
    assert initial_bearing_degrees(0.0, 0.0, -1.0, 0.0) == pytest.approx(180.0)
    assert initial_bearing_degrees(0.0, 0.0, 0.0, -1.0) == pytest.approx(270.0)


def test_route_interpolates_position_and_heading() -> None:
    route = Route(
        name="Equator loop",
        waypoints=((0.0, 0.0), (0.0, 1.0), (0.0, 0.0)),
        min_speed_kmph=10.0,
        max_speed_kmph=20.0,
    )

    start = route.position_at(0.0)
    assert start.latitude == pytest.approx(0.0)
    assert start.longitude == pytest.approx(0.0)
    assert start.heading_degrees == pytest.approx(90.0)

    outbound = route.position_at(route.length_km * 0.25)
    assert outbound.latitude == pytest.approx(0.0)
    assert outbound.longitude == pytest.approx(0.5)
    assert outbound.heading_degrees == pytest.approx(90.0)

    inbound = route.position_at(route.length_km * 0.75)
    assert inbound.latitude == pytest.approx(0.0)
    assert inbound.longitude == pytest.approx(0.5)
    assert inbound.heading_degrees == pytest.approx(270.0)

    wrapped = route.position_at(route.length_km)
    assert wrapped.longitude == pytest.approx(0.0)
    assert wrapped.heading_degrees == pytest.approx(90.0)
    assert route.length_km == pytest.approx(222.39, rel=0.01)


@pytest.mark.parametrize(
    ("waypoints", "min_speed_kmph", "max_speed_kmph"),
    [
        (((0.0, 0.0),), 10.0, 20.0),
        (((0.0, 0.0), (0.0, 0.0)), 10.0, 20.0),
        (((91.0, 0.0), (0.0, 0.0)), 10.0, 20.0),
        (((0.0, 0.0), (0.0, 181.0)), 10.0, 20.0),
        (((0.0, 0.0), (0.0, 1.0)), -1.0, 20.0),
        (((0.0, 0.0), (0.0, 1.0)), 30.0, 20.0),
    ],
)
def test_route_rejects_invalid_definitions(
    waypoints: tuple[tuple[float, float], ...],
    min_speed_kmph: float,
    max_speed_kmph: float,
) -> None:
    with pytest.raises(ValueError):
        Route(
            name="invalid",
            waypoints=waypoints,
            min_speed_kmph=min_speed_kmph,
            max_speed_kmph=max_speed_kmph,
        )


def test_route_rejects_non_finite_distance() -> None:
    route = DEFAULT_ROUTES[0]

    with pytest.raises(ValueError):
        route.position_at(float("nan"))


def test_default_routes_are_closed_and_valid() -> None:
    assert DEFAULT_ROUTES

    for route in DEFAULT_ROUTES:
        assert route.waypoints[0] == route.waypoints[-1]
        assert len(route.waypoints) >= 3
        assert route.length_km > 0
        assert 0 <= route.min_speed_kmph <= route.max_speed_kmph
        for latitude, longitude in route.waypoints:
            assert -90 <= latitude <= 90
            assert -180 <= longitude <= 180


def test_generator_emits_one_event_per_vehicle() -> None:
    vehicle_ids = [uuid4(), uuid4(), uuid4()]
    generator = GpsEventGenerator(vehicle_ids, seed=1)

    events = generator.generate(datetime(2026, 1, 1, 12, 0, tzinfo=UTC), 1.0)

    assert len(events) == len(vehicle_ids)
    assert {event.vehicle_id for event in events} == set(vehicle_ids)
    assert len({event.event_id for event in events}) == len(events)


def test_generator_deduplicates_vehicle_ids() -> None:
    vehicle_id = uuid4()

    generator = GpsEventGenerator([vehicle_id, vehicle_id], seed=1)

    assert generator.vehicle_ids == (vehicle_id,)


def test_generator_emits_utc_timestamps_and_plausible_values() -> None:
    vehicle_ids = [uuid4(), uuid4()]
    generator = GpsEventGenerator(vehicle_ids, seed=11)
    start = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

    for tick in range(20):
        recorded_at = start + timedelta(seconds=tick)
        events = generator.generate(recorded_at, 1.0)

        for event in events:
            assert event.recorded_at.tzinfo is UTC
            assert event.recorded_at == recorded_at
            assert -90 <= event.latitude <= 90
            assert -180 <= event.longitude <= 180
            assert event.heading is not None
            assert 0 <= event.heading <= 360

            route = generator.routes_by_vehicle[event.vehicle_id]
            assert route.min_speed_kmph <= event.speed_kmph <= route.max_speed_kmph


def test_generator_moves_vehicles_between_ticks() -> None:
    generator = GpsEventGenerator([uuid4()], seed=3)
    start = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

    first = generator.generate(start, 0.0)[0]
    second = generator.generate(start + timedelta(seconds=5), 5.0)[0]

    assert (first.latitude, first.longitude) != (second.latitude, second.longitude)
    assert second.speed_kmph > 0


def test_generator_is_reproducible_with_the_same_seed() -> None:
    vehicle_ids = [uuid4(), uuid4()]
    first = GpsEventGenerator(vehicle_ids, seed=1234)
    second = GpsEventGenerator(vehicle_ids, seed=1234)

    assert first.routes_by_vehicle == second.routes_by_vehicle
    assert run_generator(first) == run_generator(second)


def test_generator_changes_movement_with_a_different_seed() -> None:
    vehicle_ids = [uuid4(), uuid4()]
    first = GpsEventGenerator(vehicle_ids, seed=1234)
    second = GpsEventGenerator(vehicle_ids, seed=4321)

    assert run_generator(first) != run_generator(second)


def test_generator_event_ids_are_unique_but_not_reproducible() -> None:
    vehicle_id = uuid4()
    start = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    first = GpsEventGenerator([vehicle_id], seed=9).generate(start, 1.0)[0]
    second = GpsEventGenerator([vehicle_id], seed=9).generate(start, 1.0)[0]

    assert first.event_id != second.event_id
    assert first.model_dump(exclude={"event_id"}) == second.model_dump(exclude={"event_id"})


def test_generator_produces_unique_event_ids_across_ticks() -> None:
    generator = GpsEventGenerator([uuid4(), uuid4()], seed=7)
    start = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

    event_ids = [
        event.event_id
        for tick in range(10)
        for event in generator.generate(start + timedelta(seconds=tick), 1.0)
    ]

    assert len(event_ids) == len(set(event_ids))


def test_generator_rejects_invalid_configuration() -> None:
    with pytest.raises(ValueError):
        GpsEventGenerator([])

    with pytest.raises(ValueError):
        GpsEventGenerator([uuid4()], routes=[])

    with pytest.raises(ValueError):
        GpsEventGenerator([uuid4()], seed=1, rng=Random(1))


def test_generator_rejects_negative_elapsed_time() -> None:
    generator = GpsEventGenerator([uuid4()], seed=1)

    with pytest.raises(ValueError):
        generator.generate(datetime(2026, 1, 1, 12, 0, tzinfo=UTC), -1.0)


@pytest.mark.asyncio
async def test_runner_stops_gracefully_after_an_in_flight_tick() -> None:
    stop_event = asyncio.Event()
    publisher = RecordingPublisher(stop_after=3, stop_event=stop_event)
    generator = GpsEventGenerator([uuid4()], seed=5)
    runner = SimulationRunner(
        generator,
        publisher,
        interval_seconds=0.001,
        stop_event=stop_event,
    )

    stats = await runner.run()

    assert len(publisher.events) == 3
    assert stats.events_sent == 3
    assert stats.events_failed == 0
    assert stats.ticks == 3
    assert publisher.closed


@pytest.mark.asyncio
async def test_runner_stops_immediately_when_already_stopped() -> None:
    stop_event = asyncio.Event()
    stop_event.set()
    publisher = RecordingPublisher()
    generator = GpsEventGenerator([uuid4()], seed=5)
    runner = SimulationRunner(
        generator,
        publisher,
        interval_seconds=0.001,
        stop_event=stop_event,
    )

    stats = await runner.run()

    assert publisher.events == []
    assert stats.ticks == 0
    assert stats.events_sent == 0
    assert publisher.closed


@pytest.mark.asyncio
async def test_runner_counts_publish_failures_and_closes_transport() -> None:
    stop_event = asyncio.Event()
    publisher = FailingPublisher(stop_event)
    generator = GpsEventGenerator([uuid4()], seed=5)
    runner = SimulationRunner(
        generator,
        publisher,
        interval_seconds=0.001,
        stop_event=stop_event,
    )

    stats = await runner.run()

    assert publisher.attempts == 1
    assert stats.events_failed == 1
    assert stats.events_sent == 0
    assert publisher.closed


@pytest.mark.asyncio
async def test_runner_respects_duration() -> None:
    publisher = RecordingPublisher()
    generator = GpsEventGenerator([uuid4(), uuid4()], seed=5)
    runner = SimulationRunner(
        generator,
        publisher,
        interval_seconds=0.005,
        duration_seconds=0.02,
    )

    stats = await runner.run()

    assert stats.ticks >= 1
    assert stats.events_sent == len(publisher.events)
    assert stats.events_sent == stats.ticks * 2
    assert publisher.closed


@pytest.mark.asyncio
async def test_http_publisher_posts_json(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_urlopen(request: Any, timeout: float) -> FakeResponse:
        captured["url"] = request.full_url
        captured["method"] = request.get_method()
        captured["content_type"] = request.get_header("Content-type")
        captured["body"] = request.data
        captured["timeout"] = timeout
        return FakeResponse(202)

    monkeypatch.setattr("simulator.transport.urlopen", fake_urlopen)

    event = make_event()
    publisher = HttpEventPublisher(
        "http://localhost:8000/api/v1/gps/events",
        timeout_seconds=3.0,
    )
    await publisher.publish(event)
    await publisher.close()

    assert captured["url"] == "http://localhost:8000/api/v1/gps/events"
    assert captured["method"] == "POST"
    assert captured["content_type"] == "application/json"
    assert captured["timeout"] == 3.0

    body = json.loads(captured["body"].decode("utf-8"))
    assert body["event_id"] == str(event.event_id)
    assert body["vehicle_id"] == str(event.vehicle_id)
    assert body["schema_version"] == "1.0"
    assert body["recorded_at"].endswith(("Z", "+00:00"))


@pytest.mark.asyncio
async def test_http_publisher_wraps_http_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def failing_urlopen(request: Any, timeout: float) -> FakeResponse:
        raise HTTPError(
            "http://example.test/events",
            503,
            "unavailable",
            Message(),
            None,
        )

    monkeypatch.setattr("simulator.transport.urlopen", failing_urlopen)
    publisher = HttpEventPublisher("http://example.test/events")

    with pytest.raises(TransportError, match="503"):
        await publisher.publish(make_event())


@pytest.mark.asyncio
async def test_http_publisher_rejects_non_success_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def non_success_urlopen(request: Any, timeout: float) -> FakeResponse:
        return FakeResponse(500)

    monkeypatch.setattr("simulator.transport.urlopen", non_success_urlopen)
    publisher = HttpEventPublisher("http://example.test/events")

    with pytest.raises(TransportError, match="500"):
        await publisher.publish(make_event())


@pytest.mark.asyncio
async def test_http_publisher_wraps_network_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def failing_urlopen(request: Any, timeout: float) -> FakeResponse:
        raise OSError("connection refused")

    monkeypatch.setattr("simulator.transport.urlopen", failing_urlopen)
    publisher = HttpEventPublisher("http://example.test/events")

    with pytest.raises(TransportError, match="connection refused"):
        await publisher.publish(make_event())


@pytest.mark.asyncio
async def test_http_publisher_reports_api_error_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = json.dumps({"error": {"code": "not_found", "message": "Vehicle not found."}}).encode(
        "utf-8"
    )

    def failing_urlopen(request: Any, timeout: float) -> FakeResponse:
        raise HTTPError(
            "http://example.test/api/v1/locations",
            404,
            "not found",
            Message(),
            io.BytesIO(body),
        )

    monkeypatch.setattr("simulator.transport.urlopen", failing_urlopen)
    publisher = HttpEventPublisher("http://example.test/api/v1/locations")

    with pytest.raises(TransportError) as excinfo:
        await publisher.publish(make_event())

    assert "404" in str(excinfo.value)
    assert "Vehicle not found." in str(excinfo.value)


@pytest.mark.asyncio
async def test_http_publisher_ignores_unreadable_error_bodies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def failing_urlopen(request: Any, timeout: float) -> FakeResponse:
        raise HTTPError("http://example.test/events", 500, "boom", Message(), None)

    monkeypatch.setattr("simulator.transport.urlopen", failing_urlopen)
    publisher = HttpEventPublisher("http://example.test/events")

    with pytest.raises(TransportError, match="500"):
        await publisher.publish(make_event())


@pytest.mark.parametrize(
    "endpoint",
    ["", "not-a-url", "ftp://example.test/events", "http:///events"],
)
def test_http_publisher_rejects_invalid_endpoint(endpoint: str) -> None:
    with pytest.raises(ValueError):
        HttpEventPublisher(endpoint)


def test_http_publisher_rejects_invalid_timeout() -> None:
    with pytest.raises(ValueError):
        HttpEventPublisher("http://example.test/events", timeout_seconds=0)


@pytest.mark.asyncio
async def test_dry_run_publisher_logs_events(caplog: pytest.LogCaptureFixture) -> None:
    publisher = DryRunPublisher()

    with caplog.at_level(logging.INFO, logger="simulator.transport"):
        await publisher.publish(make_event())
    await publisher.close()

    assert "gps_event" in caplog.text


@pytest.mark.asyncio
async def test_vehicle_discovery_paginates_and_filters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active_a, active_b, inactive = uuid4(), uuid4(), uuid4()
    pages: dict[int, dict[str, object]] = {
        1: {
            "items": [
                {"id": str(active_a), "status": "active"},
                {"id": str(inactive), "status": "inactive"},
            ],
            "pages": 2,
        },
        2: {
            "items": [{"id": str(active_b), "status": "active"}],
            "pages": 2,
        },
    }
    requested_urls: list[str] = []

    def fake_get_json(url: str, timeout_seconds: float) -> object:
        requested_urls.append(url)
        query = parse_qs(urlparse(url).query)
        assert query["status"] == ["active"]
        return pages[int(query["page"][0])]

    monkeypatch.setattr("simulator.vehicles._get_json", fake_get_json)

    vehicle_ids = await fetch_active_vehicle_ids(
        "http://fleetpulse.test",
        page_size=1,
    )

    assert vehicle_ids == [active_a, active_b]
    assert len(requested_urls) == 2


@pytest.mark.asyncio
async def test_vehicle_discovery_returns_empty_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_get_json(url: str, timeout_seconds: float) -> object:
        # The FleetPulse API reports pages=0 when there are no matches.
        return {"items": [], "pages": 0}

    monkeypatch.setattr("simulator.vehicles._get_json", fake_get_json)

    assert await fetch_active_vehicle_ids("http://fleetpulse.test") == []


@pytest.mark.asyncio
async def test_vehicle_discovery_rejects_invalid_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_get_json(url: str, timeout_seconds: float) -> object:
        return {"items": "not-a-list"}

    monkeypatch.setattr("simulator.vehicles._get_json", fake_get_json)

    with pytest.raises(VehicleDiscoveryError):
        await fetch_active_vehicle_ids("http://fleetpulse.test")


@pytest.mark.asyncio
async def test_vehicle_discovery_rejects_invalid_arguments() -> None:
    with pytest.raises(ValueError):
        await fetch_active_vehicle_ids("http://fleetpulse.test", page_size=0)

    with pytest.raises(ValueError):
        await fetch_active_vehicle_ids("http://fleetpulse.test", timeout_seconds=0)


def test_cli_parser_defaults() -> None:
    args = build_parser().parse_args([])

    assert args.vehicles == 5
    assert args.interval_seconds == 1.0
    assert args.duration_seconds == 60.0
    assert args.timeout_seconds == 10.0
    assert args.seed is None
    assert args.dry_run is False


def test_cli_default_ingestion_url_targets_locations_endpoint() -> None:
    args = build_parser().parse_args([])

    assert args.ingestion_url == "http://127.0.0.1:8000/api/v1/locations"


def _noop_configure_logging(level: str) -> None:
    return None


def test_cli_runs_a_short_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("simulator.cli.configure_logging", _noop_configure_logging)

    exit_code = main(
        [
            "--dry-run",
            "--vehicle-id",
            str(uuid4()),
            "--duration",
            "0.02",
            "--interval",
            "0.005",
            "--seed",
            "7",
        ]
    )

    assert exit_code == 0


def test_cli_rejects_invalid_vehicle_count(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("simulator.cli.configure_logging", _noop_configure_logging)

    exit_code = main(["--vehicles", "0", "--dry-run", "--vehicle-id", str(uuid4())])

    assert exit_code == 1
