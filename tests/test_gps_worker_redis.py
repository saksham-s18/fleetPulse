"""Unit tests for the standalone GPS worker's Redis/Kafka lifecycle (no broker)."""

import argparse
from collections.abc import Iterator
from typing import Any, ClassVar, cast

import pytest

from app.core.config import Settings, get_settings
from app.infrastructure.redis import RedisUnavailableError
from app.services.gps_consumer import GpsConsumerError
from app.services.live_location_handler import LiveLocationGpsEventHandler
from app.workers import gps_worker


class FakeStats:
    def summary(self) -> str:
        return "received=0"


class FakeService:
    def __init__(self) -> None:
        self.stats = FakeStats()


class FakeRedisClient:
    instances: ClassVar[list["FakeRedisClient"]] = []
    start_error: ClassVar[BaseException | None] = None

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = object()
        self.events: list[str] = []
        FakeRedisClient.instances.append(self)

    async def start(self) -> None:
        self.events.append("start")
        if FakeRedisClient.start_error is not None:
            raise FakeRedisClient.start_error

    async def close(self) -> None:
        self.events.append("close")


class FakeDeadLetters:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.events: list[str] = []

    async def start(self) -> None:
        self.events.append("start")

    async def close(self) -> None:
        self.events.append("close")


class FakeConsumer:
    instances: ClassVar[list["FakeConsumer"]] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.service = kwargs["service"]
        self.events: list[str] = []
        FakeConsumer.instances.append(self)

    async def start(self) -> None:
        self.events.append("start")

    async def run(self, stop_event: Any) -> None:
        del stop_event
        self.events.append("run")

    async def stop(self) -> None:
        self.events.append("stop")


@pytest.fixture(autouse=True)
def reset_fakes() -> Iterator[None]:
    FakeRedisClient.instances.clear()
    FakeRedisClient.start_error = None
    FakeConsumer.instances.clear()
    yield


@pytest.fixture
def wired(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gps_worker, "RedisClient", FakeRedisClient)
    monkeypatch.setattr(gps_worker, "KafkaDeadLetterPublisher", FakeDeadLetters)
    monkeypatch.setattr(gps_worker, "KafkaGpsConsumer", FakeConsumer)
    monkeypatch.setattr(gps_worker, "_install_shutdown_handlers", lambda _event: lambda: None)


def build_fakes() -> tuple[FakeConsumer, FakeDeadLetters, Any, FakeRedisClient]:
    """Build the worker and narrow the monkeypatched components for mypy."""
    consumer, dead_letters, service, redis_client = gps_worker.build_worker(get_settings())
    return (
        cast(FakeConsumer, consumer),
        cast(FakeDeadLetters, dead_letters),
        service,
        cast(FakeRedisClient, redis_client),
    )


def test_build_worker_creates_one_redis_client_and_wires_live_handler(
    wired: None,
) -> None:
    consumer, dead_letters, service, redis_client = build_fakes()

    assert len(FakeRedisClient.instances) == 1
    assert redis_client is FakeRedisClient.instances[0]
    assert isinstance(dead_letters, FakeDeadLetters)
    assert isinstance(consumer, FakeConsumer)
    assert consumer.service is service
    handler = cast(LiveLocationGpsEventHandler, service._handler)
    assert isinstance(handler, LiveLocationGpsEventHandler)
    # The repository shares the worker's single Redis connection.
    assert handler._service._repository._client is redis_client.client


def test_run_starts_redis_then_kafka_and_closes_everything(
    wired: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer, dead_letters, service, redis_client = build_fakes()
    monkeypatch.setattr(
        gps_worker,
        "build_worker",
        lambda _settings: (consumer, dead_letters, service, redis_client),
    )

    exit_code = _run_once()

    assert exit_code == 0
    assert redis_client.events == ["start", "close"]
    assert dead_letters.events == ["start", "close"]
    assert consumer.events == ["start", "run", "stop"]


def test_run_fails_cleanly_when_redis_cannot_start(
    wired: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer, dead_letters, service, redis_client = build_fakes()
    FakeRedisClient.start_error = RedisUnavailableError("redis unreachable")
    monkeypatch.setattr(
        gps_worker,
        "build_worker",
        lambda _settings: (consumer, dead_letters, service, redis_client),
    )

    exit_code = _run_once()

    assert exit_code == 1
    assert redis_client.events == ["start", "close"]
    # Kafka is never started when the required live-state store is missing;
    # the already-created DLQ publisher is still closed during cleanup.
    assert "start" not in dead_letters.events
    assert consumer.events == ["stop"]


def test_run_closes_resources_when_consumer_fails(
    wired: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer, dead_letters, service, redis_client = build_fakes()

    async def failing_run(_stop_event: Any) -> None:
        raise GpsConsumerError("dlq publish failed")

    monkeypatch.setattr(consumer, "run", failing_run)
    monkeypatch.setattr(
        gps_worker,
        "build_worker",
        lambda _settings: (consumer, dead_letters, service, redis_client),
    )

    exit_code = _run_once()

    assert exit_code == 1
    assert redis_client.events == ["start", "close"]
    assert dead_letters.events == ["start", "close"]
    assert consumer.events == ["start", "stop"]


@pytest.mark.parametrize(
    ("kafka_enabled", "redis_enabled", "message"),
    [
        (False, True, "KAFKA_ENABLED=false"),
        (True, False, "REDIS_ENABLED=false"),
    ],
)
def test_main_rejects_disabled_required_backends(
    monkeypatch: pytest.MonkeyPatch,
    kafka_enabled: bool,
    redis_enabled: bool,
    message: str,
) -> None:
    settings = get_settings().model_copy(
        update={"kafka_enabled": kafka_enabled, "redis_enabled": redis_enabled}
    )
    monkeypatch.setattr(gps_worker, "get_settings", lambda: settings)
    monkeypatch.setattr(gps_worker, "configure_logging", lambda _level: None)
    called = False

    def fail_if_called(*_args: Any) -> int:
        nonlocal called
        called = True
        return 0

    monkeypatch.setattr(gps_worker, "_run", fail_if_called)

    assert gps_worker.main([]) == 1
    assert called is False
    assert message  # documented reason for the rejection


def _run_once() -> int:
    args = argparse.Namespace(duration_seconds=0.0)
    return cast(int, __import__("asyncio").run(gps_worker._run(args, get_settings())))
