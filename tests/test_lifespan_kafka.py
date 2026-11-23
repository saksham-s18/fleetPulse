"""Unit tests for the producer lifecycle managed by the FastAPI lifespan."""

from collections.abc import Iterator
from typing import Any, ClassVar

import pytest
from fastapi.testclient import TestClient

from app.core.config import get_settings
from app.main import create_app
from app.services.gps_publisher import GpsPublishError, UnavailableGpsEventPublisher


class FakeKafkaPublisher:
    """Stand-in for KafkaGpsEventPublisher recording lifecycle calls."""

    instances: ClassVar[list["FakeKafkaPublisher"]] = []
    start_error: ClassVar[BaseException | None] = None

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.started = False
        self.closed = False
        FakeKafkaPublisher.instances.append(self)

    async def start(self) -> None:
        if FakeKafkaPublisher.start_error is not None:
            raise FakeKafkaPublisher.start_error
        self.started = True

    async def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def fake_kafka_publisher(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    FakeKafkaPublisher.instances.clear()
    FakeKafkaPublisher.start_error = None
    monkeypatch.setattr("app.main.KafkaGpsEventPublisher", FakeKafkaPublisher)
    yield


def test_kafka_disabled_installs_unavailable_publisher() -> None:
    settings = get_settings()
    assert settings.kafka_enabled is False, "the unit suite must not require a broker"

    app = create_app(settings)
    with TestClient(app):
        publisher = app.state.gps_publisher

    assert isinstance(publisher, UnavailableGpsEventPublisher)
    assert FakeKafkaPublisher.instances == []


def test_kafka_enabled_starts_producer_on_startup_and_closes_on_shutdown() -> None:
    settings = get_settings().model_copy(update={"kafka_enabled": True})

    app = create_app(settings)
    with TestClient(app):
        publisher = app.state.gps_publisher
        assert isinstance(publisher, FakeKafkaPublisher)
        assert publisher.started is True
        assert publisher.closed is False
        assert publisher.kwargs["bootstrap_servers"] == settings.kafka_bootstrap_servers
        assert publisher.kwargs["topic"] == settings.kafka_gps_topic
        assert publisher.kwargs["partitions"] == settings.kafka_gps_partitions
        assert publisher.kwargs["replication_factor"] == settings.kafka_gps_replication_factor

    # A single producer is shared for the application lifetime and closed
    # exactly once on shutdown.
    assert len(FakeKafkaPublisher.instances) == 1
    assert FakeKafkaPublisher.instances[0].closed is True


def test_startup_fails_loudly_when_required_producer_cannot_start() -> None:
    settings = get_settings().model_copy(update={"kafka_enabled": True})
    FakeKafkaPublisher.start_error = GpsPublishError("broker unreachable")

    with (
        pytest.raises(RuntimeError, match="Kafka is required"),
        TestClient(create_app(settings)),
    ):
        pass  # pragma: no cover - startup must fail before this point
