"""Unit tests for the Kafka GPS publisher (no broker required).

``AIOKafkaProducer`` and ``AIOKafkaAdminClient`` are replaced with in-memory
fakes so the tests assert configuration, serialisation, keying, topic
administration and failure translation deterministically.
"""

import asyncio
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import ClassVar
from uuid import uuid4

import pytest
from aiokafka.errors import KafkaError, TopicAlreadyExistsError

from app.infrastructure.kafka import KafkaGpsEventPublisher
from app.schemas.gps import GpsEvent
from app.services.gps_publisher import GpsPublishError


def make_event(**overrides: object) -> GpsEvent:
    payload: dict[str, object] = {
        "event_id": uuid4(),
        "vehicle_id": uuid4(),
        "latitude": 52.52,
        "longitude": 13.405,
        "speed_kmph": 31.5,
        "heading": 87.5,
        "recorded_at": datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
    }
    payload.update(overrides)
    return GpsEvent.model_validate(payload)


class FakeProducer:
    """Record construction options and sends instead of talking to a broker."""

    instances: ClassVar[list["FakeProducer"]] = []

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.sent: list[dict[str, object]] = []
        self.start_error: BaseException | None = None
        self.send_error: BaseException | None = None
        self.hang_seconds = 0.0
        FakeProducer.instances.append(self)

    async def start(self) -> None:
        if self.start_error is not None:
            raise self.start_error
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send_and_wait(self, topic: str, *, value: object = None, key: object = None) -> None:
        if self.send_error is not None:
            raise self.send_error
        if self.hang_seconds > 0:
            await asyncio.sleep(self.hang_seconds)
        self.sent.append({"topic": topic, "value": value, "key": key})


class FakeAdmin:
    """Pretend to administer topics on an imaginary broker."""

    instances: ClassVar[list["FakeAdmin"]] = []
    existing_topics: ClassVar[set[str]] = set()
    create_error: ClassVar[BaseException | None] = None

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.started = False
        self.closed = False
        self.created: list[object] = []
        FakeAdmin.instances.append(self)

    async def start(self) -> None:
        self.started = True

    async def close(self) -> None:
        self.closed = True

    async def list_topics(self) -> list[str]:
        return sorted(FakeAdmin.existing_topics)

    async def create_topics(self, new_topics: list[object], **kwargs: object) -> None:
        del kwargs
        if FakeAdmin.create_error is not None:
            raise FakeAdmin.create_error
        self.created.extend(new_topics)


@pytest.fixture(autouse=True)
def fake_kafka(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    FakeProducer.instances.clear()
    FakeAdmin.instances.clear()
    FakeAdmin.existing_topics = set()
    FakeAdmin.create_error = None
    monkeypatch.setattr("app.infrastructure.kafka.AIOKafkaProducer", FakeProducer)
    monkeypatch.setattr("app.infrastructure.kafka.AIOKafkaAdminClient", FakeAdmin)
    return SimpleNamespace(producer=FakeProducer, admin=FakeAdmin)


def make_publisher(**overrides: object) -> KafkaGpsEventPublisher:
    options: dict[str, object] = {
        "bootstrap_servers": "localhost:9092",
        "topic": "gps.events",
        "partitions": 3,
        "replication_factor": 1,
        "delivery_timeout_seconds": 30.0,
    }
    options.update(overrides)
    return KafkaGpsEventPublisher(**options)  # type: ignore[arg-type]


def test_producer_requests_ack_all_and_idempotence(fake_kafka: SimpleNamespace) -> None:
    make_publisher()

    options = fake_kafka.producer.instances[0].kwargs
    assert options["acks"] == "all"
    assert options["enable_idempotence"] is True
    assert options["client_id"] == "fleetpulse-gps-producer"
    assert options["bootstrap_servers"] == "localhost:9092"
    # The delivery timeout bounds the wait for the broker acknowledgement.
    assert options["request_timeout_ms"] == 30_000


@pytest.mark.parametrize(
    ("overrides", "field"),
    [
        ({"bootstrap_servers": ""}, "bootstrap_servers"),
        ({"topic": ""}, "topic"),
        ({"partitions": 0}, "partitions"),
        ({"replication_factor": 0}, "replication_factor"),
        ({"delivery_timeout_seconds": 0}, "delivery_timeout_seconds"),
    ],
)
def test_constructor_rejects_invalid_configuration(
    overrides: dict[str, object],
    field: str,
) -> None:
    with pytest.raises(ValueError, match=field):
        make_publisher(**overrides)


@pytest.mark.asyncio
async def test_start_creates_topic_with_configured_shape(fake_kafka: SimpleNamespace) -> None:
    publisher = make_publisher(partitions=5, replication_factor=1)

    await publisher.start()

    admin = fake_kafka.admin.instances[0]
    assert admin.started and admin.closed
    assert len(admin.created) == 1
    topic = admin.created[0]
    assert topic.name == "gps.events"
    assert topic.num_partitions == 5
    assert topic.replication_factor == 1
    assert fake_kafka.producer.instances[0].started is True
    assert publisher.topic == "gps.events"


@pytest.mark.asyncio
async def test_start_skips_topic_creation_when_topic_exists(fake_kafka: SimpleNamespace) -> None:
    FakeAdmin.existing_topics.add("gps.events")

    publisher = make_publisher()
    await publisher.start()

    assert fake_kafka.admin.instances[0].created == []
    assert fake_kafka.producer.instances[0].started is True


@pytest.mark.asyncio
async def test_start_is_idempotent(fake_kafka: SimpleNamespace) -> None:
    publisher = make_publisher()

    await publisher.start()
    await publisher.start()

    assert len(fake_kafka.admin.instances) == 1
    assert len(fake_kafka.producer.instances) == 1


@pytest.mark.asyncio
async def test_topic_created_concurrently_by_the_worker_is_tolerated(
    fake_kafka: SimpleNamespace,
) -> None:
    publisher = make_publisher()
    FakeAdmin.create_error = TopicAlreadyExistsError()

    await publisher.start()

    assert fake_kafka.producer.instances[0].started is True


@pytest.mark.asyncio
async def test_start_failure_raises_publish_error(fake_kafka: SimpleNamespace) -> None:
    publisher = make_publisher()
    fake_kafka.producer.instances[0].start_error = KafkaError("broker down")

    with pytest.raises(GpsPublishError, match="failed to start"):
        await publisher.start()


@pytest.mark.asyncio
async def test_topic_creation_failure_raises_publish_error(fake_kafka: SimpleNamespace) -> None:
    publisher = make_publisher()
    FakeAdmin.create_error = KafkaError("controller unavailable")

    with pytest.raises(GpsPublishError, match="topic"):
        await publisher.start()
    assert fake_kafka.producer.instances[0].started is False


@pytest.mark.asyncio
async def test_publish_serialises_event_as_json(fake_kafka: SimpleNamespace) -> None:
    publisher = make_publisher()
    await publisher.start()
    event = make_event()

    await publisher.publish(event)

    sent = fake_kafka.producer.instances[0].sent
    assert len(sent) == 1
    assert sent[0]["topic"] == "gps.events"
    body = sent[0]["value"]
    assert isinstance(body, bytes)
    decoded = json.loads(body)
    assert decoded == event.model_dump(mode="json")


@pytest.mark.asyncio
async def test_publish_preserves_all_contract_fields(fake_kafka: SimpleNamespace) -> None:
    publisher = make_publisher()
    await publisher.start()
    shipment_id = uuid4()
    event = make_event(shipment_id=shipment_id)

    await publisher.publish(event)

    body = json.loads(fake_kafka.producer.instances[0].sent[0]["value"])
    assert body["event_id"] == str(event.event_id)
    assert body["vehicle_id"] == str(event.vehicle_id)
    assert body["shipment_id"] == str(shipment_id)
    assert body["latitude"] == 52.52
    assert body["longitude"] == 13.405
    assert body["speed_kmph"] == 31.5
    assert body["heading"] == 87.5
    assert body["recorded_at"] == "2026-01-01T12:00:00Z"
    assert body["schema_version"] == "1.0"


@pytest.mark.asyncio
async def test_publish_uses_vehicle_id_as_message_key(fake_kafka: SimpleNamespace) -> None:
    publisher = make_publisher()
    await publisher.start()
    event = make_event()

    await publisher.publish(event)

    sent = fake_kafka.producer.instances[0].sent[0]
    assert sent["key"] == str(event.vehicle_id).encode("utf-8")


@pytest.mark.asyncio
async def test_publish_before_start_is_rejected() -> None:
    publisher = make_publisher()

    with pytest.raises(GpsPublishError, match="not started"):
        await publisher.publish(make_event())


@pytest.mark.asyncio
async def test_delivery_failure_raises_publish_error_without_broker_details(
    fake_kafka: SimpleNamespace,
) -> None:
    publisher = make_publisher()
    await publisher.start()
    fake_kafka.producer.instances[0].send_error = KafkaError("NOT_LEADER_FOR_PARTITION")

    with pytest.raises(GpsPublishError) as exc_info:
        await publisher.publish(make_event())

    message = str(exc_info.value)
    assert "localhost:9092" not in message
    assert "NOT_LEADER" not in message


@pytest.mark.asyncio
async def test_delivery_timeout_caps_total_wait(fake_kafka: SimpleNamespace) -> None:
    """aiokafka retries retriable errors forever; the publisher must not."""
    publisher = make_publisher(delivery_timeout_seconds=0.05)
    await publisher.start()
    fake_kafka.producer.instances[0].hang_seconds = 5.0

    loop = asyncio.get_running_loop()
    started_at = loop.time()
    with pytest.raises(GpsPublishError, match="delivery failed"):
        await publisher.publish(make_event())
    assert loop.time() - started_at < 2.0


@pytest.mark.asyncio
async def test_serialisation_failure_raises_publish_error(fake_kafka: SimpleNamespace) -> None:
    publisher = make_publisher()
    await publisher.start()

    class UnserialisableEvent:
        event_id = uuid4()
        vehicle_id = uuid4()

        @staticmethod
        def model_dump(*, mode: str) -> dict[str, object]:
            del mode
            return {"recorded_at": object()}

    with pytest.raises(GpsPublishError, match="serialised"):
        await publisher.publish(UnserialisableEvent())  # type: ignore[arg-type]
    assert fake_kafka.producer.instances[0].sent == []


@pytest.mark.asyncio
async def test_close_stops_producer_and_is_safe_to_repeat(fake_kafka: SimpleNamespace) -> None:
    publisher = make_publisher()
    await publisher.start()

    await publisher.close()
    await publisher.close()

    assert fake_kafka.producer.instances[0].stopped is True
    with pytest.raises(GpsPublishError, match="not started"):
        await publisher.publish(make_event())
