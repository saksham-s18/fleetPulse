"""Unit tests for the Kafka consumer and DLQ transport (no broker required).

``AIOKafkaConsumer``, ``AIOKafkaProducer`` and ``AIOKafkaAdminClient`` are
replaced with in-memory fakes, so configuration, offset commits, shutdown and
dead-letter envelopes are asserted deterministically.
"""

import asyncio
import base64
import json
from collections.abc import Callable
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, ClassVar
from uuid import uuid4

import pytest
from aiokafka import OffsetAndMetadata, TopicPartition
from aiokafka.errors import CommitFailedError, KafkaError

from app.infrastructure.kafka_consumer import (
    KafkaDeadLetterPublisher,
    KafkaGpsConsumer,
)
from app.services.gps_consumer import (
    ConsumeResult,
    DeadLetterPublishError,
    DeadLetterRecord,
    GpsConsumeOutcome,
    GpsConsumerError,
    GpsMessage,
)


@dataclass
class FakeRecord:
    topic: str
    partition: int
    offset: int
    timestamp: int
    key: bytes | None
    value: bytes | None


class FakeConsumer:
    """Replay queued batches and record commits without a broker."""

    instances: ClassVar[list["FakeConsumer"]] = []

    def __init__(self, *topics: str, **kwargs: object) -> None:
        self.topics = topics
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.batches: list[dict[TopicPartition, list[FakeRecord]]] = []
        self.commit_calls: list[dict[TopicPartition, OffsetAndMetadata]] = []
        self.commit_error: BaseException | None = None
        self.getmany_error: BaseException | None = None
        self.assignment_result: list[TopicPartition] = []
        self.on_getmany: Callable[[], None] | None = None
        FakeConsumer.instances.append(self)

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def getmany(
        self, *, timeout_ms: int, max_records: int
    ) -> dict[TopicPartition, list[FakeRecord]]:
        del timeout_ms, max_records
        if self.getmany_error is not None:
            raise self.getmany_error
        callback = self.on_getmany
        self.on_getmany = None
        if callback is not None:
            callback()
        if self.batches:
            return self.batches.pop(0)
        return {}

    async def commit(self, offsets: dict[TopicPartition, OffsetAndMetadata]) -> None:
        if self.commit_error is not None:
            raise self.commit_error
        self.commit_calls.append(dict(offsets))

    def assignment(self) -> list[TopicPartition]:
        return self.assignment_result


class FakeAdmin:
    """Pretend to administer topics on an imaginary broker."""

    instances: ClassVar[list["FakeAdmin"]] = []
    existing_topics: ClassVar[set[str]] = set()

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.created: list[Any] = []
        FakeAdmin.instances.append(self)

    async def start(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def list_topics(self) -> list[str]:
        return sorted(FakeAdmin.existing_topics)

    async def create_topics(self, new_topics: list[object], **kwargs: object) -> None:
        del kwargs
        self.created.extend(new_topics)


class FakeProducer:
    """Record DLQ sends; optionally fail or hang."""

    instances: ClassVar[list["FakeProducer"]] = []

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.sent: list[dict[str, object]] = []
        self.send_error: BaseException | None = None
        self.hang_seconds = 0.0
        FakeProducer.instances.append(self)

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send_and_wait(self, topic: str, *, value: object = None, key: object = None) -> None:
        if self.send_error is not None:
            raise self.send_error
        if self.hang_seconds > 0:
            await asyncio.sleep(self.hang_seconds)
        self.sent.append({"topic": topic, "value": value, "key": key})


class FakeService:
    """Stand-in for GpsConsumerService with a fixed outcome or error."""

    def __init__(
        self,
        *,
        outcome: GpsConsumeOutcome = GpsConsumeOutcome.PROCESSED,
        error: BaseException | None = None,
    ) -> None:
        self.outcome = outcome
        self.error = error
        self.messages: list[GpsMessage] = []

    async def process(self, message: GpsMessage) -> ConsumeResult:
        self.messages.append(message)
        if self.error is not None:
            raise self.error
        return ConsumeResult(self.outcome)


@pytest.fixture(autouse=True)
def fake_kafka(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    FakeConsumer.instances.clear()
    FakeAdmin.instances.clear()
    FakeAdmin.existing_topics = set()
    FakeProducer.instances.clear()
    monkeypatch.setattr("app.infrastructure.kafka_consumer.AIOKafkaConsumer", FakeConsumer)
    monkeypatch.setattr("app.infrastructure.kafka_consumer.AIOKafkaProducer", FakeProducer)
    monkeypatch.setattr("app.infrastructure.kafka.AIOKafkaAdminClient", FakeAdmin)
    return SimpleNamespace(consumer=FakeConsumer, admin=FakeAdmin, producer=FakeProducer)


def make_consumer(
    *,
    service: FakeService | None = None,
    **overrides: Any,
) -> KafkaGpsConsumer:
    options: dict[str, object] = {
        "bootstrap_servers": "localhost:9092",
        "topic": "gps.events",
        "group_id": "fleetpulse-gps-processors",
        "partitions": 3,
        "replication_factor": 1,
        "poll_timeout_seconds": 1.0,
        "service": service or FakeService(),
    }
    options.update(overrides)
    return KafkaGpsConsumer(**options)  # type: ignore[arg-type]


def make_record(
    *,
    topic: str = "gps.events",
    partition: int = 1,
    offset: int = 5,
    timestamp: int = 1_767_268_800_000,
    key: bytes | None = b"vehicle-key",
    value: bytes | None = b"{}",
) -> FakeRecord:
    return FakeRecord(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=timestamp,
        key=key,
        value=value,
    )


def make_dlq(**overrides: object) -> KafkaDeadLetterPublisher:
    options: dict[str, object] = {
        "bootstrap_servers": "localhost:9092",
        "topic": "gps.events.dlq",
        "partitions": 3,
        "replication_factor": 1,
        "delivery_timeout_seconds": 30.0,
    }
    options.update(overrides)
    return KafkaDeadLetterPublisher(**options)  # type: ignore[arg-type]


def make_dead_letter(**overrides: object) -> DeadLetterRecord:
    options: dict[str, object] = {
        "topic": "gps.events",
        "partition": 1,
        "offset": 5,
        "timestamp_ms": 1_767_268_800_000,
        "key": b"vehicle-key",
        "payload": b'{"event_id":"broken"}',
        "error_type": "invalid_json",
        "error_message": "JSONDecodeError: Expecting value",
        "event_id": None,
    }
    options.update(overrides)
    return DeadLetterRecord(**options)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Consumer configuration and lifecycle
# ---------------------------------------------------------------------------


def test_consumer_uses_manual_offsets_and_configured_group() -> None:
    consumer = make_consumer()

    fake = FakeConsumer.instances[0]
    assert fake.topics == ("gps.events",)
    assert fake.kwargs["group_id"] == "fleetpulse-gps-processors"
    assert fake.kwargs["enable_auto_commit"] is False
    assert fake.kwargs["auto_offset_reset"] == "earliest"
    assert fake.kwargs["max_poll_records"] == 1
    assert fake.kwargs["client_id"] == "fleetpulse-gps-consumer"
    assert consumer.topic == "gps.events"
    assert consumer.group_id == "fleetpulse-gps-processors"


@pytest.mark.parametrize(
    ("overrides", "field"),
    [
        ({"bootstrap_servers": ""}, "bootstrap_servers"),
        ({"topic": ""}, "topic"),
        ({"group_id": ""}, "group_id"),
        ({"partitions": 0}, "partitions"),
        ({"replication_factor": 0}, "replication_factor"),
        ({"poll_timeout_seconds": 0}, "poll_timeout_seconds"),
    ],
)
def test_consumer_rejects_invalid_configuration(overrides: dict[str, Any], field: str) -> None:
    with pytest.raises(ValueError, match=field):
        make_consumer(**overrides)


@pytest.mark.asyncio
async def test_consumer_start_creates_topic_and_joins_group() -> None:
    consumer = make_consumer(partitions=4)

    await consumer.start()

    admin = FakeAdmin.instances[0]
    assert len(admin.created) == 1
    assert admin.created[0].name == "gps.events"
    assert admin.created[0].num_partitions == 4
    assert FakeConsumer.instances[0].started is True


@pytest.mark.asyncio
async def test_consumer_start_skips_existing_topic_and_is_idempotent() -> None:
    FakeAdmin.existing_topics.add("gps.events")
    consumer = make_consumer()

    await consumer.start()
    await consumer.start()

    assert FakeAdmin.instances[0].created == []
    assert len(FakeConsumer.instances) == 1


@pytest.mark.asyncio
async def test_run_before_start_is_rejected() -> None:
    consumer = make_consumer()

    with pytest.raises(GpsConsumerError, match="not started"):
        await consumer.run(asyncio.Event())


@pytest.mark.asyncio
async def test_stop_is_safe_to_call_more_than_once() -> None:
    consumer = make_consumer()
    await consumer.start()

    await consumer.stop()
    await consumer.stop()

    assert FakeConsumer.instances[0].stopped is True


@pytest.mark.asyncio
async def test_stop_before_start_does_not_touch_the_client() -> None:
    consumer = make_consumer()

    await consumer.stop()

    assert FakeConsumer.instances[0].stopped is False


# ---------------------------------------------------------------------------
# Offset commits
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_successful_processing_commits_next_offset() -> None:
    service = FakeService()
    consumer = make_consumer(service=service)
    await consumer.start()
    stop_event = asyncio.Event()
    partition = TopicPartition("gps.events", 1)
    FakeConsumer.instances[0].batches.append({partition: [make_record()]})
    FakeConsumer.instances[0].on_getmany = stop_event.set

    await consumer.run(stop_event)

    message = service.messages[0]
    assert (message.topic, message.partition, message.offset) == ("gps.events", 1, 5)
    assert message.timestamp_ms == 1_767_268_800_000
    assert message.key == b"vehicle-key"
    assert message.value == b"{}"
    commits = FakeConsumer.instances[0].commit_calls
    assert len(commits) == 1
    assert commits[0] == {partition: OffsetAndMetadata(6, "")}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    [GpsConsumeOutcome.DUPLICATE, GpsConsumeOutcome.QUARANTINED],
)
async def test_safe_outcomes_commit_offset(outcome: GpsConsumeOutcome) -> None:
    service = FakeService(outcome=outcome)
    consumer = make_consumer(service=service)
    await consumer.start()
    stop_event = asyncio.Event()
    partition = TopicPartition("gps.events", 1)
    FakeConsumer.instances[0].batches.append({partition: [make_record()]})
    FakeConsumer.instances[0].on_getmany = stop_event.set

    await consumer.run(stop_event)

    assert FakeConsumer.instances[0].commit_calls == [{partition: OffsetAndMetadata(6, "")}]


@pytest.mark.asyncio
async def test_processing_failure_does_not_commit() -> None:
    service = FakeService(error=GpsConsumerError("could not be dead-lettered"))
    consumer = make_consumer(service=service)
    await consumer.start()
    stop_event = asyncio.Event()
    partition = TopicPartition("gps.events", 1)
    FakeConsumer.instances[0].batches.append({partition: [make_record()]})
    FakeConsumer.instances[0].on_getmany = stop_event.set

    with pytest.raises(GpsConsumerError, match="dead-lettered"):
        await consumer.run(stop_event)

    assert FakeConsumer.instances[0].commit_calls == []


@pytest.mark.asyncio
async def test_rebalance_commit_rejection_is_not_fatal() -> None:
    consumer = make_consumer()
    await consumer.start()
    stop_event = asyncio.Event()
    fake = FakeConsumer.instances[0]
    fake.batches.append({TopicPartition("gps.events", 1): [make_record()]})
    fake.on_getmany = stop_event.set
    fake.commit_error = CommitFailedError()

    await consumer.run(stop_event)

    assert fake.commit_calls == []


@pytest.mark.asyncio
async def test_other_commit_error_stops_the_worker() -> None:
    consumer = make_consumer()
    await consumer.start()
    stop_event = asyncio.Event()
    fake = FakeConsumer.instances[0]
    fake.batches.append({TopicPartition("gps.events", 1): [make_record()]})
    fake.on_getmany = stop_event.set
    fake.commit_error = KafkaError("broker gone")

    with pytest.raises(GpsConsumerError, match="commit failed"):
        await consumer.run(stop_event)


@pytest.mark.asyncio
async def test_fetch_error_stops_the_worker() -> None:
    consumer = make_consumer()
    await consumer.start()
    stop_event = asyncio.Event()
    FakeConsumer.instances[0].getmany_error = KafkaError("broker gone")

    with pytest.raises(GpsConsumerError, match="fetch failed"):
        await consumer.run(stop_event)


@pytest.mark.asyncio
async def test_shutdown_finishes_in_flight_record_then_stops_fetching() -> None:
    consumer = make_consumer()
    await consumer.start()
    stop_event = asyncio.Event()
    partition = TopicPartition("gps.events", 1)
    fake = FakeConsumer.instances[0]
    fake.batches.append({partition: [make_record(offset=9)]})
    # The signal arrives while the batch is being fetched: the record is still
    # finished and committed, and no further fetch happens.
    fake.on_getmany = stop_event.set

    await consumer.run(stop_event)

    assert fake.commit_calls == [{partition: OffsetAndMetadata(10, "")}]
    assert fake.batches == []


@pytest.mark.asyncio
async def test_assignment_change_is_logged() -> None:
    consumer = make_consumer()
    await consumer.start()
    fake = FakeConsumer.instances[0]
    fake.assignment_result = [TopicPartition("gps.events", 0)]

    await consumer._log_assignment()  # deterministic unit check

    assert consumer._assignment == frozenset({TopicPartition("gps.events", 0)})


# ---------------------------------------------------------------------------
# Dead-letter publisher
# ---------------------------------------------------------------------------


def test_dlq_producer_uses_ack_all_and_idempotence() -> None:
    make_dlq()

    options = FakeProducer.instances[0].kwargs
    assert options["acks"] == "all"
    assert options["enable_idempotence"] is True
    assert options["client_id"] == "fleetpulse-gps-dlq-producer"
    assert options["request_timeout_ms"] == 30_000


@pytest.mark.asyncio
async def test_dlq_start_creates_topic_and_starts_producer() -> None:
    publisher = make_dlq(partitions=4)

    await publisher.start()

    admin = FakeAdmin.instances[0]
    assert len(admin.created) == 1
    assert admin.created[0].name == "gps.events.dlq"
    assert admin.created[0].num_partitions == 4
    assert FakeProducer.instances[0].started is True
    assert publisher.topic == "gps.events.dlq"


@pytest.mark.asyncio
async def test_dlq_publish_preserves_key_and_envelope_metadata() -> None:
    publisher = make_dlq()
    await publisher.start()
    event_id = uuid4()

    await publisher.publish(make_dead_letter(event_id=event_id))

    sent = FakeProducer.instances[0].sent[0]
    assert sent["topic"] == "gps.events.dlq"
    assert sent["key"] == b"vehicle-key"
    raw_envelope = sent["value"]
    assert isinstance(raw_envelope, bytes)
    envelope = json.loads(raw_envelope)
    assert envelope["original_topic"] == "gps.events"
    assert envelope["original_partition"] == 1
    assert envelope["original_offset"] == 5
    assert envelope["original_timestamp_ms"] == 1_767_268_800_000
    assert envelope["original_key"] == "vehicle-key"
    assert envelope["original_key_encoding"] == "utf-8"
    assert envelope["event_id"] == str(event_id)
    assert envelope["payload"] == '{"event_id":"broken"}'
    assert envelope["payload_encoding"] == "utf-8"
    assert envelope["error"] == {
        "type": "invalid_json",
        "message": "JSONDecodeError: Expecting value",
    }
    assert envelope["quarantined_at"]


@pytest.mark.asyncio
async def test_dlq_publish_base64_encodes_non_utf8_payload() -> None:
    publisher = make_dlq()
    await publisher.start()
    raw = b"\xff\xfe\x00"
    record = make_dead_letter(payload=raw, key=None)

    await publisher.publish(record)

    raw_envelope = FakeProducer.instances[0].sent[0]["value"]
    assert isinstance(raw_envelope, bytes)
    envelope = json.loads(raw_envelope)
    assert envelope["payload"] == base64.b64encode(raw).decode("ascii")
    assert envelope["payload_encoding"] == "base64"
    assert envelope["original_key"] is None
    assert envelope["original_key_encoding"] is None


@pytest.mark.asyncio
async def test_dlq_publish_failure_raises_safe_error() -> None:
    publisher = make_dlq()
    await publisher.start()
    FakeProducer.instances[0].send_error = KafkaError("NOT_LEADER_FOR_PARTITION")

    with pytest.raises(DeadLetterPublishError) as exc_info:
        await publisher.publish(make_dead_letter())

    message = str(exc_info.value)
    assert "localhost:9092" not in message
    assert "NOT_LEADER" not in message


@pytest.mark.asyncio
async def test_dlq_publish_timeout_is_capped() -> None:
    publisher = make_dlq(delivery_timeout_seconds=0.05)
    await publisher.start()
    FakeProducer.instances[0].hang_seconds = 5.0

    loop = asyncio.get_running_loop()
    started_at = loop.time()
    with pytest.raises(DeadLetterPublishError):
        await publisher.publish(make_dead_letter())
    assert loop.time() - started_at < 2.0


@pytest.mark.asyncio
async def test_dlq_publish_before_start_is_rejected() -> None:
    publisher = make_dlq()

    with pytest.raises(DeadLetterPublishError, match="not started"):
        await publisher.publish(make_dead_letter())


@pytest.mark.asyncio
async def test_dlq_close_stops_producer_and_is_safe_to_repeat() -> None:
    publisher = make_dlq()
    await publisher.start()

    await publisher.close()
    await publisher.close()

    assert FakeProducer.instances[0].stopped is True
    with pytest.raises(DeadLetterPublishError, match="not started"):
        await publisher.publish(make_dead_letter())


@pytest.mark.asyncio
async def test_dlq_close_before_start_does_not_touch_the_producer() -> None:
    publisher = make_dlq()

    await publisher.close()

    assert FakeProducer.instances[0].stopped is False
