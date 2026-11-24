"""Real broker tests for the Milestone 5C.2 consumer worker (marked ``kafka``).

These tests need the Kafka broker from Docker Compose::

    docker compose up -d kafka
    pytest -m kafka

PostgreSQL is not touched by the consumer tests (the worker has no database
dependency). Every test uses unique topic names and a unique consumer group, so
tests never share offsets or messages.
"""

import asyncio
import contextlib
import json
from collections.abc import Callable
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.errors import KafkaError

from app.core.config import get_settings
from app.infrastructure.kafka import KafkaGpsEventPublisher
from app.infrastructure.kafka_consumer import (
    KafkaDeadLetterPublisher,
    KafkaGpsConsumer,
)
from app.schemas.gps import GpsEvent
from app.services.gps_consumer import (
    ConsumeResult,
    DeadLetterPublisher,
    DeadLetterPublishError,
    DeadLetterRecord,
    GpsConsumeOutcome,
    GpsConsumerError,
    GpsConsumerService,
    GpsMessage,
    InMemoryEventDeduplicator,
)

pytestmark = pytest.mark.kafka

PARTITIONS = 1


def unique_topic(prefix: str = "gps.events.test") -> str:
    """Return an isolated topic name so tests never share state."""
    return f"{prefix}.{uuid4().hex[:12]}"


def unique_group() -> str:
    """Return an isolated consumer group so offsets never collide."""
    return f"fleetpulse-gps-test-{uuid4().hex[:12]}"


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


class RecordingHandler:
    """Record handled events; optionally fail every attempt."""

    def __init__(self, *, fail_always: bool = False) -> None:
        self.fail_always = fail_always
        self.events: list[GpsEvent] = []
        self.calls = 0

    async def handle(self, event: GpsEvent) -> None:
        self.calls += 1
        if self.fail_always:
            raise RuntimeError("handler failure")
        self.events.append(event)


class FailingDeadLetterPublisher:
    """Reject every quarantine attempt to exercise the no-commit path."""

    async def publish(self, record: DeadLetterRecord) -> None:
        raise DeadLetterPublishError(f"cannot quarantine offset {record.offset}")


class RecordingService:
    """Wrap the real service and record the raw consumed messages."""

    def __init__(self, service: GpsConsumerService) -> None:
        self._service = service
        self.messages: list[GpsMessage] = []
        self.stats = service.stats

    async def process(self, message: GpsMessage) -> ConsumeResult:
        self.messages.append(message)
        return await self._service.process(message)


def build_worker(
    *,
    topic: str,
    dlq_topic: str,
    group_id: str,
    handler: RecordingHandler | None = None,
    dead_letters: DeadLetterPublisher | None = None,
    max_attempts: int = 3,
    recording: bool = False,
) -> tuple[
    KafkaGpsConsumer,
    KafkaDeadLetterPublisher | None,
    GpsConsumerService,
    GpsConsumerService | RecordingService,
]:
    settings = get_settings()
    dlq_publisher: KafkaDeadLetterPublisher | None = None
    publisher: DeadLetterPublisher
    if dead_letters is None:
        dlq_publisher = KafkaDeadLetterPublisher(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=dlq_topic,
            partitions=PARTITIONS,
            replication_factor=1,
            delivery_timeout_seconds=15.0,
        )
        publisher = dlq_publisher
    else:
        publisher = dead_letters

    service = GpsConsumerService(
        handler=handler or RecordingHandler(),
        dead_letter_publisher=publisher,
        deduplicator=InMemoryEventDeduplicator(),
        max_attempts=max_attempts,
        retry_delay_seconds=0.01,
    )
    service_view = RecordingService(service) if recording else service
    consumer = KafkaGpsConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=topic,
        group_id=group_id,
        partitions=PARTITIONS,
        replication_factor=1,
        poll_timeout_seconds=0.2,
        service=service_view,  # type: ignore[arg-type]
    )
    return consumer, dlq_publisher, service, service_view


async def publish_events(topic: str, events: list[GpsEvent]) -> None:
    settings = get_settings()
    publisher = KafkaGpsEventPublisher(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=topic,
        partitions=PARTITIONS,
        replication_factor=1,
        delivery_timeout_seconds=15.0,
    )
    try:
        await publisher.start()
        for event in events:
            await publisher.publish(event)
    finally:
        await publisher.close()


async def publish_raw(topic: str, value: bytes, key: bytes) -> None:
    settings = get_settings()
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    try:
        await producer.send_and_wait(topic, value=value, key=key)
    finally:
        await producer.stop()


async def consume_one(topic: str, *, timeout_seconds: float = 20.0) -> object:
    """Return the first record on an isolated test topic."""
    settings = get_settings()
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
    )
    await consumer.start()
    try:
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while asyncio.get_running_loop().time() < deadline:
            batches = await consumer.getmany(timeout_ms=500)
            for records in batches.values():
                if records:
                    return records[0]
    finally:
        await consumer.stop()
    raise AssertionError(f"no message was consumed from {topic}")


async def committed_offsets(group_id: str, topic: str) -> dict[int, int]:
    """Read committed offsets for the test topic (empty when none exist)."""
    settings = get_settings()
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    await admin.start()
    try:
        result = await admin.list_consumer_group_offsets(
            group_id,
            partitions=[TopicPartition(topic, partition) for partition in range(PARTITIONS)],
        )
    except KafkaError:
        return {}
    finally:
        await admin.close()
    return {partition.partition: offset.offset for partition, offset in result.items()}


async def delete_topic(topic: str) -> None:
    """Best-effort cleanup of a per-test topic."""
    settings = get_settings()
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    try:
        await admin.start()
        with contextlib.suppress(KafkaError):
            await admin.delete_topics([topic])
    finally:
        await admin.close()


async def wait_until(predicate: Callable[[], bool], *, timeout_seconds: float = 30.0) -> None:
    """Poll a zero-argument predicate until it is true or fail the test."""
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition was not met before the timeout")
        await asyncio.sleep(0.05)


async def stop_worker(
    consumer: KafkaGpsConsumer,
    dlq_publisher: KafkaDeadLetterPublisher | None,
    task: asyncio.Task[None],
    stop_event: asyncio.Event,
) -> None:
    stop_event.set()
    await asyncio.wait_for(task, timeout=30.0)
    await consumer.stop()
    if dlq_publisher is not None:
        await dlq_publisher.close()


@pytest.mark.asyncio
async def test_consumer_receives_producer_event_and_preserves_key_and_contents() -> None:
    topic = unique_topic()
    dlq_topic = unique_topic("gps.events.dlq.test")
    group_id = unique_group()
    event = make_event()
    handler = RecordingHandler()
    consumer, dlq_publisher, service, view = build_worker(
        topic=topic,
        dlq_topic=dlq_topic,
        group_id=group_id,
        handler=handler,
        recording=True,
    )
    assert isinstance(view, RecordingService)

    try:
        await publish_events(topic, [event])
        await consumer.start()
        assert dlq_publisher is not None
        await dlq_publisher.start()
        stop_event = asyncio.Event()
        task = asyncio.create_task(consumer.run(stop_event))

        await wait_until(lambda: service.stats.processed == 1)
        await stop_worker(consumer, dlq_publisher, task, stop_event)

        assert len(handler.events) == 1
        assert handler.events[0].model_dump(mode="json") == event.model_dump(mode="json")
        assert view.messages[0].key == str(event.vehicle_id).encode("utf-8")
        assert view.messages[0].value is not None
        assert json.loads(view.messages[0].value) == event.model_dump(mode="json")

        offsets = await committed_offsets(group_id, topic)
        assert offsets == {view.messages[0].partition: view.messages[0].offset + 1}
    finally:
        await consumer.stop()
        if dlq_publisher is not None:
            await dlq_publisher.close()
        await delete_topic(topic)
        await delete_topic(dlq_topic)


@pytest.mark.asyncio
async def test_duplicate_event_is_processed_once_but_both_offsets_advance() -> None:
    topic = unique_topic()
    dlq_topic = unique_topic("gps.events.dlq.test")
    group_id = unique_group()
    event = make_event()
    handler = RecordingHandler()
    consumer, dlq_publisher, service, _ = build_worker(
        topic=topic,
        dlq_topic=dlq_topic,
        group_id=group_id,
        handler=handler,
    )

    try:
        await publish_events(topic, [event, event])
        await consumer.start()
        assert dlq_publisher is not None
        await dlq_publisher.start()
        stop_event = asyncio.Event()
        task = asyncio.create_task(consumer.run(stop_event))

        await wait_until(lambda: service.stats.duplicates == 1)
        await stop_worker(consumer, dlq_publisher, task, stop_event)

        assert handler.calls == 1
        assert service.stats.processed == 1
        assert service.stats.duplicates == 1
        assert await committed_offsets(group_id, topic) == {0: 2}
    finally:
        await consumer.stop()
        if dlq_publisher is not None:
            await dlq_publisher.close()
        await delete_topic(topic)
        await delete_topic(dlq_topic)


@pytest.mark.asyncio
async def test_malformed_message_is_quarantined_before_offset_commit() -> None:
    topic = unique_topic()
    dlq_topic = unique_topic("gps.events.dlq.test")
    group_id = unique_group()
    key = b"vehicle-with-broken-json"
    raw = b"{this is not json"
    handler = RecordingHandler()
    consumer, dlq_publisher, service, _ = build_worker(
        topic=topic,
        dlq_topic=dlq_topic,
        group_id=group_id,
        handler=handler,
    )

    try:
        await consumer.start()
        assert dlq_publisher is not None
        await dlq_publisher.start()
        await publish_raw(topic, raw, key)
        stop_event = asyncio.Event()
        task = asyncio.create_task(consumer.run(stop_event))

        await wait_until(lambda: service.stats.quarantined == 1)
        await stop_worker(consumer, dlq_publisher, task, stop_event)

        assert handler.calls == 0
        assert service.stats.invalid == 1

        record = await consume_one(dlq_topic)
        assert record.key == key  # type: ignore[attr-defined]
        envelope = json.loads(record.value)  # type: ignore[attr-defined]
        assert envelope["original_topic"] == topic
        assert envelope["original_partition"] == 0
        assert envelope["original_offset"] == 0
        assert isinstance(envelope["original_timestamp_ms"], int)
        assert envelope["original_key"] == key.decode("utf-8")
        assert envelope["payload"] == raw.decode("utf-8")
        assert envelope["payload_encoding"] == "utf-8"
        assert envelope["error"]["type"] == "invalid_json"

        # The original offset advanced only after the DLQ acknowledgement.
        assert await committed_offsets(group_id, topic) == {0: 1}
    finally:
        await consumer.stop()
        if dlq_publisher is not None:
            await dlq_publisher.close()
        await delete_topic(topic)
        await delete_topic(dlq_topic)


@pytest.mark.asyncio
async def test_dead_letter_failure_leaves_original_offset_uncommitted() -> None:
    topic = unique_topic()
    dlq_topic = unique_topic("gps.events.dlq.test")
    group_id = unique_group()
    consumer, _, _, _ = build_worker(
        topic=topic,
        dlq_topic=dlq_topic,
        group_id=group_id,
        handler=RecordingHandler(),
        dead_letters=FailingDeadLetterPublisher(),
        max_attempts=1,
    )
    stop_event = asyncio.Event()

    try:
        await consumer.start()
        await publish_raw(topic, b"{broken", b"vehicle-key")
        task = asyncio.create_task(consumer.run(stop_event))

        with pytest.raises(GpsConsumerError, match="dead-lettered"):
            await asyncio.wait_for(task, timeout=30.0)
        await consumer.stop()

        offsets = await committed_offsets(group_id, topic)
        assert offsets.get(0, -1) == -1
    finally:
        await consumer.stop()
        await delete_topic(topic)
        await delete_topic(dlq_topic)


@pytest.mark.asyncio
async def test_handler_retries_then_dead_letters_after_max_attempts() -> None:
    topic = unique_topic()
    dlq_topic = unique_topic("gps.events.dlq.test")
    group_id = unique_group()
    event = make_event()
    handler = RecordingHandler(fail_always=True)
    consumer, dlq_publisher, service, _ = build_worker(
        topic=topic,
        dlq_topic=dlq_topic,
        group_id=group_id,
        handler=handler,
        max_attempts=2,
    )

    try:
        await publish_events(topic, [event])
        await consumer.start()
        assert dlq_publisher is not None
        await dlq_publisher.start()
        stop_event = asyncio.Event()
        task = asyncio.create_task(consumer.run(stop_event))

        await wait_until(lambda: service.stats.quarantined == 1)
        await stop_worker(consumer, dlq_publisher, task, stop_event)

        assert handler.calls == 2
        assert service.stats.handler_failures == 2
        record = await consume_one(dlq_topic)
        envelope = json.loads(record.value)  # type: ignore[attr-defined]
        assert envelope["event_id"] == str(event.event_id)
        assert envelope["error"]["type"] == "handler_error"
        assert await committed_offsets(group_id, topic) == {0: 1}
    finally:
        await consumer.stop()
        if dlq_publisher is not None:
            await dlq_publisher.close()
        await delete_topic(topic)
        await delete_topic(dlq_topic)


def test_outcome_enum_values_are_stable() -> None:
    """Keep the documented outcome names stable for logs and dashboards."""
    assert GpsConsumeOutcome.PROCESSED.value == "processed"
    assert GpsConsumeOutcome.DUPLICATE.value == "duplicate"
    assert GpsConsumeOutcome.QUARANTINED.value == "quarantined"
