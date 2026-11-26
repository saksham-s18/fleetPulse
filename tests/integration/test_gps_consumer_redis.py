"""End-to-end Kafka consumer → Redis projection tests.

These tests need the Compose Kafka broker **and** Redis service::

    docker compose up -d kafka redis
    pytest -m kafka_redis

Every test uses an isolated GPS topic, DLQ topic, consumer group and Redis key
namespace. Topics and Redis keys are deleted afterwards; unique consumer groups
are never reused, so their offsets cannot leak into another test run.
"""

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import uuid4

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.errors import KafkaError
from redis.asyncio import Redis

from app.core.config import Settings, get_settings
from app.infrastructure.kafka import KafkaGpsEventPublisher
from app.infrastructure.kafka_consumer import (
    KafkaDeadLetterPublisher,
    KafkaGpsConsumer,
)
from app.infrastructure.redis import create_redis_client, ensure_redis_ready
from app.repositories.live_location import LiveLocationRepository
from app.schemas.gps import GpsEvent
from app.services.gps_consumer import (
    GpsConsumerService,
    InMemoryEventDeduplicator,
)
from app.services.live_location import LiveLocationService
from app.services.live_location_handler import LiveLocationGpsEventHandler
from app.workers import gps_worker

pytestmark = pytest.mark.kafka_redis

PARTITIONS = 1
RECORDED_AT = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def unique(prefix: str) -> str:
    """Return an isolated topic, group or key-prefix name."""
    return f"{prefix}.{uuid4().hex[:12]}"


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


def worker_settings(
    *,
    topic: str,
    dlq_topic: str,
    group_id: str,
    prefix: str,
    max_attempts: int = 3,
    retry_delay_seconds: float = 0.01,
) -> Settings:
    """Build isolated worker settings without mutating the cached defaults."""
    return get_settings().model_copy(
        update={
            "kafka_gps_topic": topic,
            "kafka_gps_dlq_topic": dlq_topic,
            "kafka_consumer_group_id": group_id,
            "kafka_gps_partitions": PARTITIONS,
            "kafka_gps_replication_factor": 1,
            "kafka_consumer_poll_timeout_seconds": 0.2,
            "kafka_max_processing_attempts": max_attempts,
            "kafka_retry_delay_seconds": retry_delay_seconds,
            "redis_key_prefix": prefix,
        }
    )


async def publish_events(topic: str, events: list[GpsEvent]) -> None:
    """Publish through the existing GPS producer, waiting for acknowledgement."""
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


async def wait_until(predicate: Callable[[], bool], *, timeout_seconds: float = 30.0) -> None:
    """Poll a zero-argument predicate until it is true or fail the test."""
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition was not met before the timeout")
        await asyncio.sleep(0.05)


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
    # Kafka reports -1 for a partition with no committed offset yet.
    return {
        partition.partition: offset.offset
        for partition, offset in result.items()
        if offset.offset >= 0
    }


async def consume_one(topic: str, *, timeout_seconds: float = 20.0) -> Any:
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


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[Redis]:
    """Real Redis client; fails (does not skip) when Redis is unavailable."""
    client = create_redis_client(get_settings())
    await ensure_redis_ready(client)
    try:
        yield client
    finally:
        await client.aclose()


@pytest_asyncio.fixture
async def namespace(redis_client: Redis) -> AsyncIterator[str]:
    """Isolated key namespace, cleaned up even when a test fails."""
    prefix = unique("fleetpulse-test")
    try:
        yield prefix
    finally:
        keys = [key async for key in redis_client.scan_iter(match=f"{prefix}:*")]
        if keys:
            await redis_client.delete(*keys)


async def stop_worker(
    consumer: KafkaGpsConsumer,
    dead_letters: KafkaDeadLetterPublisher,
    task: asyncio.Task[None],
    stop_event: asyncio.Event,
) -> None:
    """Request shutdown and close every Kafka resource."""
    stop_event.set()
    await asyncio.wait_for(task, timeout=30.0)
    await consumer.stop()
    await dead_letters.close()


@pytest.mark.asyncio
async def test_worker_projects_newest_event_to_hash_and_geo(
    redis_client: Redis,
    namespace: str,
) -> None:
    topic = unique("gps.events.test")
    dlq_topic = unique("gps.events.dlq.test")
    group_id = unique("fleetpulse-gps-test")
    settings = worker_settings(
        topic=topic,
        dlq_topic=dlq_topic,
        group_id=group_id,
        prefix=namespace,
    )
    vehicle_id = uuid4()
    first = make_event(
        vehicle_id=vehicle_id,
        recorded_at=RECORDED_AT + timedelta(seconds=10),
        latitude=52.6,
        longitude=13.5,
    )
    newer = make_event(
        vehicle_id=vehicle_id,
        recorded_at=RECORDED_AT + timedelta(seconds=20),
        latitude=52.7,
        longitude=13.6,
    )
    older = make_event(
        vehicle_id=vehicle_id,
        recorded_at=RECORDED_AT + timedelta(seconds=5),
        latitude=52.4,
        longitude=13.4,
    )
    consumer, dead_letters, service, worker_redis = gps_worker.build_worker(settings)
    stop_event = asyncio.Event()
    task: asyncio.Task[None] | None = None
    repository = LiveLocationRepository(redis_client, prefix=namespace)

    try:
        await publish_events(topic, [first])
        await worker_redis.start()
        await dead_letters.start()
        await consumer.start()
        task = asyncio.create_task(consumer.run(stop_event))

        await wait_until(lambda: service.stats.applied == 1)
        record = await repository.latest(vehicle_id)
        assert record is not None
        assert record.event_id == first.event_id
        assert record.latitude == pytest.approx(first.latitude)
        assert record.longitude == pytest.approx(first.longitude)

        fields = await cast(
            Awaitable[dict[str, str]],
            redis_client.hgetall(f"{namespace}:live:location:{vehicle_id}"),
        )
        assert fields["event_id"] == str(first.event_id)
        assert float(fields["latitude"]) == pytest.approx(first.latitude)
        positions = await redis_client.geopos(f"{namespace}:live:geo", str(vehicle_id))
        assert positions and positions[0] is not None
        longitude, latitude = positions[0]
        assert float(latitude) == pytest.approx(first.latitude)
        assert float(longitude) == pytest.approx(first.longitude)

        await publish_events(topic, [newer])
        await wait_until(lambda: service.stats.applied == 2)
        record = await repository.latest(vehicle_id)
        coordinates = await repository.coordinates(vehicle_id)
        assert record is not None
        assert record.event_id == newer.event_id
        assert record.latitude == pytest.approx(newer.latitude)
        assert coordinates == pytest.approx((newer.latitude, newer.longitude))

        await publish_events(topic, [older])
        await wait_until(lambda: service.stats.stale == 1)
        record = await repository.latest(vehicle_id)
        coordinates = await repository.coordinates(vehicle_id)
        assert record is not None
        assert record.event_id == newer.event_id
        assert record.latitude == pytest.approx(newer.latitude)
        assert coordinates == pytest.approx((newer.latitude, newer.longitude))

        await stop_worker(consumer, dead_letters, task, stop_event)
        task = None
        assert await committed_offsets(group_id, topic) == {0: 3}
        assert service.stats.processed == 3
        assert service.stats.applied == 2
        assert service.stats.stale == 1
        assert service.stats.redis_failures == 0
    finally:
        if task is not None:
            stop_event.set()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(task, timeout=30.0)
        await consumer.stop()
        await dead_letters.close()
        await worker_redis.close()
        await delete_topic(topic)
        await delete_topic(dlq_topic)


@pytest.mark.asyncio
async def test_redis_duplicate_after_worker_restart_is_safely_acknowledged(
    redis_client: Redis,
    namespace: str,
) -> None:
    topic = unique("gps.events.test")
    dlq_topic = unique("gps.events.dlq.test")
    group_id = unique("fleetpulse-gps-test")
    settings = worker_settings(
        topic=topic,
        dlq_topic=dlq_topic,
        group_id=group_id,
        prefix=namespace,
    )
    event = make_event(recorded_at=RECORDED_AT)
    repository = LiveLocationRepository(redis_client, prefix=namespace)

    async def run_once() -> GpsConsumerService:
        consumer, dead_letters, service, worker_redis = gps_worker.build_worker(settings)
        stop_event = asyncio.Event()
        task: asyncio.Task[None] | None = None
        try:
            await worker_redis.start()
            await dead_letters.start()
            await consumer.start()
            task = asyncio.create_task(consumer.run(stop_event))
            await wait_until(lambda: service.stats.processed >= 1)
            await stop_worker(consumer, dead_letters, task, stop_event)
            task = None
            return service
        finally:
            if task is not None:
                stop_event.set()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(task, timeout=30.0)
            await consumer.stop()
            await dead_letters.close()
            await worker_redis.close()

    try:
        await publish_events(topic, [event])
        first_service = await run_once()
        assert first_service.stats.applied == 1
        assert await committed_offsets(group_id, topic) == {0: 1}

        # Simulate redelivery after a crash between the Redis write and the
        # offset commit: a fresh worker has no in-memory dedup state, so the
        # atomic Redis compare-and-set is the final duplicate protection.
        await publish_events(topic, [event])
        second_service = await run_once()

        assert second_service.stats.applied == 0
        assert second_service.stats.location_duplicates == 1
        assert second_service.stats.duplicates == 0
        assert second_service.stats.processed == 1
        assert await committed_offsets(group_id, topic) == {0: 2}

        record = await repository.latest(event.vehicle_id)
        assert record is not None
        assert record.event_id == event.event_id
        coordinates = await repository.coordinates(event.vehicle_id)
        assert coordinates == pytest.approx((event.latitude, event.longitude))
    finally:
        await delete_topic(topic)
        await delete_topic(dlq_topic)


@pytest.mark.asyncio
async def test_redis_unavailable_does_not_commit_before_dlq_acknowledgement(
    namespace: str,
) -> None:
    topic = unique("gps.events.test")
    dlq_topic = unique("gps.events.dlq.test")
    group_id = unique("fleetpulse-gps-test")
    settings = get_settings()
    event = make_event(recorded_at=RECORDED_AT)
    unreachable = Redis.from_url(
        "redis://127.0.0.1:1/0",
        encoding="utf-8",
        decode_responses=True,
        socket_connect_timeout=0.2,
        socket_timeout=0.2,
    )
    live_location = LiveLocationService(
        LiveLocationRepository(unreachable, prefix=namespace),
        stale_after_seconds=60,
    )
    dead_letters = KafkaDeadLetterPublisher(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=dlq_topic,
        partitions=PARTITIONS,
        replication_factor=1,
        delivery_timeout_seconds=15.0,
    )
    service = GpsConsumerService(
        handler=LiveLocationGpsEventHandler(live_location),
        dead_letter_publisher=dead_letters,
        deduplicator=InMemoryEventDeduplicator(),
        max_attempts=2,
        retry_delay_seconds=3.0,
    )
    consumer = KafkaGpsConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=topic,
        group_id=group_id,
        partitions=PARTITIONS,
        replication_factor=1,
        poll_timeout_seconds=0.2,
        service=service,
    )
    stop_event = asyncio.Event()
    task: asyncio.Task[None] | None = None

    try:
        await consumer.start()
        await dead_letters.start()
        await publish_events(topic, [event])
        task = asyncio.create_task(consumer.run(stop_event))

        # The first Redis failure happens before the retry delay; nothing may
        # be committed while the event is still unhandled.
        await wait_until(lambda: service.stats.redis_failures >= 1)
        assert await committed_offsets(group_id, topic) == {}

        await wait_until(lambda: service.stats.quarantined == 1, timeout_seconds=30.0)
        await stop_worker(consumer, dead_letters, task, stop_event)
        task = None

        assert service.stats.processed == 0
        assert service.stats.applied == 0
        assert service.stats.redis_failures == 2
        assert service.stats.retries == 1
        assert service.stats.quarantined == 1
        assert await committed_offsets(group_id, topic) == {0: 1}

        record = await consume_one(dlq_topic)
        envelope = json.loads(record.value)
        assert envelope["original_topic"] == topic
        assert envelope["original_offset"] == 0
        assert envelope["event_id"] == str(event.event_id)
        assert json.loads(envelope["payload"]) == event.model_dump(mode="json")
        assert envelope["error"]["type"] == "redis_error"
        assert "127.0.0.1" not in envelope["error"]["message"]
        assert "redis://" not in envelope["error"]["message"]
    finally:
        if task is not None:
            stop_event.set()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(task, timeout=30.0)
        await consumer.stop()
        await dead_letters.close()
        await unreachable.aclose()
        await delete_topic(topic)
        await delete_topic(dlq_topic)
