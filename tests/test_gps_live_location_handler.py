"""Unit tests for the Kafka-to-Redis live-location handler (no broker/Redis)."""

import json
from datetime import UTC, datetime
from typing import cast
from uuid import uuid4

import pytest
from redis.asyncio import Redis
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
)
from redis.exceptions import (
    ResponseError,
)
from redis.exceptions import (
    TimeoutError as RedisTimeoutError,
)

from app.repositories.live_location import LiveLocationRepository
from app.schemas.gps import GpsEvent
from app.schemas.live_location import LocationUpdateOutcome, LocationUpdateResult
from app.services.gps_consumer import (
    DeadLetterRecord,
    GpsConsumeOutcome,
    GpsConsumerService,
    GpsHandlerError,
    GpsHandlerOutcome,
    GpsMessage,
    InMemoryEventDeduplicator,
)
from app.services.live_location import LiveLocationService, LiveLocationUnavailableError
from app.services.live_location_handler import LiveLocationGpsEventHandler


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


def event_bytes(event: GpsEvent) -> bytes:
    return json.dumps(event.model_dump(mode="json")).encode("utf-8")


def make_message(value: bytes, *, offset: int = 7) -> GpsMessage:
    return GpsMessage(
        topic="gps.events",
        partition=0,
        offset=offset,
        timestamp_ms=1_767_268_800_000,
        key=b"vehicle-key",
        value=value,
    )


class FakeLiveLocationService:
    """Return a configured outcome or raise a configured number of failures."""

    def __init__(
        self,
        *,
        outcome: LocationUpdateOutcome = LocationUpdateOutcome.APPLIED,
        fail_times: int = 0,
    ) -> None:
        self.outcome = outcome
        self.fail_times = fail_times
        self.calls = 0
        self.events: list[GpsEvent] = []

    async def update_from_event(
        self,
        event: GpsEvent,
        *,
        received_at: datetime | None = None,
    ) -> LocationUpdateResult:
        del received_at
        self.calls += 1
        if self.calls <= self.fail_times:
            raise LiveLocationUnavailableError("live-location update is unavailable")
        self.events.append(event)
        return LocationUpdateResult(
            outcome=self.outcome,
            vehicle_id=event.vehicle_id,
            event_id=event.event_id,
        )


class FailingRedis:
    """Minimal Redis client whose atomic update raises a configured error."""

    def __init__(self, error: BaseException) -> None:
        self.error = error

    async def eval(self, *args: object, **kwargs: object) -> object:
        del args, kwargs
        raise self.error


class RecordingDeadLetterPublisher:
    def __init__(self) -> None:
        self.records: list[DeadLetterRecord] = []

    async def publish(self, record: DeadLetterRecord) -> None:
        self.records.append(record)


def build_handler(service: FakeLiveLocationService) -> LiveLocationGpsEventHandler:
    return LiveLocationGpsEventHandler(cast(LiveLocationService, service))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("location_outcome", "handler_outcome"),
    [
        (LocationUpdateOutcome.APPLIED, GpsHandlerOutcome.APPLIED),
        (LocationUpdateOutcome.DUPLICATE, GpsHandlerOutcome.DUPLICATE),
        (LocationUpdateOutcome.STALE, GpsHandlerOutcome.STALE),
    ],
)
async def test_handler_maps_live_location_outcomes(
    location_outcome: LocationUpdateOutcome,
    handler_outcome: GpsHandlerOutcome,
) -> None:
    service = FakeLiveLocationService(outcome=location_outcome)
    handler = build_handler(service)
    event = make_event()

    result = await handler.handle(event)

    assert result.outcome is handler_outcome
    assert service.events == [event]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        RedisConnectionError("connection lost"),
        RedisTimeoutError("command timed out"),
        ResponseError("OOM command not allowed when used memory > 'maxmemory'."),
        TimeoutError("socket timed out"),
    ],
)
async def test_redis_errors_become_retryable_handler_errors(error: BaseException) -> None:
    service = LiveLocationService(
        LiveLocationRepository(cast(Redis, FailingRedis(error)), prefix="handler-test"),
    )
    handler = LiveLocationGpsEventHandler(service)

    with pytest.raises(GpsHandlerError) as exc_info:
        await handler.handle(make_event())

    assert exc_info.value.failure_kind == "redis"
    # The application-level message never exposes Redis internals.
    assert "OOM" not in str(exc_info.value)
    assert "connection lost" not in str(exc_info.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("location_outcome", "expected_stat"),
    [
        (LocationUpdateOutcome.APPLIED, "applied"),
        (LocationUpdateOutcome.DUPLICATE, "location_duplicates"),
        (LocationUpdateOutcome.STALE, "stale"),
    ],
)
async def test_consumer_counts_distinct_redis_outcomes(
    location_outcome: LocationUpdateOutcome,
    expected_stat: str,
) -> None:
    service = FakeLiveLocationService(outcome=location_outcome)
    consumer = GpsConsumerService(
        handler=build_handler(service),
        dead_letter_publisher=RecordingDeadLetterPublisher(),
        retry_delay_seconds=0,
    )
    event = make_event()

    result = await consumer.process(make_message(event_bytes(event)))

    assert result.outcome is GpsConsumeOutcome.PROCESSED
    assert consumer.stats.processed == 1
    assert getattr(consumer.stats, expected_stat) == 1
    assert consumer.stats.redis_failures == 0


@pytest.mark.asyncio
async def test_redis_failure_is_not_processed_and_is_dead_lettered() -> None:
    service = FakeLiveLocationService(fail_times=100)
    dead_letters = RecordingDeadLetterPublisher()
    deduplicator = InMemoryEventDeduplicator()
    consumer = GpsConsumerService(
        handler=build_handler(service),
        dead_letter_publisher=dead_letters,
        deduplicator=deduplicator,
        max_attempts=3,
        retry_delay_seconds=0,
    )
    event = make_event()
    message = make_message(event_bytes(event), offset=11)

    result = await consumer.process(message)

    assert result.outcome is GpsConsumeOutcome.QUARANTINED
    assert consumer.stats.processed == 0
    assert consumer.stats.applied == 0
    assert consumer.stats.redis_failures == 3
    assert consumer.stats.handler_failures == 3
    assert consumer.stats.retries == 2
    assert consumer.stats.quarantined == 1
    assert consumer.stats.invalid == 0
    assert await deduplicator.seen(event.event_id) is False

    assert len(dead_letters.records) == 1
    record = dead_letters.records[0]
    assert record.error_type == "redis_error"
    assert "GpsHandlerError" in record.error_message
    assert record.payload == message.value
    assert record.event_id == event.event_id
    assert (record.topic, record.partition, record.offset) == ("gps.events", 0, 11)


@pytest.mark.asyncio
async def test_redis_failure_recovers_within_retry_budget() -> None:
    service = FakeLiveLocationService(fail_times=2)
    consumer = GpsConsumerService(
        handler=build_handler(service),
        dead_letter_publisher=RecordingDeadLetterPublisher(),
        max_attempts=3,
        retry_delay_seconds=0,
    )
    event = make_event()

    result = await consumer.process(make_message(event_bytes(event)))

    assert result.outcome is GpsConsumeOutcome.PROCESSED
    assert consumer.stats.processed == 1
    assert consumer.stats.applied == 1
    assert consumer.stats.redis_failures == 2
    assert consumer.stats.retries == 2
    assert consumer.stats.quarantined == 0


def test_stats_summary_distinguishes_location_outcomes() -> None:
    from app.services.gps_consumer import GpsConsumerStats

    summary = GpsConsumerStats(
        received=5,
        processed=3,
        applied=1,
        duplicates=1,
        location_duplicates=1,
        stale=1,
        redis_failures=2,
        quarantined=1,
    ).summary()

    for label in (
        "received=5",
        "applied=1",
        "duplicates=1",
        "location_duplicates=1",
        "stale=1",
        "redis_failures=2",
        "quarantined=1",
    ):
        assert label in summary
