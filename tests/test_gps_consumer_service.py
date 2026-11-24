"""Unit tests for the transport-neutral GPS consumer service (no broker)."""

import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from app.schemas.gps import GpsEvent
from app.services.gps_consumer import (
    ConsumeResult,
    DeadLetterPublishError,
    DeadLetterRecord,
    GpsConsumeOutcome,
    GpsConsumerError,
    GpsConsumerService,
    GpsMessage,
    InMemoryEventDeduplicator,
)


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


def event_bytes(**overrides: object) -> bytes:
    return json.dumps(make_event(**overrides).model_dump(mode="json")).encode("utf-8")


def make_message(
    value: bytes | None,
    *,
    topic: str = "gps.events",
    partition: int = 2,
    offset: int = 41,
    timestamp_ms: int | None = 1_767_268_800_000,
    key: bytes | None = b"vehicle-key",
) -> GpsMessage:
    return GpsMessage(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp_ms=timestamp_ms,
        key=key,
        value=value,
    )


class RecordingHandler:
    """Fail a configurable number of times, then record handled events."""

    def __init__(self, *, fail_times: int = 0) -> None:
        self.fail_times = fail_times
        self.calls = 0
        self.events: list[GpsEvent] = []

    async def handle(self, event: GpsEvent) -> None:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError("handler exploded")
        self.events.append(event)


class RecordingDeadLetterPublisher:
    """Record quarantine envelopes; optionally reject publication."""

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.records: list[DeadLetterRecord] = []

    async def publish(self, record: DeadLetterRecord) -> None:
        if self.fail:
            raise DeadLetterPublishError("dlq unavailable")
        self.records.append(record)


def make_service(
    *,
    handler: RecordingHandler | None = None,
    dead_letters: RecordingDeadLetterPublisher | None = None,
    deduplicator: InMemoryEventDeduplicator | None = None,
    max_attempts: int = 3,
    retry_delay_seconds: float = 0.0,
) -> GpsConsumerService:
    return GpsConsumerService(
        handler=handler or RecordingHandler(),
        dead_letter_publisher=dead_letters or RecordingDeadLetterPublisher(),
        deduplicator=deduplicator if deduplicator is not None else InMemoryEventDeduplicator(),
        max_attempts=max_attempts,
        retry_delay_seconds=retry_delay_seconds,
    )


@pytest.mark.asyncio
async def test_valid_message_is_processed_and_marked() -> None:
    handler = RecordingHandler()
    deduplicator = InMemoryEventDeduplicator()
    service = make_service(handler=handler, deduplicator=deduplicator)
    event = make_event()

    result = await service.process(make_message(event_bytes(event_id=event.event_id)))

    assert result.outcome is GpsConsumeOutcome.PROCESSED
    assert result.event_id == event.event_id
    assert len(handler.events) == 1
    assert handler.events[0].event_id == event.event_id
    assert await deduplicator.seen(event.event_id) is True
    assert service.stats.processed == 1
    assert service.stats.received == 1


@pytest.mark.asyncio
async def test_invalid_json_is_quarantined_with_original_metadata() -> None:
    handler = RecordingHandler()
    dead_letters = RecordingDeadLetterPublisher()
    service = make_service(handler=handler, dead_letters=dead_letters)

    result = await service.process(make_message(b"{not-json"))

    assert result.outcome is GpsConsumeOutcome.QUARANTINED
    assert handler.calls == 0
    assert len(dead_letters.records) == 1
    record = dead_letters.records[0]
    assert record.error_type == "invalid_json"
    assert record.payload == b"{not-json"
    assert record.key == b"vehicle-key"
    assert (record.topic, record.partition, record.offset) == ("gps.events", 2, 41)
    assert record.timestamp_ms == 1_767_268_800_000
    assert service.stats.invalid == 1
    assert service.stats.quarantined == 1
    assert service.stats.processed == 0


@pytest.mark.asyncio
async def test_missing_value_is_quarantined() -> None:
    dead_letters = RecordingDeadLetterPublisher()
    service = make_service(dead_letters=dead_letters)

    result = await service.process(make_message(None))

    assert result.outcome is GpsConsumeOutcome.QUARANTINED
    assert dead_letters.records[0].error_type == "invalid_json"
    assert dead_letters.records[0].payload is None


@pytest.mark.asyncio
async def test_invalid_schema_is_quarantined() -> None:
    dead_letters = RecordingDeadLetterPublisher()
    service = make_service(dead_letters=dead_letters)
    payload = json.loads(event_bytes())
    payload.pop("vehicle_id")

    result = await service.process(make_message(json.dumps(payload).encode("utf-8")))

    assert result.outcome is GpsConsumeOutcome.QUARANTINED
    record = dead_letters.records[0]
    assert record.error_type == "invalid_schema"
    assert "vehicle_id" in record.error_message
    assert service.stats.invalid == 1


@pytest.mark.asyncio
async def test_out_of_range_field_is_quarantined() -> None:
    dead_letters = RecordingDeadLetterPublisher()
    service = make_service(dead_letters=dead_letters)
    payload = json.loads(event_bytes())
    payload["latitude"] = 91.0

    result = await service.process(make_message(json.dumps(payload).encode("utf-8")))

    assert result.outcome is GpsConsumeOutcome.QUARANTINED
    assert dead_letters.records[0].error_type == "invalid_schema"


@pytest.mark.asyncio
async def test_duplicate_event_skips_handler() -> None:
    handler = RecordingHandler()
    service = make_service(handler=handler)
    event = make_event()
    message = make_message(event_bytes(event_id=event.event_id))

    first = await service.process(message)
    second = await service.process(message)

    assert first.outcome is GpsConsumeOutcome.PROCESSED
    assert second.outcome is GpsConsumeOutcome.DUPLICATE
    assert second.event_id == event.event_id
    assert handler.calls == 1
    assert service.stats.processed == 1
    assert service.stats.duplicates == 1


@pytest.mark.asyncio
async def test_deduplication_state_is_not_updated_after_failure() -> None:
    handler = RecordingHandler(fail_times=100)
    deduplicator = InMemoryEventDeduplicator()
    service = make_service(
        handler=handler,
        deduplicator=deduplicator,
        max_attempts=2,
    )
    event = make_event()
    message = make_message(event_bytes(event_id=event.event_id))

    first = await service.process(message)
    assert first.outcome is GpsConsumeOutcome.QUARANTINED
    assert await deduplicator.seen(event.event_id) is False
    assert handler.calls == 2

    # Redelivery must retry the handler instead of being treated as a duplicate.
    second = await service.process(message)
    assert second.outcome is GpsConsumeOutcome.QUARANTINED
    assert handler.calls == 4
    assert service.stats.duplicates == 0


@pytest.mark.asyncio
async def test_transient_handler_failure_recovers_within_attempts() -> None:
    handler = RecordingHandler(fail_times=2)
    dead_letters = RecordingDeadLetterPublisher()
    service = make_service(
        handler=handler,
        dead_letters=dead_letters,
        max_attempts=3,
    )

    result = await service.process(make_message(event_bytes()))

    assert result.outcome is GpsConsumeOutcome.PROCESSED
    assert handler.calls == 3
    assert service.stats.retries == 2
    assert service.stats.handler_failures == 2
    assert service.stats.quarantined == 0
    assert dead_letters.records == []


@pytest.mark.asyncio
async def test_exhausted_retries_quarantine_handler_error() -> None:
    handler = RecordingHandler(fail_times=100)
    dead_letters = RecordingDeadLetterPublisher()
    service = make_service(
        handler=handler,
        dead_letters=dead_letters,
        max_attempts=3,
    )
    event = make_event()

    result = await service.process(make_message(event_bytes(event_id=event.event_id)))

    assert result.outcome is GpsConsumeOutcome.QUARANTINED
    assert handler.calls == 3
    assert service.stats.retries == 2
    assert len(dead_letters.records) == 1
    record = dead_letters.records[0]
    assert record.error_type == "handler_error"
    assert "RuntimeError" in record.error_message
    assert record.event_id == event.event_id
    assert service.stats.quarantined == 1
    assert service.stats.invalid == 0


@pytest.mark.asyncio
async def test_dead_letter_failure_raises_and_does_not_quarantine() -> None:
    handler = RecordingHandler()
    dead_letters = RecordingDeadLetterPublisher(fail=True)
    service = make_service(handler=handler, dead_letters=dead_letters)

    with pytest.raises(GpsConsumerError, match="could not be dead-lettered"):
        await service.process(make_message(b"{broken"))

    assert service.stats.quarantined == 0
    assert handler.calls == 0


@pytest.mark.asyncio
async def test_failed_handler_and_failed_dead_letter_raises() -> None:
    handler = RecordingHandler(fail_times=100)
    dead_letters = RecordingDeadLetterPublisher(fail=True)
    service = make_service(
        handler=handler,
        dead_letters=dead_letters,
        max_attempts=1,
    )

    with pytest.raises(GpsConsumerError, match="could not be dead-lettered"):
        await service.process(make_message(event_bytes()))

    assert service.stats.quarantined == 0
    assert service.stats.handler_failures == 1


@pytest.mark.asyncio
async def test_deduplicator_evicts_oldest_entries_at_capacity() -> None:
    deduplicator = InMemoryEventDeduplicator(capacity=2)
    first, second, third = uuid4(), uuid4(), uuid4()

    await deduplicator.mark(first)
    await deduplicator.mark(second)
    await deduplicator.mark(third)

    assert await deduplicator.seen(first) is False
    assert await deduplicator.seen(second) is True
    assert await deduplicator.seen(third) is True
    assert len(deduplicator) == 2


def test_service_rejects_invalid_retry_configuration() -> None:
    handler = RecordingHandler()
    dead_letters = RecordingDeadLetterPublisher()

    with pytest.raises(ValueError, match="max_attempts"):
        make_service(handler=handler, dead_letters=dead_letters, max_attempts=0)
    with pytest.raises(ValueError, match="retry_delay_seconds"):
        make_service(handler=handler, dead_letters=dead_letters, retry_delay_seconds=-1)


def test_deduplicator_rejects_non_positive_capacity() -> None:
    with pytest.raises(ValueError, match="capacity"):
        InMemoryEventDeduplicator(capacity=0)


def test_consume_result_is_immutable() -> None:
    result = ConsumeResult(GpsConsumeOutcome.PROCESSED, UUID(int=1))
    with pytest.raises(AttributeError):
        result.outcome = GpsConsumeOutcome.DUPLICATE  # type: ignore[misc]
