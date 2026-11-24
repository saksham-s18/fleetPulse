"""Transport-neutral processing pipeline for consumed GPS events.

The Kafka transport (fetching records, committing offsets) lives in
:mod:`app.infrastructure.kafka_consumer`. This module owns the decisions that
must be testable without a broker:

- decode a raw message and validate it against :class:`~app.schemas.gps.GpsEvent`;
- skip events that were already processed successfully (in-memory dedup);
- run a replaceable handler with a bounded retry policy;
- quarantine messages that can never be processed (invalid payload or exhausted
  handler retries) through a replaceable dead-letter publisher.

The service never commits offsets. It returns an outcome only after the
message is processed *or* durably quarantined, so the transport can commit
afterwards. If quarantine fails, :class:`GpsConsumerError` is raised and the
offset stays uncommitted.
"""

import asyncio
import json
import logging
from collections import OrderedDict
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from pydantic import ValidationError

from app.schemas.gps import GpsEvent

logger = logging.getLogger(__name__)

DEFAULT_DEDUPLICATION_CAPACITY = 100_000
_MAX_ERROR_MESSAGE_LENGTH = 500


class GpsConsumerError(Exception):
    """Raised when a message cannot be processed or safely quarantined.

    Messages must stay safe for server-side logs: no bootstrap servers,
    credentials or raw broker internals.
    """


class DeadLetterPublishError(Exception):
    """Raised when a quarantine record could not be acknowledged by the DLQ."""


class GpsConsumeOutcome(StrEnum):
    """Authoritative result of processing one consumed message."""

    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    QUARANTINED = "quarantined"


@dataclass(frozen=True, slots=True)
class GpsMessage:
    """One consumed Kafka record, stripped of the client-specific type."""

    topic: str
    partition: int
    offset: int
    timestamp_ms: int | None
    key: bytes | None
    value: bytes | None


@dataclass(frozen=True, slots=True)
class DeadLetterRecord:
    """Everything needed to quarantine one failed message durably."""

    topic: str
    partition: int
    offset: int
    timestamp_ms: int | None
    key: bytes | None
    payload: bytes | None
    error_type: str
    error_message: str
    event_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class ConsumeResult:
    """Outcome reported back to the transport for offset management."""

    outcome: GpsConsumeOutcome
    event_id: UUID | None = None


@dataclass(slots=True)
class GpsConsumerStats:
    """Counters exposed for worker summaries and deterministic tests."""

    received: int = 0
    processed: int = 0
    duplicates: int = 0
    invalid: int = 0
    handler_failures: int = 0
    retries: int = 0
    quarantined: int = 0

    def summary(self) -> str:
        """Return a single-line, log-friendly counter summary."""
        return (
            f"received={self.received} processed={self.processed} "
            f"duplicates={self.duplicates} invalid={self.invalid} "
            f"handler_failures={self.handler_failures} retries={self.retries} "
            f"quarantined={self.quarantined}"
        )


class GpsEventHandler(Protocol):
    """Process one validated GPS event.

    Future milestones can replace the logging handler with Redis projections or
    PostgreSQL persistence without touching the consumer service.
    """

    async def handle(self, event: GpsEvent) -> None:
        """Process ``event``; raise to trigger the retry policy."""
        ...


class DeadLetterPublisher(Protocol):
    """Durably store messages that cannot be processed."""

    async def publish(self, record: DeadLetterRecord) -> None:
        """Publish ``record`` and wait for the broker acknowledgement."""
        ...


class EventDeduplicator(Protocol):
    """Track event IDs that finished processing successfully."""

    async def seen(self, event_id: UUID) -> bool:
        """Return ``True`` when ``event_id`` was already processed."""
        ...

    async def mark(self, event_id: UUID) -> None:
        """Remember ``event_id`` after a successful handler call."""
        ...


class InMemoryEventDeduplicator:
    """Bounded, process-local duplicate detection.

    The implementation is deliberately in-memory: it does not survive worker
    restarts and is not shared between worker instances. It is enough for the
    at-least-once redelivery window of a single local worker and can be swapped
    for a durable store later through :class:`EventDeduplicator`.

    The oldest IDs are evicted once ``capacity`` is reached, so memory stays
    bounded. After eviction an old duplicate can be processed again.
    """

    def __init__(self, capacity: int = DEFAULT_DEDUPLICATION_CAPACITY) -> None:
        if capacity < 1:
            raise ValueError("capacity must be at least 1")
        self._capacity = capacity
        self._seen: OrderedDict[UUID, None] = OrderedDict()

    async def seen(self, event_id: UUID) -> bool:
        return event_id in self._seen

    async def mark(self, event_id: UUID) -> None:
        self._seen[event_id] = None
        self._seen.move_to_end(event_id)
        while len(self._seen) > self._capacity:
            self._seen.popitem(last=False)

    def __len__(self) -> int:
        return len(self._seen)


class LoggingGpsEventHandler:
    """Default handler: structured logging only (no persistence yet)."""

    async def handle(self, event: GpsEvent) -> None:
        logger.info(
            "Processed GPS event %s for vehicle %s (recorded_at=%s, lat=%.5f, lon=%.5f)",
            event.event_id,
            event.vehicle_id,
            event.recorded_at.isoformat(),
            event.latitude,
            event.longitude,
        )


class GpsConsumerService:
    """Decode, deduplicate, handle and quarantine one message at a time."""

    def __init__(
        self,
        *,
        handler: GpsEventHandler,
        dead_letter_publisher: DeadLetterPublisher,
        deduplicator: EventDeduplicator | None = None,
        max_attempts: int = 3,
        retry_delay_seconds: float = 1.0,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must not be negative")

        self._handler = handler
        self._dead_letters = dead_letter_publisher
        self._deduplicator = (
            deduplicator if deduplicator is not None else InMemoryEventDeduplicator()
        )
        self._max_attempts = max_attempts
        self._retry_delay_seconds = retry_delay_seconds
        self.stats = GpsConsumerStats()

    async def process(self, message: GpsMessage) -> ConsumeResult:
        """Process one message; return only when it is safe to commit."""
        self.stats.received += 1

        event, error_type, error_message = self._deserialize(message)
        if event is None:
            await self._quarantine(
                message,
                error_type=error_type,
                error_message=error_message,
                event_id=None,
            )
            return ConsumeResult(GpsConsumeOutcome.QUARANTINED)

        if await self._deduplicator.seen(event.event_id):
            self.stats.duplicates += 1
            logger.info(
                "Skipping duplicate GPS event %s at %s[%d]@%d",
                event.event_id,
                message.topic,
                message.partition,
                message.offset,
            )
            return ConsumeResult(GpsConsumeOutcome.DUPLICATE, event.event_id)

        last_error: Exception | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                await self._handler.handle(event)
            except Exception as exc:  # explicit handler boundary: retry/quarantine
                last_error = exc
                self.stats.handler_failures += 1
                logger.warning(
                    "GPS event %s handler attempt %d/%d failed (%s)",
                    event.event_id,
                    attempt,
                    self._max_attempts,
                    type(exc).__name__,
                )
                if attempt < self._max_attempts:
                    self.stats.retries += 1
                    if self._retry_delay_seconds > 0:
                        await asyncio.sleep(self._retry_delay_seconds)
                continue

            # Mark only after success so a failed event can be retried after
            # redelivery instead of being silently treated as a duplicate.
            await self._deduplicator.mark(event.event_id)
            self.stats.processed += 1
            logger.debug(
                "Processed GPS event %s at %s[%d]@%d",
                event.event_id,
                message.topic,
                message.partition,
                message.offset,
            )
            return ConsumeResult(GpsConsumeOutcome.PROCESSED, event.event_id)

        if last_error is None:  # pragma: no cover - loop always runs once
            raise GpsConsumerError("GPS event handler produced no result")

        await self._quarantine(
            message,
            error_type="handler_error",
            error_message=f"{type(last_error).__name__}: {last_error}",
            event_id=event.event_id,
        )
        return ConsumeResult(GpsConsumeOutcome.QUARANTINED, event.event_id)

    async def _quarantine(
        self,
        message: GpsMessage,
        *,
        error_type: str,
        error_message: str,
        event_id: UUID | None,
    ) -> None:
        """Dead-letter a message and wait for the acknowledgement."""
        record = DeadLetterRecord(
            topic=message.topic,
            partition=message.partition,
            offset=message.offset,
            timestamp_ms=message.timestamp_ms,
            key=message.key,
            payload=message.value,
            error_type=error_type,
            error_message=error_message[:_MAX_ERROR_MESSAGE_LENGTH],
            event_id=event_id,
        )
        try:
            await self._dead_letters.publish(record)
        except DeadLetterPublishError as exc:
            # The transport must not commit: the message is neither processed
            # nor safely quarantined. The worker stops and Kafka redelivers it.
            raise GpsConsumerError(
                f"message at {message.topic}[{message.partition}]@{message.offset} "
                "could not be dead-lettered"
            ) from exc

        self.stats.quarantined += 1
        if error_type != "handler_error":
            self.stats.invalid += 1
        logger.warning(
            "Quarantined message at %s[%d]@%d (%s)",
            message.topic,
            message.partition,
            message.offset,
            error_type,
        )

    @staticmethod
    def _deserialize(message: GpsMessage) -> tuple[GpsEvent | None, str, str]:
        """Return ``(event, "", "")`` or ``(None, error_type, message)``."""
        if message.value is None:
            return None, "invalid_json", "message value is empty"
        try:
            document = json.loads(message.value)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            return None, "invalid_json", _truncate(f"{type(exc).__name__}: {exc}")

        try:
            event = GpsEvent.model_validate(document)
        except ValidationError as exc:
            return None, "invalid_schema", _truncate(_summarise_validation_error(exc))
        return event, "", ""


def _truncate(message: str) -> str:
    """Keep quarantine error messages bounded and log-safe."""
    return message[:_MAX_ERROR_MESSAGE_LENGTH]


def _summarise_validation_error(exc: ValidationError) -> str:
    """Compact ``ValidationError`` into field paths and messages."""
    parts: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"])
        message = str(error["msg"])
        parts.append(f"{location}: {message}" if location else message)
    return "; ".join(parts) or "validation failed"
