"""Kafka consumer and dead-letter transport for GPS events.

This module owns every Kafka-specific consumer concern: explicit topic
creation, polling, partition assignment logging, manual offset commits and
dead-letter publication. Event processing decisions live in
:mod:`app.services.gps_consumer`.

Offset semantics:

- ``enable_auto_commit=False``; the service is asked to process exactly one
  record and the commit happens only after it reports success, a duplicate or a
  durably quarantined (dead-lettered) message.
- Commits use ``record.offset + 1``, the next offset Kafka should deliver.
- A failed quarantine raises before the commit, so the message is redelivered:
  processing is at-least-once, never exactly-once.

Ordering: one record is processed at a time, so records from a partition can
never run concurrently. Because producers key messages by ``vehicle_id``, all
events of one vehicle share a partition and keep their order. Running several
worker instances rebalances whole partitions; concurrency across independent
partitions can be added later without changing the handler contract.
"""

import asyncio
import base64
import json
import logging
from datetime import UTC, datetime
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, OffsetAndMetadata, TopicPartition
from aiokafka.errors import CommitFailedError, KafkaError

from app.infrastructure.kafka import ensure_topic
from app.services.gps_consumer import (
    DeadLetterPublishError,
    DeadLetterRecord,
    GpsConsumeOutcome,
    GpsConsumerError,
    GpsConsumerService,
    GpsMessage,
)

logger = logging.getLogger(__name__)


class KafkaGpsConsumer:
    """Fetch GPS records sequentially and commit offsets after safe handling."""

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        partitions: int,
        replication_factor: int,
        poll_timeout_seconds: float,
        service: GpsConsumerService,
    ) -> None:
        if not bootstrap_servers:
            raise ValueError("bootstrap_servers must not be empty")
        if not topic:
            raise ValueError("topic must not be empty")
        if not group_id:
            raise ValueError("group_id must not be empty")
        if partitions < 1:
            raise ValueError("partitions must be at least 1")
        if replication_factor < 1:
            raise ValueError("replication_factor must be at least 1")
        if poll_timeout_seconds <= 0:
            raise ValueError("poll_timeout_seconds must be positive")

        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._group_id = group_id
        self._partitions = partitions
        self._replication_factor = replication_factor
        self._poll_timeout_ms = max(1, int(poll_timeout_seconds * 1000))
        self._service = service
        self._started = False
        self._closed = False
        self._consumer_started = False
        self._assignment: frozenset[TopicPartition] = frozenset()
        self._consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            client_id="fleetpulse-gps-consumer",
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            # One record per partition per fetch keeps processing sequential.
            max_poll_records=1,
        )

    @property
    def topic(self) -> str:
        """Name of the consumed topic."""
        return self._topic

    @property
    def group_id(self) -> str:
        """Consumer group used for partition assignment and offsets."""
        return self._group_id

    async def start(self) -> None:
        """Create the GPS topic if needed, then join the consumer group."""
        if self._started or self._closed:
            return
        try:
            await ensure_topic(
                bootstrap_servers=self._bootstrap_servers,
                topic=self._topic,
                partitions=self._partitions,
                replication_factor=self._replication_factor,
            )
        except (TimeoutError, KafkaError, OSError) as exc:
            raise GpsConsumerError("GPS topic could not be created") from exc

        try:
            await self._consumer.start()
        except (TimeoutError, KafkaError, OSError) as exc:
            raise GpsConsumerError("Kafka GPS consumer failed to start") from exc
        self._consumer_started = True
        self._started = True
        logger.info(
            "GPS consumer started: topic=%s group=%s partitions=%d",
            self._topic,
            self._group_id,
            self._partitions,
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        """Poll and process messages until ``stop_event`` is set.

        The record in flight (if any) is always finished and committed before
        the loop exits; no new fetch is started after shutdown is requested.
        """
        if not self._started:
            raise GpsConsumerError("GPS consumer is not started")

        while not stop_event.is_set():
            try:
                batches = await self._consumer.getmany(
                    timeout_ms=self._poll_timeout_ms,
                    max_records=1,
                )
            except (TimeoutError, KafkaError, OSError) as exc:
                raise GpsConsumerError("Kafka GPS consumer fetch failed") from exc

            await self._log_assignment()

            for topic_partition, records in batches.items():
                for record in records:
                    await self._process_record(topic_partition, record)
                    if stop_event.is_set():
                        break
                if stop_event.is_set():
                    break

        logger.info("GPS consumer stopped fetching topic %s", self._topic)

    async def _process_record(self, topic_partition: TopicPartition, record: Any) -> None:
        """Process one record and commit only when the service says it is safe."""
        message = GpsMessage(
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            timestamp_ms=record.timestamp,
            key=record.key,
            value=record.value,
        )
        result = await self._service.process(message)
        await self._commit(topic_partition, record.offset + 1)
        logger.debug(
            "Committed %s[%d]@%d after %s",
            record.topic,
            record.partition,
            record.offset,
            result.outcome,
        )
        if result.outcome is GpsConsumeOutcome.QUARANTINED:
            logger.warning(
                "Offset %s[%d]@%d advanced after dead-letter acknowledgement",
                record.topic,
                record.partition,
                record.offset,
            )

    async def _commit(self, topic_partition: TopicPartition, next_offset: int) -> None:
        """Commit the next offset; a rebalance failure is left to redelivery."""
        try:
            await self._consumer.commit({topic_partition: OffsetAndMetadata(next_offset, "")})
        except CommitFailedError:
            # The group rebalanced after processing. Dedup suppresses a
            # duplicate handler run for the redelivered event.
            logger.warning(
                "Offset commit rejected for %s; the record may be redelivered",
                topic_partition,
            )
        except (TimeoutError, KafkaError, OSError) as exc:
            raise GpsConsumerError(f"Kafka offset commit failed for {topic_partition}") from exc

    async def _log_assignment(self) -> None:
        """Log partition changes once per assignment, not once per poll."""
        try:
            assignment = frozenset(self._consumer.assignment())
        except (TimeoutError, KafkaError, OSError):  # pragma: no cover - best effort
            return
        if assignment == self._assignment:
            return
        self._assignment = assignment
        if assignment:
            rendered = ", ".join(
                f"{partition.topic}[{partition.partition}]" for partition in sorted(assignment)
            )
            logger.info("GPS consumer assigned partitions: %s", rendered)
        else:
            logger.info("GPS consumer has no partitions assigned yet")

    async def stop(self) -> None:
        """Close the consumer; safe to call more than once."""
        if self._closed:
            return
        self._closed = True
        self._started = False
        if not self._consumer_started:
            return
        self._consumer_started = False
        try:
            await self._consumer.stop()
        except (TimeoutError, KafkaError, OSError):  # pragma: no cover - best effort
            logger.warning("GPS consumer stopped with errors")


class KafkaDeadLetterPublisher:
    """Publish quarantine records to an explicitly created dead-letter topic.

    The original Kafka key is preserved so per-vehicle ordering also holds in
    the DLQ. The envelope keeps the original topic/partition/offset/timestamp,
    the raw payload (UTF-8 when possible, base64 otherwise) and the error.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        partitions: int,
        replication_factor: int,
        delivery_timeout_seconds: float,
    ) -> None:
        if not bootstrap_servers:
            raise ValueError("bootstrap_servers must not be empty")
        if not topic:
            raise ValueError("topic must not be empty")
        if partitions < 1:
            raise ValueError("partitions must be at least 1")
        if replication_factor < 1:
            raise ValueError("replication_factor must be at least 1")
        if delivery_timeout_seconds <= 0:
            raise ValueError("delivery_timeout_seconds must be positive")

        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._partitions = partitions
        self._replication_factor = replication_factor
        self._delivery_timeout_seconds = delivery_timeout_seconds
        self._started = False
        self._closed = False
        self._producer_started = False
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id="fleetpulse-gps-dlq-producer",
            acks="all",
            enable_idempotence=True,
            request_timeout_ms=int(delivery_timeout_seconds * 1000),
        )

    @property
    def topic(self) -> str:
        """Name of the dead-letter topic."""
        return self._topic

    async def start(self) -> None:
        """Create the DLQ topic if needed and start the producer."""
        if self._started or self._closed:
            return
        try:
            await ensure_topic(
                bootstrap_servers=self._bootstrap_servers,
                topic=self._topic,
                partitions=self._partitions,
                replication_factor=self._replication_factor,
            )
        except (TimeoutError, KafkaError, OSError) as exc:
            raise DeadLetterPublishError("dead-letter topic could not be created") from exc

        try:
            await self._producer.start()
        except (TimeoutError, KafkaError, OSError) as exc:
            raise DeadLetterPublishError("dead-letter producer failed to start") from exc
        self._producer_started = True
        self._started = True
        logger.info("Dead-letter publisher started for topic %s", self._topic)

    async def publish(self, record: DeadLetterRecord) -> None:
        """Publish one quarantine envelope and wait for broker acknowledgement."""
        if not self._started:
            raise DeadLetterPublishError("dead-letter publisher is not started")

        envelope = self._envelope(record)
        try:
            # aiokafka retries retriable errors indefinitely; cap the total wait.
            await asyncio.wait_for(
                self._producer.send_and_wait(
                    self._topic,
                    value=envelope,
                    key=record.key,
                ),
                timeout=self._delivery_timeout_seconds,
            )
        except (TimeoutError, KafkaError, OSError) as exc:
            raise DeadLetterPublishError(
                f"dead-letter publication failed for "
                f"{record.topic}[{record.partition}]@{record.offset}"
            ) from exc

    async def close(self) -> None:
        """Stop the producer; safe to call more than once."""
        if self._closed:
            return
        self._closed = True
        self._started = False
        if not self._producer_started:
            return
        self._producer_started = False
        try:
            await self._producer.stop()
        except (TimeoutError, KafkaError, OSError):  # pragma: no cover - best effort
            logger.warning("Dead-letter producer stopped with errors")

    @staticmethod
    def _envelope(record: DeadLetterRecord) -> bytes:
        payload, payload_encoding = _encode_bytes(record.payload)
        original_key, original_key_encoding = _encode_bytes(record.key)
        envelope = {
            "original_topic": record.topic,
            "original_partition": record.partition,
            "original_offset": record.offset,
            "original_timestamp_ms": record.timestamp_ms,
            "original_key": original_key,
            "original_key_encoding": original_key_encoding,
            "event_id": str(record.event_id) if record.event_id is not None else None,
            "payload": payload,
            "payload_encoding": payload_encoding,
            "error": {
                "type": record.error_type,
                "message": record.error_message,
            },
            "quarantined_at": datetime.now(UTC).isoformat(),
        }
        return json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _encode_bytes(value: bytes | None) -> tuple[str | None, str | None]:
    """Encode a raw Kafka byte string for a JSON envelope."""
    if value is None:
        return None, None
    try:
        return value.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:
        return base64.b64encode(value).decode("ascii"), "base64"
