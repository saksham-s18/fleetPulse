"""Kafka publisher for validated GPS events.

This module owns every Kafka-specific concern — topic administration,
JSON serialisation, message keys and broker error translation — so the GPS
service only sees the :class:`~app.services.gps_publisher.GpsEventPublisher`
protocol.

Design notes:

- One producer is created per application instance (FastAPI lifespan) and
  shared by all requests; no producer is built per request.
- ``acks=all`` plus ``enable_idempotence=True`` make successful delivery
  durable on the (single) broker and prevent duplicates on retries.
- The message key is ``vehicle_id``: Kafka's default partitioner hashes the
  key, so all events for one vehicle land on the same partition and keep
  their relative order.
- Topics are created explicitly at startup (partition count and replication
  factor come from settings); broker auto-topic creation stays disabled.
"""

import asyncio
import json
import logging

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaError

from app.schemas.gps import GpsEvent
from app.services.gps_publisher import GpsPublishError

logger = logging.getLogger(__name__)


class KafkaGpsEventPublisher:
    """Publish GPS events to a Kafka topic using the shared protocol."""

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

        self._topic = topic
        self._partitions = partitions
        self._replication_factor = replication_factor
        self._bootstrap_servers = bootstrap_servers
        self._delivery_timeout_seconds = delivery_timeout_seconds
        self._started = False
        self._closed = False
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id="fleetpulse-gps-producer",
            acks="all",
            enable_idempotence=True,
            # Bounds a single broker request; the total acknowledgement wait
            # is capped separately in publish() because aiokafka retries
            # retriable errors indefinitely (it has no delivery timeout).
            request_timeout_ms=int(delivery_timeout_seconds * 1000),
        )

    @property
    def topic(self) -> str:
        """Name of the topic events are published to."""
        return self._topic

    async def start(self) -> None:
        """Create the topic if missing, then start the producer."""
        if self._started or self._closed:
            return
        await self._ensure_topic()
        try:
            await self._producer.start()
        except (TimeoutError, KafkaError, OSError) as exc:
            raise GpsPublishError("GPS producer failed to start") from exc
        self._started = True
        logger.info("GPS producer started for topic %s", self._topic)

    async def publish(self, event: GpsEvent) -> None:
        """Serialise and deliver one event, waiting for broker acknowledgement."""
        if not self._started:
            raise GpsPublishError("GPS producer is not started")

        payload = self._serialize(event)
        key = str(event.vehicle_id).encode("utf-8")
        try:
            # aiokafka retries retriable errors forever, so cap the total
            # acknowledgement wait: a downed broker must yield 503, not hang.
            await asyncio.wait_for(
                self._producer.send_and_wait(self._topic, value=payload, key=key),
                timeout=self._delivery_timeout_seconds,
            )
        except (TimeoutError, KafkaError, OSError) as exc:
            # Server-side only: the exception chain keeps broker details for
            # logs while the API returns a generic 503 envelope.
            raise GpsPublishError(f"Kafka delivery failed for GPS event {event.event_id}") from exc

    async def close(self) -> None:
        """Stop the producer; safe to call more than once."""
        if self._closed:
            return
        self._closed = True
        self._started = False
        try:
            await self._producer.stop()
        except (TimeoutError, KafkaError, OSError):  # pragma: no cover - best effort
            logger.warning("GPS producer stopped with errors")

    @staticmethod
    def _serialize(event: GpsEvent) -> bytes:
        """Encode the event as compact JSON, preserving every contract field."""
        try:
            return json.dumps(
                event.model_dump(mode="json"),
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise GpsPublishError(f"GPS event {event.event_id} could not be serialised") from exc

    async def _ensure_topic(self) -> None:
        """Create the GPS topic with the configured partitions explicitly.

        Broker auto-topic creation is disabled in Compose, so ingestion
        would otherwise fail on a fresh broker.
        """
        admin = AIOKafkaAdminClient(
            bootstrap_servers=self._bootstrap_servers,
            client_id="fleetpulse-gps-admin",
        )
        try:
            await admin.start()
            existing = await admin.list_topics()
            if self._topic in existing:
                logger.info("GPS topic %s already exists", self._topic)
                return
            await admin.create_topics(
                [
                    NewTopic(
                        self._topic,
                        num_partitions=self._partitions,
                        replication_factor=self._replication_factor,
                    )
                ]
            )
            logger.info(
                "Created GPS topic %s (%d partitions, replication factor %d)",
                self._topic,
                self._partitions,
                self._replication_factor,
            )
        except (TimeoutError, KafkaError, OSError) as exc:
            raise GpsPublishError("GPS topic could not be created") from exc
        finally:
            await admin.close()
