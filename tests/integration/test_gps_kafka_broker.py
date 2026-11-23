"""Real Kafka broker tests for Milestone 5C.1 (marked ``kafka``).

These tests need PostgreSQL and the Kafka broker from Docker Compose::

    docker compose up -d postgres kafka
    pytest -m kafka

They are excluded from the default suite (``-m 'not integration and not
kafka'``) so unit and PostgreSQL runs never require a broker.
"""

import asyncio
import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient
from fastapi.testclient import TestClient

from app.core.config import Settings, get_settings
from app.infrastructure.kafka import KafkaGpsEventPublisher
from app.main import create_app
from app.schemas.gps import GpsEvent
from tests.integration.helpers import create_vehicle

pytestmark = pytest.mark.kafka


def unique_topic() -> str:
    """Return an isolated topic name so tests never share state."""
    return f"gps.events.test.{uuid4().hex[:12]}"


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


async def consume_one(
    topic: str,
    bootstrap_servers: str,
    *,
    timeout_ms: int = 15_000,
) -> object:
    """Return the first record published to ``topic`` or fail the test."""
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
    )
    await consumer.start()
    try:
        remaining = timeout_ms
        while remaining > 0:
            step = min(1_000, remaining)
            batches = await consumer.getmany(timeout_ms=step)
            for records in batches.values():
                if records:
                    return records[0]
            remaining -= step
    finally:
        await consumer.stop()
    raise AssertionError(f"no message was consumed from topic {topic}")


async def delete_topic(topic: str, bootstrap_servers: str) -> None:
    """Best-effort cleanup of a per-test topic."""
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        await admin.start()
        await admin.delete_topics([topic])
    finally:
        await admin.close()


def test_api_returns_202_only_after_broker_acknowledgement() -> None:
    """End to end: validated event reaches Kafka before the API answers."""
    topic = unique_topic()
    settings = get_settings().model_copy(update={"kafka_enabled": True, "kafka_gps_topic": topic})
    try:
        with TestClient(create_app(settings)) as client:
            vehicle = create_vehicle(client)
            payload = {
                "event_id": str(uuid4()),
                "vehicle_id": vehicle["id"],
                "latitude": 52.52,
                "longitude": 13.405,
                "speed_kmph": 31.5,
                "heading": 87.5,
                "recorded_at": datetime(2026, 1, 1, 12, 0, tzinfo=UTC).isoformat(),
            }
            response = client.post("/api/v1/locations", json=payload)

            assert response.status_code == 202, response.text
            ack = response.json()
            assert ack["status"] == "accepted"
            assert ack["persisted"] is False

        # The application is shut down; the acked event must still be on the
        # broker (acks=all) keyed by vehicle_id.
        record = asyncio.run(consume_one(topic, settings.kafka_bootstrap_servers))
        assert record.key == vehicle["id"].encode("utf-8")  # type: ignore[attr-defined]
        body = json.loads(record.value)  # type: ignore[attr-defined]
        assert body["event_id"] == payload["event_id"]
        assert body["vehicle_id"] == vehicle["id"]
        assert body["latitude"] == 52.52
    finally:
        asyncio.run(delete_topic(topic, settings.kafka_bootstrap_servers))


@pytest.mark.asyncio
async def test_publisher_round_trip_preserves_key_and_json_payload() -> None:
    """The producer abstraction alone: key, JSON body and acknowledgement."""
    settings = get_settings()
    topic = unique_topic()
    publisher = KafkaGpsEventPublisher(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=topic,
        partitions=3,
        replication_factor=1,
        delivery_timeout_seconds=15.0,
    )
    try:
        await publisher.start()
        event = make_event()
        await publisher.publish(event)

        record = await consume_one(topic, settings.kafka_bootstrap_servers)
        assert record.topic == topic  # type: ignore[attr-defined]
        assert record.key == str(event.vehicle_id).encode("utf-8")  # type: ignore[attr-defined]
        body = json.loads(record.value)  # type: ignore[attr-defined]
        assert body == event.model_dump(mode="json")
    finally:
        await publisher.close()
        await delete_topic(topic, settings.kafka_bootstrap_servers)


@pytest.mark.asyncio
async def test_topic_is_created_with_configured_partitions() -> None:
    """Explicit topic creation with the configured partition count."""
    settings: Settings = get_settings()
    topic = unique_topic()
    publisher = KafkaGpsEventPublisher(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=topic,
        partitions=4,
        replication_factor=1,
        delivery_timeout_seconds=15.0,
    )
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    try:
        await publisher.start()

        await admin.start()
        listed = await admin.list_topics()
        assert topic in listed

        described = await admin.describe_topics([topic])
        assert described, f"topic {topic} not found in metadata"
        assert len(described[0]["partitions"]) == 4
    finally:
        await admin.close()
        await publisher.close()
        await delete_topic(topic, settings.kafka_bootstrap_servers)
