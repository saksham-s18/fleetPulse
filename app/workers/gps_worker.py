"""Standalone Kafka GPS consumer worker.

Run it with::

    python -m app.workers.gps_worker

The worker is deliberately independent from the FastAPI process: it is never
started from the application lifespan. It joins ``KAFKA_CONSUMER_GROUP_ID``,
processes ``KAFKA_GPS_TOPIC`` sequentially and dead-letters messages that can
never be processed to ``KAFKA_GPS_DLQ_TOPIC``. ``Ctrl+C`` / ``SIGTERM`` request
a graceful stop after the record in flight has been finished and committed.
"""

import argparse
import asyncio
import logging
import signal
from collections.abc import Callable, Sequence
from typing import Any

from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.infrastructure.kafka_consumer import (
    KafkaDeadLetterPublisher,
    KafkaGpsConsumer,
)
from app.infrastructure.redis import RedisClient, RedisUnavailableError
from app.repositories.live_location import LiveLocationRepository
from app.services.gps_consumer import (
    GpsConsumerError,
    GpsConsumerService,
    InMemoryEventDeduplicator,
)
from app.services.live_location import LiveLocationService
from app.services.live_location_handler import LiveLocationGpsEventHandler

logger = logging.getLogger(__name__)

LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


def build_parser() -> argparse.ArgumentParser:
    """Build the worker argument parser."""
    parser = argparse.ArgumentParser(
        prog="python -m app.workers.gps_worker",
        description=(
            "Consume validated GPS events from Kafka, process them sequentially "
            "and dead-letter messages that cannot be handled."
        ),
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=0.0,
        dest="duration_seconds",
        metavar="SECONDS",
        help="stop gracefully after N seconds; 0 runs until interrupted (default: 0)",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        choices=LOG_LEVELS,
        help="override LOG_LEVEL from settings",
    )
    return parser


def _install_shutdown_handlers(stop_event: asyncio.Event) -> Callable[[], None]:
    """Request a graceful stop on SIGINT or SIGTERM.

    ``loop.add_signal_handler`` is unavailable on Windows, so the function falls
    back to the :mod:`signal` module. The returned callable restores every
    handler it changed.
    """
    loop = asyncio.get_running_loop()
    loop_handlers: list[signal.Signals] = []
    fallback_handlers: dict[signal.Signals, Any] = {}

    for signal_name in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signal_name, stop_event.set)
        except (NotImplementedError, RuntimeError, ValueError):
            try:
                fallback_handlers[signal_name] = signal.getsignal(signal_name)
                signal.signal(
                    signal_name,
                    lambda *_: loop.call_soon_threadsafe(stop_event.set),
                )
            except (OSError, ValueError):
                logger.debug("Could not install a handler for %s", signal_name)
        else:
            loop_handlers.append(signal_name)

    def restore() -> None:
        for signal_name in loop_handlers:
            loop.remove_signal_handler(signal_name)
        for signal_name, handler in fallback_handlers.items():
            signal.signal(signal_name, handler)

    return restore


def build_worker(
    settings: Settings,
) -> tuple[
    KafkaGpsConsumer,
    KafkaDeadLetterPublisher,
    GpsConsumerService,
    RedisClient,
]:
    """Wire Kafka transport, Redis live state and the processing service.

    Exactly one ``RedisClient`` and one connection pool are created for the
    worker process. The client is returned so the worker can start (ping) and
    close it independently of the FastAPI lifespan.
    """
    dead_letters = KafkaDeadLetterPublisher(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_gps_dlq_topic,
        partitions=settings.kafka_gps_partitions,
        replication_factor=settings.kafka_gps_replication_factor,
        delivery_timeout_seconds=settings.kafka_delivery_timeout_seconds,
    )
    redis_client = RedisClient(settings)
    live_location = LiveLocationService(
        LiveLocationRepository(
            redis_client.client,
            prefix=settings.redis_key_prefix,
        ),
        stale_after_seconds=settings.redis_location_stale_after_seconds,
    )
    service = GpsConsumerService(
        handler=LiveLocationGpsEventHandler(live_location),
        dead_letter_publisher=dead_letters,
        deduplicator=InMemoryEventDeduplicator(),
        max_attempts=settings.kafka_max_processing_attempts,
        retry_delay_seconds=settings.kafka_retry_delay_seconds,
    )
    consumer = KafkaGpsConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.kafka_gps_topic,
        group_id=settings.kafka_consumer_group_id,
        partitions=settings.kafka_gps_partitions,
        replication_factor=settings.kafka_gps_replication_factor,
        poll_timeout_seconds=settings.kafka_consumer_poll_timeout_seconds,
        service=service,
    )
    return consumer, dead_letters, service, redis_client


async def _run(args: argparse.Namespace, settings: Settings) -> int:
    consumer, dead_letters, service, redis_client = build_worker(settings)
    stop_event = asyncio.Event()
    restore_handlers = _install_shutdown_handlers(stop_event)

    duration_timer: asyncio.TimerHandle | None = None
    if args.duration_seconds > 0:
        loop = asyncio.get_running_loop()
        duration_timer = loop.call_later(args.duration_seconds, stop_event.set)

    logger.info(
        "GPS worker starting: topic=%s group=%s dlq=%s attempts=%d redis_prefix=%s",
        settings.kafka_gps_topic,
        settings.kafka_consumer_group_id,
        settings.kafka_gps_dlq_topic,
        settings.kafka_max_processing_attempts,
        settings.redis_key_prefix,
    )

    exit_code = 0
    try:
        # Redis must be ready before the first record is fetched: a missing
        # live-state store is a startup failure, not a per-message surprise.
        await redis_client.start()
        await dead_letters.start()
        await consumer.start()
        await consumer.run(stop_event)
    except GpsConsumerError as exc:
        logger.error("GPS worker stopped after a failure: %s", exc)
        exit_code = 1
    except RedisUnavailableError as exc:
        logger.error(
            "GPS worker cannot start because Redis is unavailable (%s); "
            "verify REDIS_URL and 'docker compose up -d redis'",
            type(exc).__name__,
        )
        exit_code = 1
    finally:
        if duration_timer is not None:
            duration_timer.cancel()
        await consumer.stop()
        await dead_letters.close()
        await redis_client.close()
        restore_handlers()
        logger.info("GPS worker finished: %s", service.stats.summary())

    return exit_code


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments, run the worker and return a process exit code."""
    args = build_parser().parse_args(argv)
    settings = get_settings()
    configure_logging(args.log_level or settings.log_level)

    if not settings.kafka_enabled:
        logger.error("KAFKA_ENABLED=false: the GPS consumer worker requires Kafka")
        return 1
    if not settings.redis_enabled:
        logger.error("REDIS_ENABLED=false: the GPS consumer worker requires Redis")
        return 1

    try:
        return asyncio.run(_run(args, settings))
    except KeyboardInterrupt:
        logger.info("GPS worker interrupted")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
