"""FastAPI application entry point."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.errors import register_exception_handlers
from app.api.routes import assignments, drivers, health, locations, root, shipments, vehicles
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.infrastructure.database import create_engine, create_session_factory
from app.infrastructure.kafka import KafkaGpsEventPublisher
from app.infrastructure.redis import RedisClient, RedisUnavailableError
from app.services.gps_publisher import (
    GpsEventPublisher,
    GpsPublishError,
    UnavailableGpsEventPublisher,
)

logger = logging.getLogger(__name__)

API_DESCRIPTION = (
    "FleetPulse is a real-time fleet and delivery intelligence platform. "
    "This milestone provides REST APIs and business logic for vehicles, "
    "drivers, shipments and historical vehicle assignments."
)


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build and configure the FastAPI application."""
    resolved_settings = settings or get_settings()
    configure_logging(resolved_settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(resolved_settings)
        app.state.db_engine = engine
        app.state.db_session_factory = create_session_factory(engine)
        logger.info("Database engine initialized")

        publisher: GpsEventPublisher
        if resolved_settings.kafka_enabled:
            kafka_publisher = KafkaGpsEventPublisher(
                bootstrap_servers=resolved_settings.kafka_bootstrap_servers,
                topic=resolved_settings.kafka_gps_topic,
                partitions=resolved_settings.kafka_gps_partitions,
                replication_factor=resolved_settings.kafka_gps_replication_factor,
                delivery_timeout_seconds=resolved_settings.kafka_delivery_timeout_seconds,
            )
            try:
                await kafka_publisher.start()
            except GpsPublishError as exc:
                # Kafka is required here: fail startup loudly but without
                # leaking connection details into the message.
                await engine.dispose()
                logger.error(
                    "Kafka GPS producer startup failed (%s); "
                    "verify KAFKA_BOOTSTRAP_SERVERS and 'docker compose up -d kafka'",
                    type(exc).__name__,
                )
                raise RuntimeError(
                    "Kafka is required but the GPS producer failed to start."
                ) from exc
            publisher = kafka_publisher
        else:
            publisher = UnavailableGpsEventPublisher()
            logger.info("Kafka disabled: GPS ingestion will answer 503")
        app.state.gps_publisher = publisher

        # Redis is a rebuildable live-state projection. The client is created
        # once per process and shared; no connection is opened per request.
        # When disabled (unit tests), no readiness check is attempted but the
        # lazily connecting client still exists for dependency injection.
        redis = RedisClient(resolved_settings)
        app.state.redis_client = redis.client
        if resolved_settings.redis_enabled:
            try:
                await redis.start()
            except RedisUnavailableError as exc:
                await redis.close()
                await publisher.close()
                await engine.dispose()
                logger.error(
                    "Redis startup failed (%s); verify REDIS_URL and 'docker compose up -d redis'",
                    type(exc).__name__,
                )
                raise RuntimeError(
                    "Redis is required but the live-location client failed to start."
                ) from exc
        else:
            logger.info("Redis disabled: live-location startup check skipped")

        try:
            yield
        finally:
            await redis.close()
            await publisher.close()
            await engine.dispose()
            logger.info("Database engine disposed")

    app = FastAPI(
        title=resolved_settings.app_name,
        description=API_DESCRIPTION,
        version=resolved_settings.app_version,
        docs_url="/docs" if resolved_settings.is_development else None,
        redoc_url="/redoc" if resolved_settings.is_development else None,
        openapi_url="/openapi.json" if resolved_settings.is_development else None,
        lifespan=lifespan,
    )

    register_exception_handlers(app)

    app.include_router(root.router)
    app.include_router(health.router)
    app.include_router(vehicles.router, prefix="/api/v1")
    app.include_router(drivers.router, prefix="/api/v1")
    app.include_router(shipments.router, prefix="/api/v1")
    app.include_router(assignments.router, prefix="/api/v1")
    app.include_router(locations.router, prefix="/api/v1")

    return app


app = create_app()
