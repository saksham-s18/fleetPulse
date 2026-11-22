"""FastAPI application entry point."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.errors import register_exception_handlers
from app.api.routes import assignments, drivers, health, root, shipments, vehicles
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.infrastructure.database import create_engine, create_session_factory

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

        try:
            yield
        finally:
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

    return app


app = create_app()
