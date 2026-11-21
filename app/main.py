"""FastAPI application entry point."""

from fastapi import FastAPI

from app.api.routes import health, root
from app.core.config import get_settings
from app.core.logging import configure_logging

API_DESCRIPTION = (
    "FleetPulse is a real-time fleet and delivery intelligence platform. "
    "This milestone provides the application foundation and health probes."
)


def create_app() -> FastAPI:
    """Build and configure the FastAPI application."""
    settings = get_settings()
    configure_logging(settings.log_level)

    app = FastAPI(
        title=settings.app_name,
        description=API_DESCRIPTION,
        version=settings.app_version,
        docs_url="/docs" if settings.is_development else None,
        redoc_url="/redoc" if settings.is_development else None,
        openapi_url="/openapi.json" if settings.is_development else None,
    )

    app.include_router(root.router)
    app.include_router(health.router)

    return app


app = create_app()
