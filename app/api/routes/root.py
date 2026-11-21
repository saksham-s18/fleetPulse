"""Root endpoint exposing basic API information."""

from fastapi import APIRouter

from app.core.config import get_settings
from app.schemas.system import RootResponse

router = APIRouter(tags=["system"])


@router.get(
    "/",
    response_model=RootResponse,
    summary="API information",
    description="Return the service name, version, environment and documentation URL.",
)
def read_root() -> RootResponse:
    """Return basic information about this API instance."""
    settings = get_settings()
    return RootResponse(
        name=settings.app_name,
        version=settings.app_version,
        environment=settings.app_env,
        docs_url="/docs" if settings.is_development else None,
    )
