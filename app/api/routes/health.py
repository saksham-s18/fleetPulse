"""Health probe endpoints used by orchestrators and load balancers."""

from fastapi import APIRouter

from app.core.config import get_settings
from app.schemas.health import LivenessResponse, ReadinessResponse

router = APIRouter(prefix="/health", tags=["health"])


@router.get(
    "/live",
    response_model=LivenessResponse,
    summary="Liveness probe",
    description="Report that the application process is running.",
)
def liveness() -> LivenessResponse:
    """Return a successful response while the process is alive."""
    return LivenessResponse()


@router.get(
    "/ready",
    response_model=ReadinessResponse,
    summary="Readiness probe",
    description="Report that the application is ready to serve traffic.",
)
def readiness() -> ReadinessResponse:
    """Return readiness state and the checks backing it.

    No external dependencies (databases, brokers) exist yet. Configuration is
    validated when settings are loaded, so reaching this handler means it is
    safe to receive traffic.
    """
    settings = get_settings()
    return ReadinessResponse(checks={"configuration": f"ok ({settings.app_env})"})
