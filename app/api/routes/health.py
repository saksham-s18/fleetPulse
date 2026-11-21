"""Health probe endpoints used by orchestrators and load balancers."""

from typing import Annotated

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy.ext.asyncio import AsyncEngine

from app.infrastructure.database import check_database, get_database_engine
from app.schemas.health import LivenessResponse, ReadinessResponse

router = APIRouter(prefix="/health", tags=["health"])


@router.get(
    "/live",
    response_model=LivenessResponse,
    summary="Liveness probe",
    description="Report that the application process is running.",
)
def liveness() -> LivenessResponse:
    """Return a successful response while the process is alive.

    This probe never touches PostgreSQL, so a database outage cannot cause the
    process to be restarted.
    """
    return LivenessResponse()


@router.get(
    "/ready",
    response_model=ReadinessResponse,
    summary="Readiness probe",
    description="Report that the application can reach PostgreSQL.",
    responses={
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": ReadinessResponse,
            "description": "PostgreSQL is unavailable.",
        }
    },
)
async def readiness(
    response: Response,
    engine: Annotated[AsyncEngine, Depends(get_database_engine)],
) -> ReadinessResponse:
    """Return readiness based on actual database connectivity."""
    if await check_database(engine):
        return ReadinessResponse(status="ready", checks={"database": "ok"})

    response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return ReadinessResponse(status="not_ready", checks={"database": "unavailable"})
