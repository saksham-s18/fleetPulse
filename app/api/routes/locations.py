"""GPS location ingestion REST endpoint."""

from fastapi import APIRouter, status

from app.api.dependencies import GpsIngestServiceDep
from app.schemas.gps import GpsEvent, GpsIngestAck

router = APIRouter(prefix="/locations", tags=["locations"])


@router.post(
    "",
    response_model=GpsIngestAck,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest a GPS event",
)
async def ingest_location(
    payload: GpsEvent,
    service: GpsIngestServiceDep,
) -> GpsIngestAck:
    """Validate a GPS event and accept it through the temporary HTTP path.

    A ``202`` response means the event passed validation; in this milestone it
    is not durably stored (no GPS tables or broker exist yet).
    """
    return await service.ingest(payload)
