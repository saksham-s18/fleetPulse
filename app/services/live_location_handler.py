"""GPS event handler that projects validated events into Redis live state.

This is the Milestone 6.2 bridge between the transport-neutral consumer
pipeline and the Milestone 6.1 :class:`~app.services.live_location.LiveLocationService`.
It does not duplicate the Redis key model or Lua script: the service performs
the atomic compare-and-set and this handler only maps its outcome onto the
consumer's statistics contract.

A Redis failure is deliberately raised as :class:`GpsHandlerError` with
``failure_kind="redis"``. The consumer therefore treats it as retryable (never
as an invalid GPS payload), leaves the Kafka offset uncommitted, and eventually
quarantines the original message through the existing DLQ policy.
"""

import logging

from app.schemas.gps import GpsEvent
from app.schemas.live_location import LocationUpdateOutcome
from app.services.gps_consumer import (
    GpsHandlerError,
    GpsHandlerOutcome,
    GpsHandlerResult,
)
from app.services.live_location import LiveLocationService, LiveLocationUnavailableError

logger = logging.getLogger(__name__)

_OUTCOME_MAP = {
    LocationUpdateOutcome.APPLIED: GpsHandlerOutcome.APPLIED,
    LocationUpdateOutcome.DUPLICATE: GpsHandlerOutcome.DUPLICATE,
    LocationUpdateOutcome.STALE: GpsHandlerOutcome.STALE,
}


class LiveLocationGpsEventHandler:
    """Project one validated GPS event into the Redis live-location state."""

    def __init__(self, service: LiveLocationService) -> None:
        self._service = service

    async def handle(self, event: GpsEvent) -> GpsHandlerResult:
        """Apply ``event``; raise a retryable error when Redis is unavailable."""
        try:
            result = await self._service.update_from_event(event)
        except LiveLocationUnavailableError as exc:
            # Keep Redis internals and credentials out of the message; the
            # original exception is retained only as a log-safe cause.
            logger.warning(
                "Live-location update failed for GPS event %s (vehicle %s, %s)",
                event.event_id,
                event.vehicle_id,
                type(exc).__name__,
            )
            raise GpsHandlerError(
                "live-location update is unavailable",
                failure_kind="redis",
            ) from exc

        outcome = _OUTCOME_MAP[result.outcome]
        logger.info(
            "Live-location %s for GPS event %s (vehicle %s, recorded_at=%s)",
            outcome.value,
            event.event_id,
            event.vehicle_id,
            event.recorded_at.isoformat(),
        )
        return GpsHandlerResult(outcome=outcome)
