"""Publisher contract for validated GPS events.

The protocol lives in the service layer so GPS business validation depends
only on "deliver this event", never on Kafka specifics. The Kafka
implementation lives in :mod:`app.infrastructure.kafka` and can be swapped
for an in-memory test double without touching the service or the API.
"""

from typing import Protocol

from app.schemas.gps import GpsEvent


class GpsPublishError(Exception):
    """Raised when a validated GPS event could not be delivered.

    Messages must stay safe for server-side logs: no bootstrap servers,
    credentials or raw broker internals.
    """


class GpsEventPublisher(Protocol):
    """Deliver one validated GPS event to the ingestion pipeline."""

    async def publish(self, event: GpsEvent) -> None:
        """Publish ``event`` or raise :class:`GpsPublishError`."""
        ...

    async def start(self) -> None:
        """Acquire resources (connect, ensure topics). Idempotent."""
        ...

    async def close(self) -> None:
        """Release resources. Safe to call more than once."""
        ...


class UnavailableGpsEventPublisher:
    """Placeholder used when Kafka is disabled; rejects every publication.

    The API therefore answers ``503`` instead of pretending an event was
    accepted when no delivery pipeline exists.
    """

    async def publish(self, event: GpsEvent) -> None:
        del event
        raise GpsPublishError("Kafka publishing is disabled")

    async def start(self) -> None:
        """Nothing to start: this publisher holds no connections."""

    async def close(self) -> None:
        """Nothing to close: this publisher holds no connections."""
