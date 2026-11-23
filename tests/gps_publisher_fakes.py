"""In-memory doubles for the GPS event publisher protocol.

Used by unit tests and by the PostgreSQL integration suite so neither needs
a running Kafka broker.
"""

from app.schemas.gps import GpsEvent
from app.services.gps_publisher import GpsPublishError


class RecordingGpsEventPublisher:
    """Record every published event; optionally fail on demand."""

    def __init__(self, *, fail_with: GpsPublishError | None = None) -> None:
        self.events: list[GpsEvent] = []
        self.started = False
        self.closed = False
        self.fail_with = fail_with

    async def publish(self, event: GpsEvent) -> None:
        if self.fail_with is not None:
            raise self.fail_with
        self.events.append(event)

    async def start(self) -> None:
        self.started = True

    async def close(self) -> None:
        self.closed = True
