"""Async simulation loop with deterministic ticks and graceful shutdown."""

import asyncio
import contextlib
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from simulator.generator import GpsEventGenerator
from simulator.transport import EventPublisher, TransportError

logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    """Return the current timezone-aware UTC time."""
    return datetime.now(UTC)


@dataclass(frozen=True)
class SimulationStats:
    """Summary of one completed simulation run."""

    ticks: int
    events_sent: int
    events_failed: int
    started_at: datetime
    finished_at: datetime


class SimulationRunner:
    """Drive a generator and publisher until the duration expires or stop is requested."""

    def __init__(
        self,
        generator: GpsEventGenerator,
        publisher: EventPublisher,
        *,
        interval_seconds: float,
        duration_seconds: float = 0.0,
        stop_event: asyncio.Event | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if duration_seconds < 0:
            raise ValueError("duration_seconds must not be negative")

        self._generator = generator
        self._publisher = publisher
        self._interval_seconds = interval_seconds
        self._duration_seconds = duration_seconds
        self._stop_event = stop_event if stop_event is not None else asyncio.Event()
        self._clock = clock

    async def run(self) -> SimulationStats:
        """Run the simulation and always close the publisher.

        ``duration_seconds=0`` means "run until stopped". A stop request is
        observed between ticks, so an in-flight tick is completed and the
        publisher is closed before the coroutine returns.
        """
        started_at = self._clock()
        previous_tick = started_at
        ticks = 0
        events_sent = 0
        events_failed = 0

        try:
            while not self._stop_event.is_set():
                now = self._clock()
                if (
                    self._duration_seconds > 0
                    and (now - started_at).total_seconds() >= self._duration_seconds
                ):
                    break

                elapsed_seconds = max(0.0, (now - previous_tick).total_seconds())
                for event in self._generator.generate(now, elapsed_seconds):
                    try:
                        await self._publisher.publish(event)
                    except TransportError as exc:
                        events_failed += 1
                        logger.warning(
                            "Failed to publish GPS event for vehicle %s: %s",
                            event.vehicle_id,
                            exc,
                        )
                    else:
                        events_sent += 1

                ticks += 1
                previous_tick = now

                if self._stop_event.is_set():
                    break

                timeout = self._next_timeout(started_at)
                if timeout <= 0:
                    break

                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stop_event.wait(), timeout=timeout)
        finally:
            await self._publisher.close()

        return SimulationStats(
            ticks=ticks,
            events_sent=events_sent,
            events_failed=events_failed,
            started_at=started_at,
            finished_at=self._clock(),
        )

    def _next_timeout(self, started_at: datetime) -> float:
        if self._duration_seconds <= 0:
            return self._interval_seconds

        remaining = self._duration_seconds - (self._clock() - started_at).total_seconds()
        return min(self._interval_seconds, remaining)
