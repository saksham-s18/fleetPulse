"""Deterministic GPS event generation for simulated vehicles."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from math import isfinite
from random import Random
from uuid import UUID, uuid4

from app.schemas.gps import GpsEvent
from simulator.routes import DEFAULT_ROUTES, Route

SPEED_SMOOTHING = 0.35
COORDINATE_PRECISION = 6
SPEED_PRECISION = 2
HEADING_PRECISION = 1


@dataclass
class _VehicleState:
    vehicle_id: UUID
    route: Route
    distance_km: float
    speed_kmph: float


class GpsEventGenerator:
    """Generate one GPS event per simulated vehicle and tick.

    Movement and speed variation are driven by a single :class:`random.Random`
    instance, so two generators created with the same seed and the same vehicle
    order produce identical coordinates, speeds, headings and timestamps. Event
    IDs use :func:`uuid.uuid4` and are intentionally *not* reproducible: running
    the same simulation twice must not reuse IDs that an ingestion system may
    already have seen.
    """

    def __init__(
        self,
        vehicle_ids: Sequence[UUID],
        *,
        seed: int | None = None,
        routes: Sequence[Route] | None = None,
        rng: Random | None = None,
        event_id_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        unique_ids = list(dict.fromkeys(vehicle_ids))
        if not unique_ids:
            raise ValueError("at least one vehicle ID is required")
        if rng is not None and seed is not None:
            raise ValueError("pass either seed or rng, not both")

        available_routes = list(routes) if routes is not None else list(DEFAULT_ROUTES)
        if not available_routes:
            raise ValueError("at least one route is required")

        self._rng = rng if rng is not None else Random(seed)
        self._event_id_factory = event_id_factory

        shuffled_routes = list(available_routes)
        self._rng.shuffle(shuffled_routes)

        self._states: list[_VehicleState] = []
        for index, vehicle_id in enumerate(unique_ids):
            route = shuffled_routes[index % len(shuffled_routes)]
            self._states.append(
                _VehicleState(
                    vehicle_id=vehicle_id,
                    route=route,
                    distance_km=self._rng.uniform(0.0, route.length_km),
                    speed_kmph=self._rng.uniform(
                        route.min_speed_kmph,
                        route.max_speed_kmph,
                    ),
                )
            )

    @property
    def vehicle_ids(self) -> tuple[UUID, ...]:
        """Return the simulated vehicle IDs in a stable order."""
        return tuple(state.vehicle_id for state in self._states)

    @property
    def routes_by_vehicle(self) -> dict[UUID, Route]:
        """Return the route assigned to each simulated vehicle."""
        return {state.vehicle_id: state.route for state in self._states}

    def generate(self, recorded_at: datetime, elapsed_seconds: float) -> list[GpsEvent]:
        """Advance every vehicle and return their GPS events.

        ``elapsed_seconds`` is the time since the previous tick. It is explicit
        rather than derived from the wall clock so simulations are deterministic
        and straightforward to test.
        """
        if not isfinite(elapsed_seconds) or elapsed_seconds < 0:
            raise ValueError("elapsed_seconds must be a finite, non-negative number")

        events: list[GpsEvent] = []
        for state in self._states:
            state.speed_kmph = self._next_speed(state)
            state.distance_km = (
                state.distance_km + state.speed_kmph * elapsed_seconds / 3600
            ) % state.route.length_km
            position = state.route.position_at(state.distance_km)

            events.append(
                GpsEvent(
                    event_id=self._event_id_factory(),
                    vehicle_id=state.vehicle_id,
                    latitude=round(position.latitude, COORDINATE_PRECISION),
                    longitude=round(position.longitude, COORDINATE_PRECISION),
                    speed_kmph=round(state.speed_kmph, SPEED_PRECISION),
                    heading=round(position.heading_degrees, HEADING_PRECISION),
                    recorded_at=recorded_at,
                )
            )

        return events

    def _next_speed(self, state: _VehicleState) -> float:
        """Smoothly vary speed inside the route's plausible range."""
        route = state.route
        target = self._rng.uniform(route.min_speed_kmph, route.max_speed_kmph)
        next_speed = state.speed_kmph + (target - state.speed_kmph) * SPEED_SMOOTHING
        return min(max(next_speed, route.min_speed_kmph), route.max_speed_kmph)
