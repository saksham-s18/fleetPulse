"""Predefined routes and interpolation for simulated vehicles."""

from dataclasses import dataclass, field
from itertools import pairwise
from math import isfinite

from simulator.geo import haversine_km, initial_bearing_degrees


@dataclass(frozen=True)
class RoutePosition:
    """A point on a route together with the heading of travel."""

    latitude: float
    longitude: float
    heading_degrees: float


@dataclass(frozen=True)
class Route:
    """A closed polyline with a realistic speed range.

    The last waypoint must equal the first one so that wrapping the travelled
    distance back to zero does not teleport the vehicle.
    """

    name: str
    waypoints: tuple[tuple[float, float], ...]
    min_speed_kmph: float
    max_speed_kmph: float
    cumulative_km: tuple[float, ...] = field(init=False, repr=False, compare=False)
    length_km: float = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if len(self.waypoints) < 2:
            raise ValueError("a route needs at least two waypoints")
        if self.min_speed_kmph < 0:
            raise ValueError("min_speed_kmph must not be negative")
        if self.max_speed_kmph < self.min_speed_kmph:
            raise ValueError("max_speed_kmph must be greater than or equal to min_speed_kmph")

        for latitude, longitude in self.waypoints:
            if not isfinite(latitude) or not isfinite(longitude):
                raise ValueError("waypoint coordinates must be finite")
            if not -90 <= latitude <= 90:
                raise ValueError("waypoint latitude must be between -90 and 90")
            if not -180 <= longitude <= 180:
                raise ValueError("waypoint longitude must be between -180 and 180")

        cumulative = [0.0]
        for (latitude_a, longitude_a), (latitude_b, longitude_b) in pairwise(self.waypoints):
            cumulative.append(
                cumulative[-1] + haversine_km(latitude_a, longitude_a, latitude_b, longitude_b)
            )

        if cumulative[-1] <= 0:
            raise ValueError("a route must have a non-zero length")

        object.__setattr__(self, "cumulative_km", tuple(cumulative))
        object.__setattr__(self, "length_km", cumulative[-1])

    def position_at(self, distance_km: float) -> RoutePosition:
        """Return the position and heading at a travelled distance."""
        if not isfinite(distance_km):
            raise ValueError("distance_km must be finite")

        distance = distance_km % self.length_km
        last_segment = len(self.waypoints) - 2

        for index in range(last_segment + 1):
            start = self.cumulative_km[index]
            end = self.cumulative_km[index + 1]
            if distance < end or index == last_segment:
                span = end - start
                fraction = 0.0 if span == 0 else (distance - start) / span
                latitude_a, longitude_a = self.waypoints[index]
                latitude_b, longitude_b = self.waypoints[index + 1]
                return RoutePosition(
                    latitude=latitude_a + (latitude_b - latitude_a) * fraction,
                    longitude=longitude_a + (longitude_b - longitude_a) * fraction,
                    heading_degrees=initial_bearing_degrees(
                        latitude_a,
                        longitude_a,
                        latitude_b,
                        longitude_b,
                    ),
                )

        raise RuntimeError("route interpolation failed")


DEFAULT_ROUTES: tuple[Route, ...] = (
    Route(
        name="Berlin City Loop",
        waypoints=(
            (52.5219, 13.4132),
            (52.5163, 13.3777),
            (52.5096, 13.3760),
            (52.5005, 13.4180),
            (52.5105, 13.4348),
            (52.5219, 13.4132),
        ),
        min_speed_kmph=20,
        max_speed_kmph=45,
    ),
    Route(
        name="Hamburg Harbour Circuit",
        waypoints=(
            (53.5511, 9.9937),
            (53.5458, 9.9694),
            (53.5390, 9.9350),
            (53.5320, 9.9480),
            (53.5413, 9.9842),
            (53.5511, 9.9937),
        ),
        min_speed_kmph=25,
        max_speed_kmph=50,
    ),
    Route(
        name="Munich Ring",
        waypoints=(
            (48.1374, 11.5755),
            (48.1520, 11.5450),
            (48.1630, 11.5860),
            (48.1450, 11.6250),
            (48.1210, 11.5980),
            (48.1374, 11.5755),
        ),
        min_speed_kmph=35,
        max_speed_kmph=70,
    ),
    Route(
        name="Cologne-Bonn Rhine Corridor",
        waypoints=(
            (50.9375, 6.9603),
            (50.8950, 6.9950),
            (50.8600, 7.0300),
            (50.8100, 7.0700),
            (50.7600, 7.0950),
            (50.7339, 7.0997),
            (50.7600, 7.0950),
            (50.8100, 7.0700),
            (50.8600, 7.0300),
            (50.8950, 6.9950),
            (50.9375, 6.9603),
        ),
        min_speed_kmph=70,
        max_speed_kmph=100,
    ),
    Route(
        name="Stuttgart Hills",
        waypoints=(
            (48.7758, 9.1829),
            (48.7900, 9.2200),
            (48.8100, 9.2000),
            (48.8000, 9.1500),
            (48.7758, 9.1829),
        ),
        min_speed_kmph=25,
        max_speed_kmph=55,
    ),
    Route(
        name="Leipzig Loop",
        waypoints=(
            (51.3397, 12.3731),
            (51.3500, 12.4100),
            (51.3300, 12.4300),
            (51.3150, 12.3950),
            (51.3397, 12.3731),
        ),
        min_speed_kmph=30,
        max_speed_kmph=65,
    ),
)
