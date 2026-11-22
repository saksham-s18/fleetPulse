"""Geographic helpers used to move simulated vehicles along routes."""

from math import asin, atan2, cos, degrees, radians, sin, sqrt

EARTH_RADIUS_KM = 6371.0088


def haversine_km(
    latitude_a: float,
    longitude_a: float,
    latitude_b: float,
    longitude_b: float,
) -> float:
    """Return the great-circle distance between two points in kilometres."""
    latitude_a_rad = radians(latitude_a)
    latitude_b_rad = radians(latitude_b)
    delta_latitude = latitude_b_rad - latitude_a_rad
    delta_longitude = radians(longitude_b - longitude_a)

    a = (
        sin(delta_latitude / 2) ** 2
        + cos(latitude_a_rad) * cos(latitude_b_rad) * sin(delta_longitude / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * asin(sqrt(min(1.0, a)))


def initial_bearing_degrees(
    latitude_a: float,
    longitude_a: float,
    latitude_b: float,
    longitude_b: float,
) -> float:
    """Return the initial compass bearing from point A to point B.

    The result is in the range ``[0, 360)``. Two identical points return ``0``.
    """
    latitude_a_rad = radians(latitude_a)
    latitude_b_rad = radians(latitude_b)
    delta_longitude = radians(longitude_b - longitude_a)

    y = sin(delta_longitude) * cos(latitude_b_rad)
    x = cos(latitude_a_rad) * sin(latitude_b_rad) - sin(latitude_a_rad) * cos(latitude_b_rad) * cos(
        delta_longitude
    )
    return (degrees(atan2(y, x)) + 360) % 360
