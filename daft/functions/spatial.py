"""Spatial functions for vector inputs."""

from __future__ import annotations

from daft.expressions import Expression


def great_circle_distance(lat1: Expression, lon1: Expression, lat2: Expression, lon2: Expression) -> Expression:
    """Compute the great circle distance between two points on the Earth.

    Args:
        lat1: Latitude of the first point in degrees.
        lon1: Longitude of the first point in degrees.
        lat2: Latitude of the second point in degrees.
        lon2: Longitude of the second point in degrees.

    Returns:
        Great circle distance in meters between the two points.
    """
    return Expression._call_builtin_scalar_fn("great_circle_distance", lat1, lon1, lat2, lon2)
