"""Geospatial functions."""

from __future__ import annotations

from daft.expressions import Expression


def great_circle_distance(lat1: Expression, lon1: Expression, lat2: Expression, lon2: Expression) -> Expression:
    """Compute the great circle distance between two points on the Earth.

    Invalid inputs including nulls, non-finite values, or out-of-range coordinates
    (lat ∈ [-90, 90], lon ∈ [-180, 180]) produce null outputs.

    Args:
        lat1: Latitude of the first point in degrees.
        lon1: Longitude of the first point in degrees.
        lat2: Latitude of the second point in degrees.
        lon2: Longitude of the second point in degrees.

    Returns:
        Great circle distance in meters between the two points.

    Examples:
        >>> import daft
        >>> from daft.functions import great_circle_distance
        >>> df = daft.from_pydict({"lat1": [0.0], "lon1": [0.0], "lat2": [0.0], "lon2": [1.0]})
        >>> df.select(great_circle_distance(df["lat1"], df["lon1"], df["lat2"], df["lon2"]).alias("meters")).show()
    """
    return Expression._call_builtin_scalar_fn("great_circle_distance", lat1, lon1, lat2, lon2)
