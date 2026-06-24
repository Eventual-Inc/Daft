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
        >>> df = df.select(great_circle_distance(df["lat1"], df["lon1"], df["lat2"], df["lon2"]).alias("meters"))
        >>> df.show()  # doctest: +SKIP
    """
    return Expression._call_builtin_scalar_fn("great_circle_distance", lat1, lon1, lat2, lon2)


# ── Unary geometry functions ────────────────────────────────────────────────


def st_area(geom: Expression) -> Expression:
    """Return the 2D area of a geometry.

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

    Returns:
        Float64 column with the unsigned area in coordinate-system units squared.
        Returns null for null or unparseable geometries.

    Examples:
        >>> import daft
        >>> from daft.functions.spatial import st_area
        >>> df = daft.from_pydict({"geom": [b"...wkb..."]})  # doctest: +SKIP
        >>> df.select(st_area(df["geom"])).show()  # doctest: +SKIP
    """
    return Expression._call_builtin_scalar_fn("st_area", geom)


def st_length(geom: Expression) -> Expression:
    """Return the Euclidean length of a geometry.

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

    Returns:
        Float64 column. For points returns 0, for polygons returns perimeter length.
    """
    return Expression._call_builtin_scalar_fn("st_length", geom)


def st_isvalid(geom: Expression) -> Expression:
    """Return whether a geometry is topologically valid according to OGC rules.

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

    Returns:
        Boolean column.
    """
    return Expression._call_builtin_scalar_fn("st_isvalid", geom)


def st_geometrytype(geom: Expression) -> Expression:
    """Return the geometry type name as a string.

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

    Returns:
        Utf8 column with values like ``'Point'``, ``'LineString'``, ``'Polygon'``, etc.
    """
    return Expression._call_builtin_scalar_fn("st_geometrytype", geom)


def st_x(geom: Expression) -> Expression:
    """Return the X (longitude) coordinate of a Point geometry.

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

    Returns:
        Float64 column. Returns null for non-point geometries.
    """
    return Expression._call_builtin_scalar_fn("st_x", geom)


def st_y(geom: Expression) -> Expression:
    """Return the Y (latitude) coordinate of a Point geometry.

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

    Returns:
        Float64 column. Returns null for non-point geometries.
    """
    return Expression._call_builtin_scalar_fn("st_y", geom)


def st_centroid(geom: Expression) -> Expression:
    """Return the geometric centroid (center of mass) of a geometry as a Point.

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

    Returns:
        Geometry column containing Point geometries.
    """
    return Expression._call_builtin_scalar_fn("st_centroid", geom)


# ── Binary geometry predicates ─────────────────────────────────────────────


def st_contains(geom_a: Expression, geom_b: Expression) -> Expression:
    """Return whether geometry A completely contains geometry B.

    Args:
        geom_a: Container geometry (Geometry or Binary WKB column).
        geom_b: Contained geometry (Geometry or Binary WKB column).

    Returns:
        Boolean column.
    """
    return Expression._call_builtin_scalar_fn("st_contains", geom_a, geom_b)


def st_intersects(geom_a: Expression, geom_b: Expression) -> Expression:
    """Return whether geometry A and geometry B spatially intersect.

    Args:
        geom_a: First geometry column.
        geom_b: Second geometry column (supports scalar broadcast).

    Returns:
        Boolean column.
    """
    return Expression._call_builtin_scalar_fn("st_intersects", geom_a, geom_b)


def st_within(geom_a: Expression, geom_b: Expression) -> Expression:
    """Return whether geometry A is completely within geometry B.

    Args:
        geom_a: Inner geometry column.
        geom_b: Container geometry column.

    Returns:
        Boolean column.
    """
    return Expression._call_builtin_scalar_fn("st_within", geom_a, geom_b)


def st_distance(geom_a: Expression, geom_b: Expression) -> Expression:
    """Return the minimum Euclidean distance between two geometries.

    Args:
        geom_a: First geometry column.
        geom_b: Second geometry column (supports scalar broadcast).

    Returns:
        Float64 column.
    """
    return Expression._call_builtin_scalar_fn("st_distance", geom_a, geom_b)


def st_geomfromtext(wkt: Expression) -> Expression:
    """Parse a Well-Known Text (WKT) string and return a WKB geometry."""
    return Expression._call_builtin_scalar_fn("st_geomfromtext", wkt)


def st_touches(geom_a: Expression, geom_b: Expression) -> Expression:
    """Return true where A and B share a boundary but their interiors do not intersect."""
    return Expression._call_builtin_scalar_fn("st_touches", geom_a, geom_b)


def st_crosses(geom_a: Expression, geom_b: Expression) -> Expression:
    """Return true where A and B cross."""
    return Expression._call_builtin_scalar_fn("st_crosses", geom_a, geom_b)


def st_overlaps(geom_a: Expression, geom_b: Expression) -> Expression:
    """Return true where A and B overlap (same dimension, partial intersection)."""
    return Expression._call_builtin_scalar_fn("st_overlaps", geom_a, geom_b)


def st_disjoint(geom_a: Expression, geom_b: Expression) -> Expression:
    """Return true where A and B share no points."""
    return Expression._call_builtin_scalar_fn("st_disjoint", geom_a, geom_b)


def st_equals(geom_a: Expression, geom_b: Expression) -> Expression:
    """Return true where A and B are topologically equal."""
    return Expression._call_builtin_scalar_fn("st_equals", geom_a, geom_b)


# ── Geometry-producing functions ────────────────────────────────────────────


def st_buffer(geom: Expression, distance: float) -> Expression:
    """Return a geometry expanded by the given distance.

    For Point geometries this returns a circular polygon approximation.
    For other types this returns the bounding-box envelope expanded by
    ``distance`` in each direction.

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).
        distance: Buffer distance in the same units as the geometry's coordinate system.

    Returns:
        Geometry column.
    """
    return Expression._call_builtin_scalar_fn("st_buffer", geom, distance)


# ── Geohash functions ────────────────────────────────────────────────────────


def st_geohash(geom: Expression, precision: int = 5) -> Expression:
    """Return the geohash of a geometry's centroid.

    Geohash is a hierarchical spatial string encoding. Shorter strings cover
    larger areas; each additional character adds roughly 5× precision.

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).
        precision: Geohash precision (1–12). Default is 5 (~5 km resolution).

    Returns:
        Utf8 column with geohash strings.

    Notes:
        Adding a ``_geohash`` column alongside a geometry column enables
        automatic geohash-based partition pruning for spatial predicates.

    Examples:
        >>> import daft
        >>> from daft.functions.spatial import st_geohash
        >>> df = daft.from_pydict({"geom": [b"...wkb..."]})  # doctest: +SKIP
        >>> df.select(st_geohash(df["geom"], precision=6))  # doctest: +SKIP
    """
    return Expression._call_builtin_scalar_fn("st_geohash", geom, precision)
