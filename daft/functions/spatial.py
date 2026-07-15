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


def st_area(geom: Expression, use_spheroid: bool = False) -> Expression:
    """Return the 2D area of a geometry.

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).
        use_spheroid: If True, compute geodesic area in WGS84 square meters (lon/lat input
            assumed). Default False uses planar (coordinate-system units squared).

    Returns:
        Float64 column with the unsigned area in coordinate-system units squared (planar)
        or WGS84 square meters (geodesic). Returns null for null or unparseable geometries.

    Examples:
        >>> import daft
        >>> from daft.functions.spatial import st_area
        >>> df = daft.from_pydict({"geom": [b"...wkb..."]})  # doctest: +SKIP
        >>> df.select(st_area(df["geom"])).show()  # doctest: +SKIP
    """
    if use_spheroid:
        return Expression._call_builtin_scalar_fn("st_area", geom, use_spheroid)
    return Expression._call_builtin_scalar_fn("st_area", geom)


def st_length(geom: Expression, use_spheroid: bool = False) -> Expression:
    """Return the length of line geometries.

    Supported geometry types: ``Line``, ``LineString``, ``MultiLineString``.
    All other types (``Point``, ``Polygon``, ``MultiPolygon``, etc.) return 0.0 —
    to obtain the geodesic perimeter of a polygon use ``st_area`` with
    ``use_spheroid=True`` (``GeodesicArea.geodesic_perimeter``).

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).
        use_spheroid: If True, compute geodesic length in WGS84 meters (lon/lat input
            assumed). Default False uses planar Euclidean length (coordinate units).

    Returns:
        Float64 column. Returns 0.0 for Points, Polygons, and all other non-line types.
        Planar (coordinate units) by default; WGS84 geodesic meters when use_spheroid=True.
    """
    if use_spheroid:
        return Expression._call_builtin_scalar_fn("st_length", geom, use_spheroid)
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


def st_distance(geom_a: Expression, geom_b: Expression, use_spheroid: bool = False) -> Expression:
    """Return the minimum distance between two geometries.

    Args:
        geom_a: First geometry column.
        geom_b: Second geometry column (supports scalar broadcast).
        use_spheroid: If True, compute WGS84 geodesic distance in meters (lon/lat point
            inputs assumed). Default False uses planar Euclidean distance (coordinate units).

    Returns:
        Float64 column. Planar by default; WGS84 geodesic meters when use_spheroid=True
        (point inputs only; other geometry pairs return NaN in geodesic mode).
    """
    if use_spheroid:
        return Expression._call_builtin_scalar_fn("st_distance", geom_a, geom_b, use_spheroid)
    return Expression._call_builtin_scalar_fn("st_distance", geom_a, geom_b)


def st_dwithin(geom_a: Expression, geom_b: Expression, distance: float) -> Expression:
    """Returns true if the planar distance between two geometries is <= ``distance`` (coordinate units).

    ``distance`` must be a numeric literal.
    """
    return Expression._call_builtin_scalar_fn("st_dwithin", geom_a, geom_b, distance)


def st_geomfromtext(wkt: Expression) -> Expression:
    """Parse a Well-Known Text (WKT) string into a Geometry.

    Args:
        wkt: A Utf8 column of WKT geometry strings (e.g. ``'POINT(1 2)'``,
            ``'POLYGON((0 0,1 0,1 1,0 1,0 0))'``).

    Returns:
        A ``DataType.geometry()`` column. Returns null for rows where the input is null
        or the WKT string cannot be parsed as a valid geometry.
    """
    return Expression._call_builtin_scalar_fn("st_geomfromtext", wkt)


def st_geomfromgeojson(geojson: Expression) -> Expression:
    """Parse a GeoJSON geometry or feature string into a Geometry.

    Args:
        geojson: A Utf8 column of GeoJSON strings (e.g. ``'{"type":"Point","coordinates":[1,2]}'``).

    Returns:
        A ``DataType.geometry()`` column. Returns null for rows where the input is null
        or the GeoJSON string cannot be parsed as a valid geometry or feature.
    """
    return Expression._call_builtin_scalar_fn("st_geomfromgeojson", geojson)


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


def st_covers(geom_a: Expression, geom_b: Expression) -> Expression:
    """Returns true if geometry A covers geometry B (no point of B is outside A; boundary included)."""
    return Expression._call_builtin_scalar_fn("st_covers", geom_a, geom_b)


def st_covered_by(geom_a: Expression, geom_b: Expression) -> Expression:
    """Returns true if geometry A is covered by geometry B (no point of A is outside B; boundary included)."""
    return Expression._call_builtin_scalar_fn("st_covered_by", geom_a, geom_b)


# ── Overlay (set-operation) functions ──────────────────────────────────────


def st_union(geom_a: Expression, geom_b: Expression) -> Expression:
    """Return the geometric union of two polygon geometries.

    Both operands must be Polygon or MultiPolygon; other types (e.g. Point,
    LineString) return null.

    Args:
        geom_a: First geometry column (Geometry or Binary WKB).
        geom_b: Second geometry column (Geometry or Binary WKB).

    Returns:
        Geometry column (MultiPolygon). Returns null for non-polygon inputs
        or if the underlying boolean operation raises an error.
    """
    return Expression._call_builtin_scalar_fn("st_union", geom_a, geom_b)


def st_intersection(geom_a: Expression, geom_b: Expression) -> Expression:
    """Return the geometric intersection of two polygon geometries.

    Both operands must be Polygon or MultiPolygon; other types return null.

    Args:
        geom_a: First geometry column (Geometry or Binary WKB).
        geom_b: Second geometry column (Geometry or Binary WKB).

    Returns:
        Geometry column (MultiPolygon). Returns null for non-polygon inputs.
    """
    return Expression._call_builtin_scalar_fn("st_intersection", geom_a, geom_b)


def st_difference(geom_a: Expression, geom_b: Expression) -> Expression:
    """Return the part of geometry A that does not intersect geometry B.

    Both operands must be Polygon or MultiPolygon; other types return null.

    Args:
        geom_a: First geometry column (Geometry or Binary WKB).
        geom_b: Second geometry column (Geometry or Binary WKB).

    Returns:
        Geometry column (MultiPolygon). Returns null for non-polygon inputs.
    """
    return Expression._call_builtin_scalar_fn("st_difference", geom_a, geom_b)


def st_symdifference(geom_a: Expression, geom_b: Expression) -> Expression:
    """Return the symmetric difference (XOR) of two polygon geometries.

    Returns the regions in either geometry but not in both.
    Both operands must be Polygon or MultiPolygon; other types return null.

    Args:
        geom_a: First geometry column (Geometry or Binary WKB).
        geom_b: Second geometry column (Geometry or Binary WKB).

    Returns:
        Geometry column (MultiPolygon). Returns null for non-polygon inputs.
    """
    return Expression._call_builtin_scalar_fn("st_symdifference", geom_a, geom_b)


# ── Geometry-producing functions ────────────────────────────────────────────


def st_envelope(geom: Expression) -> Expression:
    """Return the minimum bounding rectangle of a geometry as a Polygon.

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

    Returns:
        Geometry column containing the bounding-box polygon. Returns null for
        null or unparseable inputs, or for geometries with no extent.
    """
    return Expression._call_builtin_scalar_fn("st_envelope", geom)


def st_convexhull(geom: Expression) -> Expression:
    """Return the convex hull of a geometry as a Polygon.

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

    Returns:
        Geometry column containing the convex hull polygon. Returns null for
        null or unparseable inputs.
    """
    return Expression._call_builtin_scalar_fn("st_convexhull", geom)


def st_simplify(geom: Expression, tolerance: float) -> Expression:
    """Simplify a geometry using the Ramer–Douglas–Peucker algorithm.

    Applies to LineString, MultiLineString, Polygon, and MultiPolygon.
    Other geometry types (Point, MultiPoint, etc.) are returned unchanged.

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).
        tolerance: Simplification tolerance in the same units as the geometry's
            coordinate system. Larger values produce coarser simplification.

    Returns:
        Geometry column with simplified geometries.
    """
    return Expression._call_builtin_scalar_fn("st_simplify", geom, tolerance)


def st_astext(geom: Expression) -> Expression:
    """Return the Well-Known Text (WKT) representation of a geometry.

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

    Returns:
        Utf8 column with WKT strings.
    """
    return Expression._call_builtin_scalar_fn("st_astext", geom)


def st_geojsonfromgeom(geom: Expression) -> Expression:
    """Return the GeoJSON representation of a geometry.

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

    Returns:
        Utf8 column with GeoJSON strings.
    """
    return Expression._call_builtin_scalar_fn("st_geojsonfromgeom", geom)


def st_buffer(geom: Expression, distance: float) -> Expression:
    """Return a geometry that is the given distance from the input geometry (planar Cartesian).

    - **Point**: returns a 64-vertex circular polygon approximation (radius = distance).
    - **Polygon / MultiPolygon**: computes a real planar offset via straight-skeleton;
      falls back to bounding-box envelope expansion if the offset returns an empty result.
    - **LineString and all other types**: falls back to expanding the bounding-box envelope
      by ``distance`` in each direction (geo-buffer 0.2 has no line-buffering support).

    All buffer operations are planar (Cartesian). For geodesic buffers, project your
    coordinates to a local metric CRS before calling this function.

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).
        distance: Buffer distance in the same units as the geometry's coordinate system.

    Returns:
        Geometry column.
    """
    return Expression._call_builtin_scalar_fn("st_buffer", geom, distance)


# ── Geometry constructors ───────────────────────────────────────────────────


def st_point(x: Expression, y: Expression) -> Expression:
    """Construct a Point geometry from x and y coordinate columns.

    Args:
        x: Numeric column of X (longitude) coordinates.
        y: Numeric column of Y (latitude) coordinates.

    Returns:
        Geometry column containing Point geometries. Returns null for rows
        where either coordinate is null.
    """
    return Expression._call_builtin_scalar_fn("st_point", x, y)


def st_makeline(geom_a: Expression, geom_b: Expression) -> Expression:
    """Construct a LineString geometry from two Point geometries.

    Args:
        geom_a: First Point geometry column (Geometry or Binary WKB).
        geom_b: Second Point geometry column (Geometry or Binary WKB).

    Returns:
        Geometry column containing LineString geometries. Returns null for rows
        where either input is not a Point geometry.
    """
    return Expression._call_builtin_scalar_fn("st_makeline", geom_a, geom_b)


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


# ── Bounding box ─────────────────────────────────────────────────────────────


def st_bbox(geom: Expression) -> Expression:
    """Returns the geometry's bounding box as a struct ``{min_x, min_y, max_x, max_y}`` (Float64).

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

    Returns:
        Struct column with Float64 fields ``min_x``, ``min_y``, ``max_x``, ``max_y``.
        Returns null for null or empty geometries.
    """
    return Expression._call_builtin_scalar_fn("st_bbox", geom)


# ── Perimeter / representative point / repair ────────────────────────────────


def st_perimeter(geom: Expression, use_spheroid: bool = False) -> Expression:
    """Return the perimeter of areal geometries (Polygon, MultiPolygon).

    Sums the length of the exterior ring and all interior rings (holes). All
    non-areal types (Point, LineString, MultiLineString, etc.) return 0.0.

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).
        use_spheroid: If True, compute geodesic perimeter in WGS84 meters (lon/lat input
            assumed). Default False uses planar Euclidean length (coordinate units).

    Returns:
        Float64 column. Returns 0.0 for non-areal geometries; planar (coordinate units)
        by default, WGS84 geodesic meters when use_spheroid=True.
    """
    if use_spheroid:
        return Expression._call_builtin_scalar_fn("st_perimeter", geom, use_spheroid)
    return Expression._call_builtin_scalar_fn("st_perimeter", geom)


def st_pointonsurface(geom: Expression) -> Expression:
    """Return a Point guaranteed to lie on the surface of a geometry.

    Unlike ``st_centroid`` (which may fall outside a concave shape or a hole),
    the returned point always intersects the input geometry.

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

    Returns:
        Geometry column containing Point geometries. Returns null for empty
        or unparseable geometries.
    """
    return Expression._call_builtin_scalar_fn("st_pointonsurface", geom)


def st_makevalid(geom: Expression) -> Expression:
    """Repair an invalid geometry, returning a valid one.

    Repairs invalid **polygonal** geometries (self-intersections, bowties, etc.)
    and returns a valid ``MultiPolygon``. Non-polygonal geometries (Point,
    LineString, ...) are returned unchanged, since the pure-Rust engine only
    repairs areal geometries (unlike PostGIS/GEOS, which handles all types).

    Args:
        geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

    Returns:
        Geometry column. Returns null when the geometry cannot be repaired.
    """
    return Expression._call_builtin_scalar_fn("st_makevalid", geom)
