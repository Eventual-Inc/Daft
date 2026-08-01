# functions spatial

## great_circle_distance

```python
great_circle_distance(lat1: Expression, lon1: Expression, lat2: Expression, lon2: Expression) -> Expression
```

Compute the great circle distance between two points on the Earth.

Invalid inputs including nulls, non-finite values, or out-of-range coordinates
(lat ∈ [-90, 90], lon ∈ [-180, 180]) produce null outputs.

Args:
    lat1: Latitude of the first point in degrees.
    lon1: Longitude of the first point in degrees.
    lat2: Latitude of the second point in degrees.
    lon2: Longitude of the second point in degrees.

Returns:
    Great circle distance in meters between the two points.

## st_area

```python
st_area(geom: Expression, use_spheroid: bool=False) -> Expression
```

Return the 2D area of a geometry.

Args:
    geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).
    use_spheroid: If True, compute geodesic area in WGS84 square meters (lon/lat input
        assumed). Default False uses planar (coordinate-system units squared).

Returns:
    Float64 column with the unsigned area in coordinate-system units squared (planar)
    or WGS84 square meters (geodesic). Returns null for null or unparseable geometries.

## st_astext

```python
st_astext(geom: Expression) -> Expression
```

Return the Well-Known Text (WKT) representation of a geometry.

Args:
    geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

Returns:
    Utf8 column with WKT strings.

## st_bbox

```python
st_bbox(geom: Expression) -> Expression
```

Returns the geometry's bounding box as a struct ``{min_x, min_y, max_x, max_y}`` (Float64).

Args:
    geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

Returns:
    Struct column with Float64 fields ``min_x``, ``min_y``, ``max_x``, ``max_y``.
    Returns null for null or empty geometries.

## st_buffer

```python
st_buffer(geom: Expression, distance: float) -> Expression
```

Return a geometry that is the given distance from the input geometry (planar Cartesian).

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

## st_centroid

```python
st_centroid(geom: Expression) -> Expression
```

Return the geometric centroid (center of mass) of a geometry as a Point.

Args:
    geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

Returns:
    Geometry column containing Point geometries.

## st_contains

```python
st_contains(geom_a: Expression, geom_b: Expression) -> Expression
```

Return whether geometry A completely contains geometry B.

Args:
    geom_a: Container geometry (Geometry or Binary WKB column).
    geom_b: Contained geometry (Geometry or Binary WKB column).

Returns:
    Boolean column.

## st_convexhull

```python
st_convexhull(geom: Expression) -> Expression
```

Return the convex hull of a geometry as a Polygon.

Args:
    geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

Returns:
    Geometry column containing the convex hull polygon. Returns null for
    null or unparseable inputs.

## st_covered_by

```python
st_covered_by(geom_a: Expression, geom_b: Expression) -> Expression
```

Returns true if geometry A is covered by geometry B (no point of A is outside B; boundary included).

## st_covers

```python
st_covers(geom_a: Expression, geom_b: Expression) -> Expression
```

Returns true if geometry A covers geometry B (no point of B is outside A; boundary included).

## st_crosses

```python
st_crosses(geom_a: Expression, geom_b: Expression) -> Expression
```

Return true where A and B cross.

## st_difference

```python
st_difference(geom_a: Expression, geom_b: Expression) -> Expression
```

Return the part of geometry A that does not intersect geometry B.

Both operands must be Polygon or MultiPolygon; other types return null.

Args:
    geom_a: First geometry column (Geometry or Binary WKB).
    geom_b: Second geometry column (Geometry or Binary WKB).

Returns:
    Geometry column (MultiPolygon). Returns null for non-polygon inputs.

## st_disjoint

```python
st_disjoint(geom_a: Expression, geom_b: Expression) -> Expression
```

Return true where A and B share no points.

## st_distance

```python
st_distance(geom_a: Expression, geom_b: Expression, use_spheroid: bool=False) -> Expression
```

Return the minimum distance between two geometries.

Args:
    geom_a: First geometry column.
    geom_b: Second geometry column (supports scalar broadcast).
    use_spheroid: If True, compute WGS84 geodesic distance in meters (lon/lat point
        inputs assumed). Default False uses planar Euclidean distance (coordinate units).

Returns:
    Float64 column. Planar by default; WGS84 geodesic meters when use_spheroid=True
    (point inputs only; other geometry pairs return NaN in geodesic mode).

## st_dump

```python
st_dump(geom: Expression) -> Expression
```

Return a list of dumped members with PostGIS-style path metadata.

Each output element is a struct ``{path, geom}``, where ``path`` is a list of
integer indexes describing the component location and ``geom`` is the component
geometry. Atomic geometries return a singleton element with an empty path.
Multi-geometries and geometry collections use 1-based path indexing.

Args:
    geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

Returns:
    ``List[Struct{path: List[Int64], geom: Geometry}]`` column.

## st_dumprings

```python
st_dumprings(geom: Expression) -> Expression
```

Return polygon rings with PostGIS-style path metadata.

Each output element is a struct ``{path, geom}``, where ``geom`` is a single-ring
Polygon and ``path`` is ``[0]`` for the exterior ring, then ``[1..n]``
for interior rings. This function is polygon-only; non-polygonal inputs return null.

Args:
    geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

Returns:
    ``List[Struct{path: List[Int64], geom: Geometry}]`` column.

## st_dwithin

```python
st_dwithin(geom_a: Expression, geom_b: Expression, distance: float) -> Expression
```

Returns true if the planar distance between two geometries is <= ``distance`` (coordinate units).

``distance`` must be a numeric literal.

## st_envelope

```python
st_envelope(geom: Expression) -> Expression
```

Return the minimum bounding rectangle of a geometry as a Polygon.

Args:
    geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

Returns:
    Geometry column containing the bounding-box polygon. Returns null for
    null or unparseable inputs, or for geometries with no extent.

## st_equals

```python
st_equals(geom_a: Expression, geom_b: Expression) -> Expression
```

Return true where A and B are topologically equal.

## st_geohash

```python
st_geohash(geom: Expression, precision: int=5) -> Expression
```

Return the geohash of a geometry's centroid.

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

## st_geojsonfromgeom

```python
st_geojsonfromgeom(geom: Expression) -> Expression
```

Return the GeoJSON representation of a geometry.

Args:
    geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

Returns:
    Utf8 column with GeoJSON strings.

## st_geometrytype

```python
st_geometrytype(geom: Expression) -> Expression
```

Return the geometry type name as a string.

Args:
    geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

Returns:
    Utf8 column with values like ``'Point'``, ``'LineString'``, ``'Polygon'``, etc.

## st_geomfromgeojson

```python
st_geomfromgeojson(geojson: Expression) -> Expression
```

Parse a GeoJSON geometry or feature string into a Geometry.

Args:
    geojson: A Utf8 column of GeoJSON strings (e.g. ``'{"type":"Point","coordinates":[1,2]}'``).

Returns:
    A ``DataType.geometry()`` column. Returns null for rows where the input is null
    or the GeoJSON string cannot be parsed as a valid geometry or feature.

## st_geomfromtext

```python
st_geomfromtext(wkt: Expression) -> Expression
```

Parse a Well-Known Text (WKT) string into a Geometry.

Args:
    wkt: A Utf8 column of WKT geometry strings (e.g. ``'POINT(1 2)'``,
        ``'POLYGON((0 0,1 0,1 1,0 1,0 0))'``).

Returns:
    A ``DataType.geometry()`` column. Returns null for rows where the input is null
    or the WKT string cannot be parsed as a valid geometry.

## st_intersection

```python
st_intersection(geom_a: Expression, geom_b: Expression) -> Expression
```

Return the geometric intersection of two polygon geometries.

Both operands must be Polygon or MultiPolygon; other types return null.

Args:
    geom_a: First geometry column (Geometry or Binary WKB).
    geom_b: Second geometry column (Geometry or Binary WKB).

Returns:
    Geometry column (MultiPolygon). Returns null for non-polygon inputs.

## st_intersects

```python
st_intersects(geom_a: Expression, geom_b: Expression) -> Expression
```

Return whether geometry A and geometry B spatially intersect.

Args:
    geom_a: First geometry column.
    geom_b: Second geometry column (supports scalar broadcast).

Returns:
    Boolean column.

## st_isvalid

```python
st_isvalid(geom: Expression) -> Expression
```

Return whether a geometry is topologically valid according to OGC rules.

Args:
    geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

Returns:
    Boolean column.

## st_length

```python
st_length(geom: Expression, use_spheroid: bool=False) -> Expression
```

Return the length of line geometries.

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

## st_makeline

```python
st_makeline(geom_a: Expression, geom_b: Expression) -> Expression
```

Construct a LineString geometry from two Point geometries.

Args:
    geom_a: First Point geometry column (Geometry or Binary WKB).
    geom_b: Second Point geometry column (Geometry or Binary WKB).

Returns:
    Geometry column containing LineString geometries. Returns null for rows
    where either input is not a Point geometry.

## st_makevalid

```python
st_makevalid(geom: Expression) -> Expression
```

Repair an invalid geometry, returning a valid one.

Repairs invalid **polygonal** geometries (self-intersections, bowties, etc.)
and returns a valid ``MultiPolygon``. Non-polygonal geometries (Point,
LineString, ...) are returned unchanged, since the pure-Rust engine only
repairs areal geometries (unlike PostGIS/GEOS, which handles all types).

Args:
    geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

Returns:
    Geometry column. Returns null when the geometry cannot be repaired.

## st_overlaps

```python
st_overlaps(geom_a: Expression, geom_b: Expression) -> Expression
```

Return true where A and B overlap (same dimension, partial intersection).

## st_perimeter

```python
st_perimeter(geom: Expression, use_spheroid: bool=False) -> Expression
```

Return the perimeter of areal geometries (Polygon, MultiPolygon).

Sums the length of the exterior ring and all interior rings (holes). All
non-areal types (Point, LineString, MultiLineString, etc.) return 0.0.

Args:
    geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).
    use_spheroid: If True, compute geodesic perimeter in WGS84 meters (lon/lat input
        assumed). Default False uses planar Euclidean length (coordinate units).

Returns:
    Float64 column. Returns 0.0 for non-areal geometries; planar (coordinate units)
    by default, WGS84 geodesic meters when use_spheroid=True.

## st_point

```python
st_point(x: Expression, y: Expression) -> Expression
```

Construct a Point geometry from x and y coordinate columns.

Args:
    x: Numeric column of X (longitude) coordinates.
    y: Numeric column of Y (latitude) coordinates.

Returns:
    Geometry column containing Point geometries. Returns null for rows
    where either coordinate is null.

## st_pointonsurface

```python
st_pointonsurface(geom: Expression) -> Expression
```

Return a Point guaranteed to lie on the surface of a geometry.

Unlike ``st_centroid`` (which may fall outside a concave shape or a hole),
the returned point always intersects the input geometry.

Args:
    geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

Returns:
    Geometry column containing Point geometries. Returns null for empty
    or unparseable geometries.

## st_simplify

```python
st_simplify(geom: Expression, tolerance: float) -> Expression
```

Simplify a geometry using the Ramer–Douglas–Peucker algorithm.

Applies to LineString, MultiLineString, Polygon, and MultiPolygon.
Other geometry types (Point, MultiPoint, etc.) are returned unchanged.

Args:
    geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).
    tolerance: Simplification tolerance in the same units as the geometry's
        coordinate system. Larger values produce coarser simplification.

Returns:
    Geometry column with simplified geometries.

## st_symdifference

```python
st_symdifference(geom_a: Expression, geom_b: Expression) -> Expression
```

Return the symmetric difference (XOR) of two polygon geometries.

Returns the regions in either geometry but not in both.
Both operands must be Polygon or MultiPolygon; other types return null.

Args:
    geom_a: First geometry column (Geometry or Binary WKB).
    geom_b: Second geometry column (Geometry or Binary WKB).

Returns:
    Geometry column (MultiPolygon). Returns null for non-polygon inputs.

## st_touches

```python
st_touches(geom_a: Expression, geom_b: Expression) -> Expression
```

Return true where A and B share a boundary but their interiors do not intersect.

## st_union

```python
st_union(geom_a: Expression, geom_b: Expression) -> Expression
```

Return the geometric union of two polygon geometries.

Both operands must be Polygon or MultiPolygon; other types (e.g. Point,
LineString) return null.

Args:
    geom_a: First geometry column (Geometry or Binary WKB).
    geom_b: Second geometry column (Geometry or Binary WKB).

Returns:
    Geometry column (MultiPolygon). Returns null for non-polygon inputs
    or if the underlying boolean operation raises an error.

## st_within

```python
st_within(geom_a: Expression, geom_b: Expression) -> Expression
```

Return whether geometry A is completely within geometry B.

Args:
    geom_a: Inner geometry column.
    geom_b: Container geometry column.

Returns:
    Boolean column.

## st_x

```python
st_x(geom: Expression) -> Expression
```

Return the X (longitude) coordinate of a Point geometry.

Args:
    geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

Returns:
    Float64 column. Returns null for non-point geometries.

## st_y

```python
st_y(geom: Expression) -> Expression
```

Return the Y (latitude) coordinate of a Point geometry.

Args:
    geom: A column of type ``DataType.geometry()`` or ``DataType.binary()`` (WKB).

Returns:
    Float64 column. Returns null for non-point geometries.
