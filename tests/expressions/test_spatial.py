from __future__ import annotations

import math

import pyarrow as pa
import pytest

import daft
import daft.functions
from daft.functions import st_touches, st_disjoint, st_equals, st_crosses, st_overlaps, st_geomfromtext, st_distance, st_buffer, st_area, st_isvalid


def _great_circle_distance(
    lat1: list[float | None],
    lon1: list[float | None],
    lat2: list[float | None],
    lon2: list[float | None],
) -> list[float | None]:
    df = daft.from_pydict({"lat1": lat1, "lon1": lon1, "lat2": lat2, "lon2": lon2})
    return df.select(
        daft.functions.great_circle_distance(df["lat1"], df["lon1"], df["lat2"], df["lon2"]).alias("distance")
    ).to_pydict()["distance"]


def _assert_distance_results(
    actual: list[float | None],
    expected: list[float | None],
) -> None:
    assert len(actual) == len(expected)
    for actual_value, expected_value in zip(actual, expected):
        if expected_value is None:
            assert actual_value is None
        else:
            assert actual_value == pytest.approx(expected_value, rel=1e-6)


@pytest.mark.parametrize(
    "lat1,lon1,lat2,lon2,expected",
    [
        pytest.param(
            [0.0, 10.0, 20.0],
            [0.0, 10.0, 20.0],
            [1.0, 11.0, 21.0],
            [1.0, 11.0, 21.0],
            [157249.38127194397, 155941.21480117142, 152354.11114794918],
            id="diagonal_one_degree_offsets",
        ),
        pytest.param([0.0], [0.0], [0.0], [0.0], [0.0], id="same_point_is_zero"),
        pytest.param(
            [0.0],
            [0.0],
            [0.0],
            [180.0],
            [math.pi * 6_371_000.0],
            id="antipodal_along_equator",
        ),
        pytest.param(
            [90.0],
            [0.0],
            [-90.0],
            [0.0],
            [math.pi * 6_371_000.0],
            id="pole_to_pole",
        ),
        pytest.param(
            [0.0],
            [0.0],
            [0.0],
            [1.0],
            [111194.92664455873],
            id="one_degree_along_equator",
        ),
        pytest.param(
            [0.0000001],
            [0.0],
            [-0.0000001],
            [179.9999999],
            [math.pi * 6_371_000.0],
            id="near_antipodal_does_not_nan",
        ),
        pytest.param(
            [None, 10.0, None],
            [0.0, None, 20.0],
            [1.0, 11.0, 21.0],
            [1.0, 11.0, None],
            [None, None, None],
            id="any_null_input_yields_null",
        ),
        pytest.param(
            [0.0, None],
            [0.0, None],
            [0.0, None],
            [0.0, None],
            [0.0, None],
            id="mixed_null_and_non_null_rows",
        ),
    ],
)
def test_great_circle_distance(
    lat1: list[float | None],
    lon1: list[float | None],
    lat2: list[float | None],
    lon2: list[float | None],
    expected: list[float | None],
) -> None:
    actual = _great_circle_distance(lat1, lon1, lat2, lon2)
    _assert_distance_results(actual, expected)


@pytest.mark.parametrize(
    "lat1,lon1,lat2,lon2,expected",
    [
        pytest.param([float("nan")], [0.0], [0.0], [1.0], [None], id="nan_lat1_yields_null"),
        pytest.param([0.0], [float("inf")], [0.0], [1.0], [None], id="inf_lon1_yields_null"),
        pytest.param([91.0], [0.0], [0.0], [1.0], [None], id="lat1_out_of_range_yields_null"),
        pytest.param([0.0], [181.0], [0.0], [1.0], [None], id="lon1_out_of_range_yields_null"),
        pytest.param([0.0], [0.0], [-91.0], [1.0], [None], id="lat2_out_of_range_yields_null"),
        pytest.param([0.0], [0.0], [0.0], [-181.0], [None], id="lon2_out_of_range_yields_null"),
        pytest.param(
            [0.0, 91.0, 0.0, float("nan")],
            [0.0, 0.0, 181.0, 0.0],
            [0.0, 0.0, 0.0, 0.0],
            [1.0, 1.0, 1.0, 1.0],
            [111194.92664455873, None, None, None],
            id="mixed_valid_and_invalid_rows",
        ),
    ],
)
def test_great_circle_distance_invalid_inputs_yield_null(
    lat1: list[float | None],
    lon1: list[float | None],
    lat2: list[float | None],
    lon2: list[float | None],
    expected: list[float | None],
) -> None:
    actual = _great_circle_distance(lat1, lon1, lat2, lon2)
    _assert_distance_results(actual, expected)


@pytest.mark.parametrize(
    "arrow_type,lat1,lon1,lat2,lon2,expected",
    [
        pytest.param(
            pa.int32(),
            [0],
            [0],
            [0],
            [1],
            111194.92664455873,
            id="int32_inputs_cast_to_float64",
        ),
        pytest.param(
            pa.float32(),
            [0.0],
            [0.0],
            [0.0],
            [1.0],
            111194.92664455873,
            id="float32_inputs_cast_to_float64",
        ),
    ],
)
def test_great_circle_distance_numeric_input_types(
    arrow_type: pa.DataType,
    lat1: list[int | float],
    lon1: list[int | float],
    lat2: list[int | float],
    lon2: list[int | float],
    expected: float,
) -> None:
    table = pa.table(
        {
            "lat1": pa.array(lat1, type=arrow_type),
            "lon1": pa.array(lon1, type=arrow_type),
            "lat2": pa.array(lat2, type=arrow_type),
            "lon2": pa.array(lon2, type=arrow_type),
        }
    )
    df = daft.from_arrow(table)
    actual: list[float | None] = df.select(
        daft.functions.great_circle_distance(df["lat1"], df["lon1"], df["lat2"], df["lon2"]).alias("distance")
    ).to_pydict()["distance"]
    assert actual[0] == pytest.approx(expected, rel=1e-6)


def test_great_circle_distance_sql_smoke() -> None:
    actual: list[float | None] = daft.sql(
        "SELECT great_circle_distance(0.0, 0.0, 0.0, 1.0) AS distance",
    ).to_pydict()["distance"]
    assert actual[0] == pytest.approx(111194.92664455873, rel=1e-6)


# ── Spatial predicate tests ──────────────────────────────────────────────────


def test_st_disjoint_and_touches():
    # two unit squares sharing an edge → touch, not disjoint
    a = "POLYGON((0 0,1 0,1 1,0 1,0 0))"
    b = "POLYGON((1 0,2 0,2 1,1 1,1 0))"
    df = daft.from_pydict({"a": [a], "b": [b]}).select(
        st_touches(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("b"))).alias("t"),
        st_disjoint(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("b"))).alias("d"),
    ).to_pydict()
    assert df["t"] == [True]
    assert df["d"] == [False]


def test_st_equals():
    a = "POINT(1 2)"
    df = daft.from_pydict({"a": [a]}).select(
        st_equals(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("a"))).alias("e"),
    ).to_pydict()
    assert df["e"] == [True]


def test_st_crosses():
    # A line that crosses the polygon boundary (enters the interior) → True
    line_crossing = "LINESTRING(-1 1, 3 1)"
    polygon = "POLYGON((0 0,2 0,2 2,0 2,0 0))"
    # A line entirely outside → False
    line_outside = "LINESTRING(5 5, 6 6)"
    df = daft.from_pydict({"line": [line_crossing, line_outside], "poly": [polygon, polygon]}).select(
        st_crosses(st_geomfromtext(daft.col("line")), st_geomfromtext(daft.col("poly"))).alias("c"),
    ).to_pydict()
    assert df["c"] == [True, False]


def test_geodesic_distance_meters():
    # Great-circle distance between two lon/lat points; planar vs spheroid differ massively.
    # POINT(0 0) = lon=0, lat=0; POINT(0 1) = lon=0, lat=1 (1° latitude difference).
    # Planar (Euclidean) distance is 1.0 (coordinate units).
    # WGS84 geodesic: geographiclib gives ~110574 m for 1° lat at equator.
    a = "POINT(0 0)"
    b = "POINT(0 1)"
    df = daft.from_pydict({"a": [a], "b": [b]}).select(
        st_distance(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("b"))).alias("planar"),
        st_distance(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("b")), use_spheroid=True).alias("geo"),
    ).to_pydict()
    assert abs(df["planar"][0] - 1.0) < 1e-9      # planar = 1.0 (degrees)
    assert abs(df["geo"][0] - 110574.0) < 200.0   # geodesic meters, WGS84 (geo crate)


def test_st_overlaps():
    # Two partially overlapping same-dimension polygons → True
    poly_a = "POLYGON((0 0,2 0,2 2,0 2,0 0))"
    poly_b = "POLYGON((1 1,3 1,3 3,1 3,1 1))"
    # poly_a fully contains poly_small — not an overlap (containment) → False
    poly_small = "POLYGON((0.5 0.5,1 0.5,1 1,0.5 1,0.5 0.5))"
    df = daft.from_pydict({"a": [poly_a, poly_a], "b": [poly_b, poly_small]}).select(
        st_overlaps(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("b"))).alias("o"),
    ).to_pydict()
    assert df["o"] == [True, False]


def test_sql_geodesic_distance():
    # Verify the SQL path exercises geodesic distance via SQLStMeasureBinary.
    # POINT(0 0) = lon=0,lat=0; POINT(0 1) = lon=0,lat=1 (1 degree latitude apart).
    # Planar Euclidean distance = 1.0 coordinate unit.
    # WGS84 geodesic distance ≈ 110574 m (geo crate / geographiclib value at equator).
    base = daft.from_pydict({"a": ["POINT(0 0)"], "b": ["POINT(0 1)"]})  # noqa: F841
    geo_out = daft.sql(
        "SELECT st_distance(st_geomfromtext(a), st_geomfromtext(b), true) AS d FROM base"
    ).to_pydict()
    assert abs(geo_out["d"][0] - 110574.0) < 300.0, f"geodesic SQL distance: {geo_out['d'][0]}"

    planar_out = daft.sql(
        "SELECT st_distance(st_geomfromtext(a), st_geomfromtext(b)) AS d FROM base"
    ).to_pydict()
    assert abs(planar_out["d"][0] - 1.0) < 1e-9, f"planar SQL distance: {planar_out['d'][0]}"


def test_sql_geodesic_length():
    # Verify SQL path for st_length with use_spheroid=true.
    # A linestring spanning 1 degree of latitude should have geodesic length ~110574 m.
    # Planar length = 1.0 coordinate unit.
    ls = daft.from_pydict({"geom": ["LINESTRING(0 0, 0 1)"]})  # noqa: F841
    geo_out = daft.sql(
        "SELECT st_length(st_geomfromtext(geom), true) AS l FROM ls"
    ).to_pydict()
    assert abs(geo_out["l"][0] - 110574.0) < 300.0, f"geodesic SQL length: {geo_out['l'][0]}"

    planar_out = daft.sql(
        "SELECT st_length(st_geomfromtext(geom)) AS l FROM ls"
    ).to_pydict()
    assert abs(planar_out["l"][0] - 1.0) < 1e-9, f"planar SQL length: {planar_out['l'][0]}"


def test_geodesic_length_polygon_returns_zero():
    # Verify geodesic st_length returns 0.0 for polygons, matching the planar branch.
    # Users needing geodesic perimeter of polygons should use st_area (GeodesicArea).
    poly = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"
    df = daft.from_pydict({"g": [poly]}).select(
        st_geomfromtext(daft.col("g")).alias("geom")
    )
    import daft.functions as F
    result = df.select(
        F.st_length(daft.col("geom")).alias("planar"),
        F.st_length(daft.col("geom"), use_spheroid=True).alias("geodesic"),
    ).to_pydict()
    assert result["planar"][0] == 0.0, f"planar polygon length should be 0.0, got {result['planar'][0]}"
    assert result["geodesic"][0] == 0.0, f"geodesic polygon length should be 0.0, got {result['geodesic'][0]}"


# ── st_buffer tests ──────────────────────────────────────────────────────────


def test_st_buffer_point_area_approx_pi():
    """Buffer of radius-1 around a point should have area ≈ π (64-gon circle)."""
    df = daft.from_pydict({"w": ["POINT(0 0)"]})
    geom = st_geomfromtext(daft.col("w"))
    result = df.select(st_area(st_buffer(geom, 1.0)).alias("a")).to_pydict()
    area = result["a"][0]
    assert abs(area - math.pi) < 0.01, f"Expected area ≈ π, got {area}"


def test_st_buffer_point_area_approx_pi_sql():
    """SQL: buffer of radius-1 around a point should have area ≈ π."""
    df = daft.from_pydict({"w": ["POINT(0 0)"]})
    result = daft.sql(
        "SELECT st_area(st_buffer(st_geomfromtext(w), 1.0)) AS a FROM df"
    ).to_pydict()
    area = result["a"][0]
    assert abs(area - math.pi) < 0.01, f"SQL: expected area ≈ π, got {area}"


def test_st_buffer_polygon_expands():
    """Buffer of a unit-square polygon should produce a larger geometry."""
    df = daft.from_pydict({"w": ["POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"]})
    geom = st_geomfromtext(daft.col("w"))
    result = df.select(st_area(st_buffer(geom, 1.0)).alias("a")).to_pydict()
    area = result["a"][0]
    # Area must be larger than the unit square (area=1)
    assert area > 1.0, f"Buffered polygon area {area} should exceed unit square area"


# ── st_isvalid tests ─────────────────────────────────────────────────────────


def test_st_isvalid_simple_square():
    """A simple square polygon is valid."""
    df = daft.from_pydict({"w": ["POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"]})
    result = df.select(st_isvalid(st_geomfromtext(daft.col("w"))).alias("v")).to_pydict()
    assert result["v"][0] is True


def test_st_isvalid_bowtie_false():
    """A bowtie (self-intersecting) polygon is invalid."""
    # Ring: (0,0)→(2,2)→(2,0)→(0,2)→(0,0) — edges cross at (1,1)
    df = daft.from_pydict({"w": ["POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))"]})
    result = df.select(st_isvalid(st_geomfromtext(daft.col("w"))).alias("v")).to_pydict()
    assert result["v"][0] is False


def test_union_area():
    """Union area = 4+4-1=7; intersection area = 1 for two overlapping 2×2 squares."""
    from daft.functions import st_union, st_intersection
    a = "POLYGON((0 0,2 0,2 2,0 2,0 0))"
    b = "POLYGON((1 1,3 1,3 3,1 3,1 1))"
    df = daft.from_pydict({"a": [a], "b": [b]}).select(
        st_area(st_union(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("b")))).alias("u"),
        st_area(st_intersection(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("b")))).alias("i"),
    ).to_pydict()
    assert abs(df["u"][0] - 7.0) < 1e-6, f"union area: {df['u'][0]}"
    assert abs(df["i"][0] - 1.0) < 1e-6, f"intersection area: {df['i'][0]}"


def test_difference_symdifference_area():
    """difference = 3 (A minus overlap); symdifference = 6 (total minus 2*overlap)."""
    from daft.functions import st_difference, st_symdifference
    a = "POLYGON((0 0,2 0,2 2,0 2,0 0))"
    b = "POLYGON((1 1,3 1,3 3,1 3,1 1))"
    df = daft.from_pydict({"a": [a], "b": [b]}).select(
        st_area(st_difference(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("b")))).alias("d"),
        st_area(st_symdifference(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("b")))).alias("sd"),
    ).to_pydict()
    assert abs(df["d"][0] - 3.0) < 1e-6, f"difference area: {df['d'][0]}"
    assert abs(df["sd"][0] - 6.0) < 1e-6, f"symdifference area: {df['sd'][0]}"


def test_overlay_non_polygon_yields_null():
    """Non-polygon geometries (Point) return null for overlay ops."""
    from daft.functions import st_union
    df = daft.from_pydict({"a": ["POINT(0 0)"], "b": ["POINT(1 1)"]}).select(
        st_union(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("b"))).alias("u"),
    ).to_pydict()
    assert df["u"][0] is None, f"Expected null for point union, got {df['u'][0]}"


def test_overlay_sql_parity():
    """SQL overlay results match Python for union and intersection."""
    from daft.functions import st_union, st_intersection
    a_wkt = "POLYGON((0 0,2 0,2 2,0 2,0 0))"
    b_wkt = "POLYGON((1 1,3 1,3 3,1 3,1 1))"
    df = daft.from_pydict({"a": [a_wkt], "b": [b_wkt]})  # noqa: F841
    py = daft.from_pydict({"a": [a_wkt], "b": [b_wkt]}).select(
        st_area(st_union(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("b")))).alias("u"),
        st_area(st_intersection(st_geomfromtext(daft.col("a")), st_geomfromtext(daft.col("b")))).alias("i"),
    ).to_pydict()
    sql = daft.sql(
        "SELECT st_area(st_union(st_geomfromtext(a), st_geomfromtext(b))) AS u, "
        "st_area(st_intersection(st_geomfromtext(a), st_geomfromtext(b))) AS i FROM df"
    ).to_pydict()
    assert abs(py["u"][0] - sql["u"][0]) < 1e-6, f"union parity: py={py['u'][0]} sql={sql['u'][0]}"
    assert abs(py["i"][0] - sql["i"][0]) < 1e-6, f"intersection parity: py={py['i'][0]} sql={sql['i'][0]}"


def test_st_isvalid_sql_parity():
    """SQL st_isvalid should match Python result for both valid and invalid geometries."""
    df = daft.from_pydict({
        "w": [
            "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",   # valid square
            "POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))",   # bowtie → invalid
        ]
    })
    # Python
    py_result = df.select(
        st_isvalid(st_geomfromtext(daft.col("w"))).alias("v")
    ).to_pydict()
    # SQL
    sql_result = daft.sql(
        "SELECT st_isvalid(st_geomfromtext(w)) AS v FROM df"
    ).to_pydict()
    assert py_result["v"] == [True, False]
    assert sql_result["v"] == [True, False]
