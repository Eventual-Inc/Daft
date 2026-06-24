from __future__ import annotations

import math

import pyarrow as pa
import pytest

import daft
import daft.functions
from daft.functions import st_touches, st_disjoint, st_equals, st_crosses, st_overlaps, st_geomfromtext, st_distance


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
