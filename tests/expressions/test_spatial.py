from __future__ import annotations

import math

import pytest

import daft
import daft.functions


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
        pytest.param(
            [0.0],
            [0.0],
            [0.0],
            [0.0],
            [0.0],
            id="same_point_is_zero",
        ),
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
    df = daft.from_pydict({"lat1": lat1, "lon1": lon1, "lat2": lat2, "lon2": lon2})

    result = df.select(
        daft.functions.great_circle_distance(df["lat1"], df["lon1"], df["lat2"], df["lon2"]).alias("distance")
    )

    actual = result.to_pydict()["distance"]
    assert len(actual) == len(expected)
    for actual_value, expected_value in zip(actual, expected):
        if expected_value is None:
            assert actual_value is None
        else:
            assert actual_value == pytest.approx(expected_value, rel=1e-6)
