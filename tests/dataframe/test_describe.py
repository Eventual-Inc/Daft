from __future__ import annotations

import pytest

import daft


def test_describe_dataframe_missing_col() -> None:
    df = daft.from_pydict({"a": [1, 2, 3], "b": [None, "a", "b"]})

    with pytest.raises(ValueError):
        df = df.describe(["foo", "b"])

    with pytest.raises(ValueError):
        df = df.describe("foo")


def test_describe_dataframe(make_df, valid_data: list[dict[str, float]]) -> None:
    df = daft.from_pydict({"a": [1, 2, 3], "b": [None, "a", "b"]})
    expected = {
        "a_count": [3],
        "a_nulls": [0],
        "a_approx_distinct": [3],
        "a_min": [1],
        "a_max": [3],
        "b_count": [3],
        "b_nulls": [1],
        "b_approx_distinct": [2],
        "b_min": ["a"],
        "b_max": ["b"],
    }

    df_all_cols = df.describe(["a", "b"])
    assert df_all_cols.collect().to_pydict() == expected

    df_none_specified = df.describe()
    assert df_none_specified.collect().to_pydict() == expected

    expected_one_col = {
        "a_count": [3],
        "a_nulls": [0],
        "a_approx_distinct": [3],
        "a_min": [1],
        "a_max": [3],
    }

    df_one_col = df.describe("a")
    assert df_one_col.collect().to_pydict() == expected_one_col
