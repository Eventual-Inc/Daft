from __future__ import annotations

import pytest

from daft.expressions import col
from daft.functions import (
    columns_max,
    columns_mean,
    columns_min,
    columns_sum,
)
from daft.functions.functions import columns_avg


def test_columns_sum(make_df) -> None:
    data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}
    df = make_df(data).with_column("sum", columns_sum("a", "b", "c"))

    assert df.to_pydict()["sum"] == [12, 15, 18]


def test_columns_sum_with_nulls(make_df) -> None:
    data = {"a": [1, None, 3], "b": [4, 5, None], "c": [None, 8, 9]}
    df = make_df(data).with_column("sum", columns_sum("a", "b", "c"))

    assert df.to_pydict()["sum"] == [5, 13, 12]


def test_columns_sum_with_expressions(make_df) -> None:
    data = {"a": [1, 2, 3], "b": [1, 2, 3], "c": [1, 2, 3]}
    df = make_df(data).with_column("sum", columns_sum(col("a") + 1, col("b") + 2, col("c") + 3))

    assert df.to_pydict()["sum"] == [9, 12, 15]


def test_columns_sum_in_select(make_df) -> None:
    df = make_df({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
    df = df.select(columns_sum("a", "b", "c"))

    assert df.to_pydict()["columns_sum"] == [12, 15, 18]


def test_columns_sum_with_no_input_columns(make_df) -> None:
    df = make_df({"a": [1, 2, 3]})

    with pytest.raises(ValueError, match="columns_sum requires at least one expression"):
        df.with_column("sum", columns_sum()).collect()


def test_columns_mean(make_df) -> None:
    data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}
    df = make_df(data).with_columns(
        {
            "mean": columns_mean("a", "b", "c"),
            "avg": columns_avg("a", "b", "c"),
        }
    )

    assert df.to_pydict()["mean"] == [4.0, 5.0, 6.0]
    assert df.to_pydict()["avg"] == [4.0, 5.0, 6.0]


def test_columns_mean_with_nulls(make_df) -> None:
    data = {"a": [1, None, None], "b": [None, 5, None], "c": [None, None, 9]}
    df = make_df(data).with_columns(
        {
            "mean": columns_mean("a", "b", "c"),
            "avg": columns_avg("a", "b", "c"),
        }
    )

    assert df.to_pydict()["mean"] == [1.0, 5.0, 9.0]
    assert df.to_pydict()["avg"] == [1.0, 5.0, 9.0]


def test_columns_mean_with_expressions(make_df) -> None:
    data = {"a": [1, 2, 3], "b": [1, 2, 3], "c": [1, 2, 3]}
    df = make_df(data).with_columns(
        {
            "mean": columns_mean(col("a") + 1, col("b") + 2, col("c") + 3),
            "avg": columns_avg(col("a") + 1, col("b") + 2, col("c") + 3),
        }
    )

    assert df.to_pydict()["mean"] == [3.0, 4.0, 5.0]
    assert df.to_pydict()["avg"] == [3.0, 4.0, 5.0]


def test_columns_mean_in_select(make_df) -> None:
    df = make_df({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
    df = df.select(
        columns_mean("a", "b", "c"),
        columns_avg("a", "b", "c"),
    )

    assert df.to_pydict()["columns_mean"] == [4.0, 5.0, 6.0]
    assert df.to_pydict()["columns_avg"] == [4.0, 5.0, 6.0]


def test_columns_mean_with_no_input_columns(make_df) -> None:
    df = make_df({"a": [1, 2, 3]})

    with pytest.raises(ValueError, match="columns_mean requires at least one expression"):
        df.with_column("mean", columns_mean()).collect()

    with pytest.raises(ValueError, match="columns_avg requires at least one expression"):
        df.with_column("avg", columns_avg()).collect()


def test_columns_min(make_df) -> None:
    data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}
    df = make_df(data).with_column("min", columns_min("a", "b", "c"))

    assert df.to_pydict()["min"] == [1, 2, 3]


def test_columns_min_with_nulls(make_df) -> None:
    data = {"a": [1, None, 3], "b": [4, 5, None], "c": [None, 8, 9]}
    df = make_df(data).with_column("min", columns_min("a", "b", "c"))

    assert df.to_pydict()["min"] == [1, 5, 3]


def test_columns_min_with_expressions(make_df) -> None:
    data = {"a": [1, 2, 3], "b": [1, 2, 3], "c": [1, 2, 3]}
    df = make_df(data).with_column("min", columns_min(col("a") + 1, col("b") + 2, col("c") + 3))

    assert df.to_pydict()["min"] == [2, 3, 4]


def test_columns_min_in_select(make_df) -> None:
    df = make_df({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
    df = df.select(columns_min("a", "b", "c"))

    assert df.to_pydict()["columns_min"] == [1, 2, 3]


def test_columns_min_with_no_input_columns(make_df) -> None:
    df = make_df({"a": [1, 2, 3]})

    with pytest.raises(ValueError, match="columns_min requires at least one expression"):
        df.with_column("min", columns_min()).collect()


def test_columns_max(make_df) -> None:
    data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}
    df = make_df(data).with_column("max", columns_max("a", "b", "c"))

    assert df.to_pydict()["max"] == [7, 8, 9]


def test_columns_max_with_nulls(make_df) -> None:
    data = {"a": [1, None, 3], "b": [4, 5, None], "c": [None, 8, 9]}
    df = make_df(data).with_column("max", columns_max("a", "b", "c"))

    assert df.to_pydict()["max"] == [4, 8, 9]


def test_columns_max_with_expressions(make_df) -> None:
    data = {"a": [1, 2, 3], "b": [1, 2, 3], "c": [1, 2, 3]}
    df = make_df(data).with_column("max", columns_max(col("a") + 1, col("b") + 2, col("c") + 3))

    assert df.to_pydict()["max"] == [4, 5, 6]


def test_columns_max_in_select(make_df) -> None:
    df = make_df({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
    df = df.select(columns_max("a", "b", "c"))

    assert df.to_pydict()["columns_max"] == [7, 8, 9]


def test_columns_max_with_no_input_columns(make_df) -> None:
    df = make_df({"a": [1, 2, 3]})

    with pytest.raises(ValueError, match="columns_max requires at least one expression"):
        df.with_column("max", columns_max()).collect()
