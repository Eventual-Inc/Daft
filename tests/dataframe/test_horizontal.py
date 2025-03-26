from __future__ import annotations

import pytest

from daft.expressions import col
from daft.functions import (
    max_horizontal,
    mean_horizontal,
    min_horizontal,
    sum_horizontal,
)


def test_sum_horizontal(make_df) -> None:
    data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}
    df = make_df(data).with_column("sum", sum_horizontal("a", "b", "c"))

    assert df.to_pydict()["sum"] == [12, 15, 18]


def test_sum_horizontal_with_nulls(make_df) -> None:
    data = {"a": [1, None, 3], "b": [4, 5, None], "c": [None, 8, 9]}
    df = make_df(data).with_column("sum", sum_horizontal("a", "b", "c"))

    assert df.to_pydict()["sum"] == [5, 13, 12]


def test_sum_horizontal_with_expressions(make_df) -> None:
    data = {"a": [1, 2, 3], "b": [1, 2, 3], "c": [1, 2, 3]}
    df = make_df(data).with_column("sum", sum_horizontal(col("a") + 1, col("b") + 2, col("c") + 3))

    assert df.to_pydict()["sum"] == [9, 12, 15]


def test_sum_horizontal_with_no_input_columns(make_df) -> None:
    df = make_df({"a": [1, 2, 3]})

    with pytest.raises(ValueError, match="sum_horizontal requires at least one expression"):
        df.with_column("sum", sum_horizontal()).collect()


def test_mean_horizontal(make_df) -> None:
    data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}
    df = make_df(data).with_column("mean", mean_horizontal("a", "b", "c"))

    assert df.to_pydict()["mean"] == [4.0, 5.0, 6.0]


def test_mean_horizontal_with_nulls(make_df) -> None:
    data = {"a": [1, None, None], "b": [None, 5, None], "c": [None, None, 9]}
    df = make_df(data).with_column("mean", mean_horizontal("a", "b", "c"))

    assert df.to_pydict()["mean"] == [1.0, 5.0, 9.0]


def test_mean_horizontal_with_expressions(make_df) -> None:
    data = {"a": [1, 2, 3], "b": [1, 2, 3], "c": [1, 2, 3]}
    df = make_df(data).with_column("mean", mean_horizontal(col("a") + 1, col("b") + 2, col("c") + 3))

    assert df.to_pydict()["mean"] == [3.0, 4.0, 5.0]


def test_mean_horizontal_with_no_input_columns(make_df) -> None:
    df = make_df({"a": [1, 2, 3]})

    with pytest.raises(ValueError, match="mean_horizontal requires at least one expression"):
        df.with_column("mean", mean_horizontal()).collect()


def test_min_horizontal(make_df) -> None:
    data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}
    df = make_df(data).with_column("min", min_horizontal("a", "b", "c"))

    assert df.to_pydict()["min"] == [1, 2, 3]


def test_min_horizontal_with_nulls(make_df) -> None:
    data = {"a": [1, None, 3], "b": [4, 5, None], "c": [None, 8, 9]}
    df = make_df(data).with_column("min", min_horizontal("a", "b", "c"))

    assert df.to_pydict()["min"] == [1, 5, 3]


def test_min_horizontal_with_expressions(make_df) -> None:
    data = {"a": [1, 2, 3], "b": [1, 2, 3], "c": [1, 2, 3]}
    df = make_df(data).with_column("min", min_horizontal(col("a") + 1, col("b") + 2, col("c") + 3))

    assert df.to_pydict()["min"] == [2, 3, 4]


def test_min_horizontal_with_no_input_columns(make_df) -> None:
    df = make_df({"a": [1, 2, 3]})

    with pytest.raises(ValueError, match="min_horizontal requires at least one expression"):
        df.with_column("min", min_horizontal()).collect()


def test_max_horizontal(make_df) -> None:
    data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}
    df = make_df(data).with_column("max", max_horizontal("a", "b", "c"))

    assert df.to_pydict()["max"] == [7, 8, 9]


def test_max_horizontal_with_nulls(make_df) -> None:
    data = {"a": [1, None, 3], "b": [4, 5, None], "c": [None, 8, 9]}
    df = make_df(data).with_column("max", max_horizontal("a", "b", "c"))

    assert df.to_pydict()["max"] == [4, 8, 9]


def test_max_horizontal_with_expressions(make_df) -> None:
    data = {"a": [1, 2, 3], "b": [1, 2, 3], "c": [1, 2, 3]}
    df = make_df(data).with_column("max", max_horizontal(col("a") + 1, col("b") + 2, col("c") + 3))

    assert df.to_pydict()["max"] == [4, 5, 6]


def test_max_horizontal_with_no_input_columns(make_df) -> None:
    df = make_df({"a": [1, 2, 3]})

    with pytest.raises(ValueError, match="max_horizontal requires at least one expression"):
        df.with_column("max", max_horizontal()).collect()
