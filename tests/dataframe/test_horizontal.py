from __future__ import annotations

import pytest

from daft.expressions import col
from daft.functions import (
    GREATEST,
    LEAST,
    all_of,
    any_of,
    columns_avg,
    columns_max,
    columns_mean,
    columns_min,
    columns_sum,
    max_of,
    mean_of,
    min_of,
    sum_of,
)


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


def test_min_of_matches_columns_min(make_df) -> None:
    data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}
    df = make_df(data).with_columns(
        {
            "min_of": min_of("a", "b", "c"),
            "columns_min": columns_min("a", "b", "c"),
        }
    )
    assert df.to_pydict()["min_of"] == df.to_pydict()["columns_min"]


def test_max_of_matches_columns_max(make_df) -> None:
    data = {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}
    df = make_df(data).with_columns(
        {
            "max_of": max_of("a", "b", "c"),
            "columns_max": columns_max("a", "b", "c"),
        }
    )
    assert df.to_pydict()["max_of"] == df.to_pydict()["columns_max"]


def test_sum_of_matches_columns_sum(make_df) -> None:
    data = {"a": [1, None, 3], "b": [4, 5, None], "c": [None, 8, 9]}
    df = make_df(data).with_columns(
        {
            "sum_of": sum_of("a", "b", "c"),
            "columns_sum": columns_sum("a", "b", "c"),
        }
    )
    assert df.to_pydict()["sum_of"] == df.to_pydict()["columns_sum"]


def test_mean_of_matches_columns_mean(make_df) -> None:
    data = {"a": [1, None, 3], "b": [4, 5, None], "c": [None, 8, 9]}
    df = make_df(data).with_columns(
        {
            "mean_of": mean_of("a", "b", "c"),
            "columns_mean": columns_mean("a", "b", "c"),
        }
    )
    assert df.to_pydict()["mean_of"] == df.to_pydict()["columns_mean"]


def test_all_of_any_of_boolean(make_df) -> None:
    data = {"a": [True, None, True, None], "b": [True, False, None, None], "c": [None, False, None, True]}
    df = make_df(data).select(
        all_of("a", "b", "c").alias("all_of"),
        any_of("a", "b", "c").alias("any_of"),
    )
    out = df.to_pydict()
    assert out["all_of"] == [True, False, True, True]
    assert out["any_of"] == [True, False, True, True]


def test_least_greatest_aliases(make_df) -> None:
    data = {"a": [1, 2, 3], "b": [4, 1, 2], "c": [0, 5, 6]}
    df_aliases = make_df(data).select(
        LEAST("a", "b", "c").alias("least"),
        GREATEST("a", "b", "c").alias("greatest"),
    )
    out_aliases = df_aliases.to_pydict()

    df_minmax = make_df(data).select(
        min_of("a", "b", "c").alias("min_of"),
        max_of("a", "b", "c").alias("max_of"),
    )
    out_minmax = df_minmax.to_pydict()

    assert out_aliases["least"] == out_minmax["min_of"]
    assert out_aliases["greatest"] == out_minmax["max_of"]


def test_min_of_requires_2_expressions(make_df) -> None:
    df = make_df({"a": [1, 2, 3]})
    with pytest.raises(ValueError, match="min_of requires at least 2 expressions"):
        df.with_column("min_of", min_of()).collect()
    with pytest.raises(ValueError, match="min_of requires at least 2 expressions"):
        df.with_column("min_of", min_of("a")).collect()


def test_max_of_requires_2_expressions(make_df) -> None:
    df = make_df({"a": [1, 2, 3]})
    with pytest.raises(ValueError, match="max_of requires at least 2 expressions"):
        df.with_column("max_of", max_of()).collect()
    with pytest.raises(ValueError, match="max_of requires at least 2 expressions"):
        df.with_column("max_of", max_of("a")).collect()


def test_sum_of_requires_2_expressions(make_df) -> None:
    df = make_df({"a": [1, 2, 3]})
    with pytest.raises(ValueError, match="sum_of requires at least 2 expressions"):
        df.with_column("sum_of", sum_of()).collect()
    with pytest.raises(ValueError, match="sum_of requires at least 2 expressions"):
        df.with_column("sum_of", sum_of("a")).collect()


def test_mean_of_requires_2_expressions(make_df) -> None:
    df = make_df({"a": [1, 2, 3]})
    with pytest.raises(ValueError, match="mean_of requires at least 2 expressions"):
        df.with_column("mean_of", mean_of()).collect()
    with pytest.raises(ValueError, match="mean_of requires at least 2 expressions"):
        df.with_column("mean_of", mean_of("a")).collect()


def test_all_of_requires_2_expressions(make_df) -> None:
    df = make_df({"a": [True, False, None]})
    with pytest.raises(ValueError, match="all_of requires at least 2 expressions"):
        df.with_column("all_of", all_of()).collect()
    with pytest.raises(ValueError, match="all_of requires at least 2 expressions"):
        df.with_column("all_of", all_of("a")).collect()


def test_any_of_requires_2_expressions(make_df) -> None:
    df = make_df({"a": [True, False, None]})
    with pytest.raises(ValueError, match="any_of requires at least 2 expressions"):
        df.with_column("any_of", any_of()).collect()
    with pytest.raises(ValueError, match="any_of requires at least 2 expressions"):
        df.with_column("any_of", any_of("a")).collect()
