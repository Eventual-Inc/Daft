from __future__ import annotations

import pytest

import daft
from daft import col, lit


def test_select_global_agg_returns_single_row() -> None:
    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})

    res = df.select(col("a").sum().alias("sum_a")).collect().to_pydict()

    assert res == {"sum_a": [6]}


def test_select_global_agg_allows_literals() -> None:
    df = daft.from_pydict({"a": [1, 2, 3]})

    res = (
        df.select(
            col("a").sum().alias("sum_a"),
            lit(1).alias("one"),
        )
        .collect()
        .to_pydict()
    )

    assert res == {"sum_a": [6], "one": [1]}


def test_select_global_agg_allows_multiple_aggs() -> None:
    df = daft.from_pydict({"a": [1, 2, 3]})

    res = (
        df.select(
            col("a").sum().alias("sum_a"),
            col("a").count().alias("cnt_a"),
        )
        .collect()
        .to_pydict()
    )

    assert res == {"sum_a": [6], "cnt_a": [3]}


def test_select_global_agg_without_alias() -> None:
    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})

    res = df.select(col("a").sum()).collect().to_pydict()

    assert res == {"a": [6]}


def test_select_global_agg_rejects_non_agg_column_reference() -> None:
    df = daft.from_pydict({"a": [1, 2, 3]})

    with pytest.raises(ValueError, match="Expressions in aggregations"):
        df.select(col("a").sum().alias("sum_a"), col("a")).collect()
