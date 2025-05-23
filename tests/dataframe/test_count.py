# corner cases for df.count()
from __future__ import annotations

import pytest

import daft
from daft import col


def test_count_star() -> None:
    df = daft.from_pydict({"a": [1, 2, 3, 4], "b": [1, None, 3, None]})
    res = df.count("*").to_pydict()
    assert res == {"count": [4]}


def test_count_col_star() -> None:
    df = daft.from_pydict({"a": [1, 2, 3, 4], "b": [1, None, 3, None]})
    res = df.count(col("*")).to_pydict()
    assert res == {"a": [4], "b": [2]}


def test_count_mixed_col_star() -> None:
    df = daft.from_pydict({"a": [1, 2, 3, 4], "b": [1, None, 3, None]})
    res = df.count(col("*"), col("a").alias("c")).to_pydict()
    assert res == {"a": [4], "b": [2], "c": [4]}


def test_count_mixed_star() -> None:
    df = daft.from_pydict({"a": [1, 2, 3, 4], "b": [1, None, 3, None]})
    with pytest.raises(ValueError, match=r"Cannot call count\(\) with both \* and column names"):
        df.count("*", "a")
