from __future__ import annotations

import pytest

import daft
from daft import col


def test_explode_basic():
    df = daft.from_pydict({"x": [[1, 2, 3], [4, 5], [], [6], None]})
    exploded = df.select(col("x").explode())
    result = exploded.to_pydict()["x"]
    # Should flatten all lists into rows, skipping empty lists
    assert result == [1, 2, 3, 4, 5, None, 6, None]


def test_explode_with_other_columns():
    df = daft.from_pydict({"id": [1, 2], "vals": [[10, 20], [30]]})
    exploded = df.select(col("id"), col("vals").explode())
    result = exploded.to_pydict()
    assert result["id"] == [1, 1, 2]
    assert result["vals"] == [10, 20, 30]


def test_explode_non_list_column():
    df = daft.from_pydict({"x": [1, 2, 3]})
    with pytest.raises(Exception):
        df.select(col("x").explode()).to_pydict()


def test_explode_multi_column():
    df = daft.from_pydict({"x": [[1, 2, 3], [4, 5], [], None], "y": [[6, 7, 8], [9, 10], [], None]})
    exploded = df.select(col("x").explode(), col("y").explode())
    result = exploded.to_pydict()
    # Should flatten, including None, and skip None rows
    assert result == {"x": [1, 2, 3, 4, 5, None, None], "y": [6, 7, 8, 9, 10, None, None]}


def test_explode_mismatched_column():
    df = daft.from_pydict({"x": [[1, 2, 3]], "y": [[4, 5]]})
    exploded = df.select(col("x").explode(), col("y").explode())

    with pytest.raises(Exception, match="DaftError::ValueError In multicolumn explode, list length did not match"):
        exploded.collect()
