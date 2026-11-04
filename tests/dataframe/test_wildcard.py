from __future__ import annotations

import pytest

import daft
from daft import col


def test_wildcard_select():
    df = daft.from_pydict(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )

    res = df.select("*").to_pydict()
    assert res == {
        "a": [1, 2, 3],
        "b": [4, 5, 6],
    }


def test_wildcard_select_expr():
    df = daft.from_pydict(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )

    res = df.select(col("*") * 2).to_pydict()
    assert res == {
        "a": [2, 4, 6],
        "b": [8, 10, 12],
    }


def test_wildcard_select_with_structs():
    df = daft.from_pydict(
        {
            "a": [
                {"x": 1, "y": 2},
                {"x": 3, "y": 4},
            ],
            "b": [5, 6],
        }
    )

    res = df.select("*").to_pydict()
    assert res == {
        "a": [
            {"x": 1, "y": 2},
            {"x": 3, "y": 4},
        ],
        "b": [5, 6],
    }


def test_wildcard_in_struct_constructor():
    df = daft.from_pydict(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )

    res = df.select(daft.functions.to_struct(col("*"))).to_pydict()
    assert res == {
        "struct": [
            {"a": 1, "b": 4},
            {"a": 2, "b": 5},
            {"a": 3, "b": 6},
        ]
    }


def test_wildcard_in_struct_constructor_with_other_fields():
    df = daft.from_pydict(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )

    res = df.select(daft.functions.to_struct(col("*"), doubled=col("a") * 2)).to_pydict()
    assert res == {
        "struct": [
            {"a": 1, "b": 4, "doubled": 2},
            {"a": 2, "b": 5, "doubled": 4},
            {"a": 3, "b": 6, "doubled": 6},
        ]
    }


@pytest.mark.skip(reason="Sorting by wildcard columns is not supported")
def test_wildcard_sort():
    df = daft.from_pydict(
        {
            "a": [4, 2, 2, 1, 4],
            "b": [3, 5, 1, 6, 4],
        }
    )

    res = df.sort("*").to_pydict()
    assert res == {
        "a": [1, 2, 2, 4, 4],
        "b": [6, 1, 5, 3, 4],
    }


def test_wildcard_explode():
    df = daft.from_pydict(
        {
            "a": [[1, 2], [3, 4, 5]],
            "b": [[6, 7], [8, 9, 10]],
        }
    )

    res = df.explode("*").to_pydict()
    assert res == {
        "a": [1, 2, 3, 4, 5],
        "b": [6, 7, 8, 9, 10],
    }


def test_wildcard_agg():
    df = daft.from_pydict(
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
        }
    )

    res = df.sum("*").to_pydict()
    assert res == {
        "a": [6],
        "b": [15],
    }

    res = df.agg(col("*").mean()).to_pydict()
    assert res == {
        "a": [2],
        "b": [5],
    }
