from __future__ import annotations

from typing import Any

import pytest

import daft
from daft import DataFrame, col
from tests.conftest import check_answer


def helper(make_df, op: str, left: dict[str, Any], right: dict[str, Any], expected: dict[str, Any]):
    df1 = make_df(left)
    df2 = make_df(right)
    df_helper(op, df1, df2, expected)


def df_helper(op: str, df1: DataFrame, df2: DataFrame, expected: dict[str, Any]):
    if op == "intersect":
        result = df1.intersect(df2)
    elif op == "except_distinct":
        result = df1.except_distinct(df2)
    elif op == "intersect_all":
        result = df1.intersect_all(df2)
    else:
        result = df1.except_all(df2)
    check_answer(result, expected)


@pytest.mark.parametrize(
    "op, left, right, expected",
    [
        ("intersect", {"foo": [1, 2, 3]}, {"bar": [2, 3, 4]}, {"foo": [2, 3]}),
        ("intersect_all", {"foo": [1, 2, 2]}, {"bar": [2, 2, 4]}, {"foo": [2, 2]}),
        ("except_distinct", {"foo": [1, 2, 3]}, {"bar": [2, 3, 4]}, {"foo": [1]}),
        ("except_all", {"foo": [1, 2, 2]}, {"bar": [2, 4]}, {"foo": [1, 2]}),
    ],
)
def test_simple_intersect_or_except(make_df, op, left, right, expected):
    helper(make_df, op, left, right, expected)


@pytest.mark.parametrize(
    "op, left, right, expected",
    [
        ("intersect", {"foo": [1, 2, 2, 3]}, {"bar": [2, 3, 3]}, {"foo": [2, 3]}),
        ("intersect_all", {"foo": [1, 2, 2, 3]}, {"bar": [2, 3, 3]}, {"foo": [2, 3]}),
        ("except_distinct", {"foo": [1, 2, 2, 3]}, {"bar": [2, 3, 3]}, {"foo": [1]}),
        ("except_all", {"foo": [1, 2, 2, 3]}, {"bar": [2, 3, 3]}, {"foo": [1, 2]}),
    ],
)
def test_with_duplicate(make_df, op, left, right, expected):
    helper(make_df, op, left, right, expected)


@pytest.mark.parametrize(
    "op, df, expected",
    [
        ("intersect", {"foo": [1, 2, 3]}, {"foo": [1, 2, 3]}),
        ("intersect_all", {"foo": [1, 2, 3]}, {"foo": [1, 2, 3]}),
        ("except_distinct", {"foo": [1, 2, 3]}, {"foo": []}),
        ("except_all", {"foo": [1, 2, 2]}, {"foo": []}),
    ],
)
def test_with_self(make_df, op, df, expected):
    df = make_df(df)
    df_helper(op, df, df, expected)


@pytest.mark.parametrize(
    "op, left, expected",
    [
        ("intersect", {"foo": [1, 2, 3]}, {"foo": []}),
        ("intersect_all", {"foo": [1, 2, 3]}, {"foo": []}),
        ("except_distinct", {"foo": [1, 2, 3]}, {"foo": [1, 2, 3]}),
        ("except_all", {"foo": [1, 2, 2]}, {"foo": [1, 2, 2]}),
    ],
)
def test_with_empty(make_df, op, left, expected):
    df1 = make_df(left)
    df2 = make_df({"bar": []}).select(col("bar").cast(daft.DataType.int64()))
    df_helper(op, df1, df2, expected)


@pytest.mark.parametrize(
    "op, left, right, expected",
    [
        ("intersect", {"foo": [1, 2, None]}, {"foo": [2, 3, None]}, {"foo": [2, None]}),
        ("intersect_all", {"foo": [1, 2, None]}, {"foo": [2, 3, None]}, {"foo": [2, None]}),
        ("intersect", {"foo": [1, 2]}, {"foo": [2, 3, None]}, {"foo": [2]}),
        ("intersect_all", {"foo": [1, 2]}, {"foo": [2, 3, None]}, {"foo": [2]}),
        ("intersect", {"foo": [1, 2, None]}, {"foo": [2, 3]}, {"foo": [2]}),
        ("intersect_all", {"foo": [1, 2, None]}, {"foo": [2, 3]}, {"foo": [2]}),
    ],
)
def test_intersect_with_nulls(make_df, op, left, right, expected):
    helper(make_df, op, left, right, expected)


@pytest.mark.parametrize(
    "op, left, right, expected",
    [
        ("except_distinct", {"foo": [1, 2, None]}, {"foo": [2, 3, None]}, {"foo": [1]}),
        ("except_all", {"foo": [1, 2, None]}, {"foo": [2, 3, None]}, {"foo": [1]}),
        ("except_distinct", {"foo": [1, 2]}, {"foo": [2, 3, None]}, {"foo": [1]}),
        ("except_all", {"foo": [1, 2]}, {"foo": [2, 3, None]}, {"foo": [1]}),
        ("except_distinct", {"foo": [1, 2, None]}, {"foo": [2, 3]}, {"foo": [1, None]}),
        ("except_all", {"foo": [1, 2, None]}, {"foo": [2, 3]}, {"foo": [1, None]}),
    ],
)
def test_except_with_nulls(make_df, op, left, right, expected):
    helper(make_df, op, left, right, expected)


@pytest.mark.parametrize(
    "op, left, right, expected",
    [
        (
            "intersect_all",
            {"foo": [1, 2, 2], "bar": [2, 3, 3]},
            {"a": [2, 2, 4], "b": [3, 3, 4]},
            {"foo": [2, 2], "bar": [3, 3]},
        ),
        (
            "except_all",
            {"foo": [1, 2, 2], "bar": [2, 3, 3]},
            {"a": [2, 2, 4], "b": [3, 3, 4]},
            {"foo": [1], "bar": [2]},
        ),
    ],
)
def test_multiple_fields(make_df, op, left, right, expected):
    helper(make_df, op, left, right, expected)


def test_union():
    df1 = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    df2 = daft.from_pydict({"x": [3, 4, 5], "y": [6, 7, 8]})
    expected = {"x": [1, 2, 3, 4, 5], "y": [4, 5, 6, 7, 8]}
    actual = df1.union(df2).sort("x").to_pydict()
    actual_sql = daft.sql("select * from df1 union select * from df2").sort("x").to_pydict()
    assert actual == expected
    assert actual_sql == expected


@pytest.mark.parametrize("quantifier", [None, "all"])
def test_union_fails_for_different_schemas(quantifier):
    df1 = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    df2 = daft.from_pydict({"x": [3, 4, 5], "z": [6, 7, 8]})
    with pytest.raises(daft.exceptions.DaftCoreException):
        if quantifier == "all":
            df1.union_all(df2).collect()
        else:
            df1.union(df2).collect()


def test_union_all():
    df1 = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
    df2 = daft.from_pydict({"x": [3, 2, 1], "y": [6, 5, 4]})
    expected = {"x": [1, 1, 2, 2, 3, 3], "y": [4, 4, 5, 5, 6, 6]}
    actual = df1.union_all(df2).sort("x").to_pydict()
    actual_sql = daft.sql("select * from df1 union all select * from df2").sort("x").to_pydict()
    assert actual == expected
    assert actual_sql == expected


def test_union_by_name():
    df1 = daft.from_pydict({"name": ["Alice", "Bob", "Bob"], "age": [25, 30, 30], "city": ["NY", "LA", "LA"]})

    df2 = daft.from_pydict(
        {
            "name": ["Bob", "Carol", "Bob"],
            "salary": [50000, 60000, 50000],
            "city": ["LA", "SF", "LA"],
            "occupation": ["barista", "cashier", "barista"],
        }
    )

    expected = {
        "name": ["Alice", "Bob", "Bob", "Carol"],
        "age": [25, None, 30, None],
        "city": ["NY", "LA", "LA", "SF"],
        "salary": [None, 50000, None, 60000],
        "occupation": [None, "barista", None, "cashier"],
    }

    actual_sql = (
        daft.sql("select * from df1 union by name select * from df2")
        .sort(by=["name", "city", "occupation"])
        .to_pydict()
    )
    actual_df = df1.union_by_name(df2).sort(by=["name", "city", "occupation"]).to_pydict()

    assert actual_sql == expected
    assert actual_df == expected


def test_union_all_by_name():
    df1 = daft.from_pydict({"name": ["Alice", "Bob", "Bob"], "age": [25, 30, 30], "city": ["NY", "LA", "LA"]})

    df2 = daft.from_pydict(
        {
            "name": ["Bob", "Carol", "Bob"],
            "salary": [50000, 60000, 50000],
            "city": ["LA", "SF", "LA"],
            "occupation": ["barista", "cashier", "barista"],
        }
    )

    expected = {
        "name": ["Alice", "Bob", "Bob", "Bob", "Bob", "Carol"],
        "age": [25, None, None, 30, 30, None],
        "city": ["NY", "LA", "LA", "LA", "LA", "SF"],
        "salary": [None, 50000, 50000, None, None, 60000],
        "occupation": [None, "barista", "barista", None, None, "cashier"],
    }

    actual_sql = (
        daft.sql("select * from df1 union all by name select * from df2")
        .sort(by=["name", "city", "occupation"])
        .to_pydict()
    )
    actual_df = df1.union_all_by_name(df2).sort(by=["name", "city", "occupation"]).to_pydict()

    assert actual_sql == expected
    assert actual_df == expected
