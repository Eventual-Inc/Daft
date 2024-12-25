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
