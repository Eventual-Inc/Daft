from __future__ import annotations

from typing import Any

import pytest

import daft
from tests.conftest import check_answer


def make_spark_df(spark, data: dict[str, Any]):
    fields = [name for name in data]
    rows = list(zip(*[data[name] for name in fields]))
    return spark.createDataFrame(rows, fields)


def helper(spark, make_df, op: str, left: dict[str, Any], right: dict[str, Any], expected: dict[str, Any]):
    df1 = make_spark_df(spark, left)
    df2 = make_spark_df(spark, right)
    df_helper(op, df1, df2, expected)


def df_helper(op: str, df1, df2, expected: dict[str, Any]):
    if op == "intersect":
        result = df1.intersect(df2)
    elif op == "intersect_all":
        result = df1.intersectAll(df2)
    else:
        result = df1.exceptAll(df2)
    result = daft.from_pandas(result.toPandas())
    check_answer(result, expected)


@pytest.mark.parametrize(
    "op, left, right, expected",
    [
        ("intersect", {"foo": [1, 2, 3]}, {"bar": [2, 3, 4]}, {"foo": [2, 3]}),
        ("intersect_all", {"foo": [1, 2, 2]}, {"bar": [2, 2, 4]}, {"foo": [2, 2]}),
        ("except_all", {"foo": [1, 2, 2]}, {"bar": [2, 4]}, {"foo": [1, 2]}),
    ],
)
def test_simple_intersect_or_except(spark_session, make_df, op, left, right, expected):
    helper(spark_session, make_df, op, left, right, expected)


@pytest.mark.parametrize(
    "op, left, right, expected",
    [
        ("intersect", {"foo": [1, 2, 2, 3]}, {"bar": [2, 3, 3]}, {"foo": [2, 3]}),
        ("intersect_all", {"foo": [1, 2, 2, 3]}, {"bar": [2, 3, 3]}, {"foo": [2, 3]}),
        ("except_all", {"foo": [1, 2, 2, 3]}, {"bar": [2, 3, 3]}, {"foo": [1, 2]}),
    ],
)
def test_with_duplicate(spark_session, make_df, op, left, right, expected):
    helper(spark_session, make_df, op, left, right, expected)


@pytest.mark.parametrize(
    "op, df, expected",
    [
        ("intersect", {"foo": [1, 2, 3]}, {"foo": [1, 2, 3]}),
        ("intersect_all", {"foo": [1, 2, 3]}, {"foo": [1, 2, 3]}),
        ("except_all", {"foo": [1, 2, 2]}, {"foo": []}),
    ],
)
def test_with_self(spark_session, make_df, op, df, expected):
    df = make_spark_df(spark_session, df)
    df_helper(op, df, df, expected)


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
def test_intersect_with_nulls(spark_session, make_df, op, left, right, expected):
    helper(spark_session, make_df, op, left, right, expected)


@pytest.mark.parametrize(
    "op, left, right, expected",
    [
        ("except_all", {"foo": [1, 2, None]}, {"foo": [2, 3, None]}, {"foo": [1]}),
        ("except_all", {"foo": [1, 2]}, {"foo": [2, 3, None]}, {"foo": [1]}),
        ("except_all", {"foo": [1, 2, None]}, {"foo": [2, 3]}, {"foo": [1, None]}),
    ],
)
def test_except_with_nulls(spark_session, make_df, op, left, right, expected):
    helper(spark_session, make_df, op, left, right, expected)


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
def test_multiple_fields(spark_session, make_df, op, left, right, expected):
    helper(spark_session, make_df, op, left, right, expected)
