from __future__ import annotations

import copy
from datetime import date, datetime

import pytest
import pytz

from daft.datatype import DataType, TimeUnit
from daft.expressions import col, lit
from daft.expressions.testing import expr_structurally_equal
from daft.series import Series
from daft.table import MicroPartition


@pytest.mark.parametrize(
    "data, expected_dtype",
    [
        (1, DataType.int32()),
        (2**32, DataType.int64()),
        (1 << 63, DataType.uint64()),
        (1.2, DataType.float64()),
        ("a", DataType.string()),
        (b"a", DataType.binary()),
        (True, DataType.bool()),
        (None, DataType.null()),
        (Series.from_pylist([1, 2, 3]), DataType.int64()),
        (date(2023, 1, 1), DataType.date()),
        (datetime(2023, 1, 1), DataType.timestamp(timeunit=TimeUnit.from_str("us"))),
        (datetime(2022, 1, 1, tzinfo=pytz.utc), DataType.timestamp(timeunit=TimeUnit.from_str("us"), timezone="UTC")),
    ],
)
def test_make_lit(data, expected_dtype) -> None:
    l = lit(data)
    assert l.name() == "literal"
    empty_table = MicroPartition.empty()
    lit_table = empty_table.eval_expression_list([l])
    series = lit_table.get_column("literal")
    assert series.datatype() == expected_dtype
    repr_out = repr(l)

    assert repr_out.startswith("lit(")
    assert repr_out.endswith(")")
    copied = copy.deepcopy(l)
    assert repr_out == repr(copied)


import operator as ops

OPS = [
    (ops.add, "+"),
    (ops.sub, "-"),
    (ops.mul, "*"),
    (ops.truediv, "/"),
    (ops.mod, "%"),
    (ops.lt, "<"),
    (ops.le, "<="),
    (ops.eq, "=="),
    (ops.ne, "!="),
    (ops.ge, ">="),
    (ops.gt, ">"),
]


@pytest.mark.parametrize("op, symbol", OPS)
def test_repr_binary_operators(op, symbol) -> None:
    a = col("a")
    b = col("b")
    y = op(a, b)
    output = repr(y)
    tokens = output.split(" ")
    assert len(tokens) == 3
    assert tokens[0] == "col(a)"
    assert tokens[1] == symbol
    assert tokens[2] == "col(b)"
    copied = copy.deepcopy(y)
    assert output == repr(copied)


def test_repr_functions_abs() -> None:
    a = col("a")
    y = abs(a)
    repr_out = repr(y)
    assert repr_out == "abs(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_day() -> None:
    a = col("a")
    y = a.dt.day()
    repr_out = repr(y)
    assert repr_out == "day(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_month() -> None:
    a = col("a")
    y = a.dt.month()
    repr_out = repr(y)
    assert repr_out == "month(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_year() -> None:
    a = col("a")
    y = a.dt.year()
    repr_out = repr(y)
    assert repr_out == "year(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_day_of_week() -> None:
    a = col("a")
    y = a.dt.day_of_week()
    repr_out = repr(y)
    assert repr_out == "day_of_week(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_expr_structurally_equal() -> None:
    e1 = (col("a")._max() == col("b").alias("moo") - 3).is_null()
    e2 = (col("a")._max() == col("b").alias("moo") - 3).is_null()
    e3 = (col("a")._max() == col("b").alias("moo") - 4).is_null()
    assert expr_structurally_equal(e1, e2)
    assert not expr_structurally_equal(e2, e3)


def test_str_concat_delegation() -> None:
    a = col("a")
    b = "foo"
    c = a.str.concat(b)
    expected = a + lit(b)
    assert expr_structurally_equal(c, expected)
    output = repr(c)
    assert output == 'col(a) + lit("foo")'


def test_float_is_nan() -> None:
    a = col("a")
    c = a.float.is_nan()
    output = repr(c)
    assert output == "is_nan(col(a))"


def test_date_lit_post_epoch() -> None:
    d = lit(date(2022, 1, 1))
    output = repr(d)
    assert output == "lit(2022-01-01)"


def test_date_lit_pre_epoch() -> None:
    d = lit(date(1950, 1, 1))
    output = repr(d)
    assert output == "lit(1950-01-01)"


def test_datetime_lit_post_epoch() -> None:
    d = lit(datetime(2022, 1, 1))
    output = repr(d)
    assert output == "lit(2022-01-01T00:00:00.000000)"


def test_datetime_tz_lit_post_epoch() -> None:
    d = lit(datetime(2022, 1, 1, tzinfo=pytz.utc))
    output = repr(d)
    assert output == "lit(2022-01-01T00:00:00.000000+00:00)"


def test_datetime_lit_pre_epoch() -> None:
    d = lit(datetime(1950, 1, 1))
    output = repr(d)
    assert output == "lit(1950-01-01T00:00:00.000000)"


def test_repr_series_lit() -> None:
    s = lit(Series.from_pylist([1, 2, 3]))
    output = repr(s)
    assert output == "lit([1, 2, 3])"
