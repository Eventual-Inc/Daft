from __future__ import annotations

import copy
from datetime import date, datetime, time, timedelta, timezone

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
        (time(1, 2, 3, 4), DataType.time(timeunit=TimeUnit.from_str("us"))),
        (datetime(2023, 1, 1), DataType.timestamp(timeunit=TimeUnit.from_str("us"))),
        (
            datetime(2022, 1, 1, tzinfo=pytz.utc),
            DataType.timestamp(timeunit=TimeUnit.from_str("us"), timezone="UTC"),
        ),
    ],
)
def test_make_lit(data, expected_dtype) -> None:
    literal = lit(data)
    assert literal.name() == "literal"
    empty_table = MicroPartition.empty()
    lit_table = empty_table.eval_expression_list([literal])
    series = lit_table.get_column("literal")
    assert series.datatype() == expected_dtype
    repr_out = repr(literal)

    assert repr_out.startswith("lit(")
    assert repr_out.endswith(")")
    copied = copy.deepcopy(literal)
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


def test_repr_functions_ceil() -> None:
    a = col("a")
    y = a.ceil()
    repr_out = repr(y)
    assert repr_out == "ceil(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_floor() -> None:
    a = col("a")
    y = a.floor()
    repr_out = repr(y)
    assert repr_out == "floor(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_sign() -> None:
    a = col("a")
    y = a.sign()
    repr_out = repr(y)
    assert repr_out == "sign(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_round() -> None:
    a = col("a")
    y = a.round()
    repr_out = repr(y)
    assert repr_out == "round(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_log2() -> None:
    a = col("a")
    y = a.log2()
    repr_out = repr(y)
    assert repr_out == "log2(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_log10() -> None:
    a = col("a")
    y = a.log10()
    repr_out = repr(y)
    assert repr_out == "log10(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_log() -> None:
    a = col("a")
    y = a.log()
    repr_out = repr(y)
    assert repr_out == "log(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_ln() -> None:
    a = col("a")
    y = a.ln()
    repr_out = repr(y)
    assert repr_out == "ln(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_shift_left() -> None:
    a = col("a")
    b = col("b")
    y = a << (b)
    repr_out = repr(y)
    assert repr_out == "col(a) << col(b)"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_shift_right() -> None:
    a = col("a")
    b = col("b")
    y = a >> (b)
    repr_out = repr(y)
    assert repr_out == "col(a) >> col(b)"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


@pytest.mark.parametrize(
    "fun",
    [
        "sin",
        "cos",
        "tan",
        "cot",
        "arcsin",
        "arccos",
        "arctan",
        "radians",
        "degrees",
    ],
)
def test_repr_functions_trigonometry(fun: str) -> None:
    a = col("a")
    y = getattr(a, fun)()
    repr_out = repr(y)
    assert repr_out == f"{fun}(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_atan2() -> None:
    a = col("a")
    b = col("b")
    y = a.arctan2(b)
    repr_out = repr(y)
    assert repr_out == "atan2(col(a), col(b))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_arctanh() -> None:
    a = col("a")
    y = a.arctanh()
    repr_out = repr(y)
    assert repr_out == "arctanh(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_arccosh() -> None:
    a = col("a")
    y = a.arccosh()
    repr_out = repr(y)
    assert repr_out == "arccosh(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_arcsinh() -> None:
    a = col("a")
    y = a.arcsinh()
    repr_out = repr(y)
    assert repr_out == "arcsinh(col(a))"
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


def test_repr_functions_exp() -> None:
    a = col("a")
    y = a.exp()
    repr_out = repr(y)
    assert repr_out == "exp(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_sqrt() -> None:
    a = col("a")
    y = a.sqrt()
    repr_out = repr(y)
    assert repr_out == "sqrt(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_hash() -> None:
    a = col("a")
    y = a.hash()
    repr_out = repr(y)
    assert repr_out == "hash(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_hash_2() -> None:
    a = col("a")
    y = a.hash(lit(1))
    repr_out = repr(y)
    assert repr_out == "hash(col(a), lit(1))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_minhash() -> None:
    a = col("a")
    y = a.minhash(1, 2)
    repr_out = repr(y)
    assert repr_out == "minhash(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_minhash_2() -> None:
    a = col("a")
    y = a.minhash(1, 2, 3)
    repr_out = repr(y)
    assert repr_out == "minhash(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_tokenize_encode() -> None:
    a = col("a")
    y = a.str.tokenize_encode("cl100k_base")
    repr_out = repr(y)
    assert repr_out == "tokenize_encode(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_tokenize_decode() -> None:
    a = col("a")
    y = a.str.tokenize_decode("cl100k_base")
    repr_out = repr(y)
    assert repr_out == "tokenize_decode(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_expr_structurally_equal() -> None:
    e1 = (col("a").max() == col("b").alias("moo") - 3).is_null()
    e2 = (col("a").max() == col("b").alias("moo") - 3).is_null()
    e3 = (col("a").max() == col("b").alias("moo") - 4).is_null()
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


def test_float_is_inf() -> None:
    a = col("a")
    c = a.float.is_inf()
    output = repr(c)
    assert output == "is_inf(col(a))"


def test_float_not_nan() -> None:
    a = col("a")
    c = a.float.not_nan()
    output = repr(c)
    assert output == "not_nan(col(a))"


def test_date_lit_post_epoch() -> None:
    d = lit(date(2022, 1, 1))
    output = repr(d)
    assert output == "lit(2022-01-01)"


def test_date_lit_pre_epoch() -> None:
    d = lit(date(1950, 1, 1))
    output = repr(d)
    assert output == "lit(1950-01-01)"


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            datetime(2022, 1, 1, 12, 30, 59, 0),
            "lit(2022-01-01 12:30:59)",
        ),
        (
            datetime(2022, 1, 1, 12, 30, 59, 123456),
            "lit(2022-01-01 12:30:59.123456)",
        ),
    ],
)
def test_datetime_lit_post_epoch(input, expected) -> None:
    d = lit(input)
    output = repr(d)
    assert output == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            pytz.timezone("US/Eastern").localize(datetime(2022, 1, 1, 12, 30, 59, 0)),
            "lit(2022-01-01 12:30:59 EST)",
        ),
        (
            datetime(2022, 1, 1, 12, 30, 59, 123456, tzinfo=timezone(timedelta(hours=9))),
            "lit(2022-01-01 12:30:59.123456 +09:00)",
        ),
    ],
)
def test_datetime_tz_lit_post_epoch(input, expected) -> None:
    d = lit(input)
    output = repr(d)
    assert output == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            datetime(1950, 1, 1, 12, 30, 59, 0),
            "lit(1950-01-01 12:30:59)",
        ),
        (
            datetime(1950, 1, 1, 12, 30, 59, 123456),
            "lit(1950-01-01 12:30:59.123456)",
        ),
    ],
)
def test_datetime_lit_pre_epoch(input, expected) -> None:
    d = lit(input)
    output = repr(d)
    assert output == expected


@pytest.mark.parametrize(
    "timeunit, expected",
    [
        (
            "s",
            "1970-01-01 00:00:01",
        ),
        (
            "ms",
            "1970-01-01 00:00:00.001",
        ),
        (
            "us",
            "1970-01-01 00:00:00.000001",
        ),
        (
            "ns",
            "1970-01-01 00:00:00.000000001",
        ),
    ],
)
def test_datetime_lit_different_timeunits(timeunit, expected) -> None:
    import pyarrow as pa

    pa_array = pa.array([1], type=pa.timestamp(timeunit))
    pa_table = pa.table({"dt": pa_array})
    mp = MicroPartition.from_arrow(pa_table)

    series = mp.get_column("dt")
    output = repr(series)

    # Series repr is very long, so we just check the last line which contains the timestamp
    # Also remove the │ │, which is what the [1:-1] and strip is doing
    timestamp_repr = output.split("\n")[-3][1:-1].strip()
    assert timestamp_repr == expected


def test_repr_series_lit() -> None:
    s = lit(Series.from_pylist([1, 2, 3]))
    output = repr(s)
    assert output == "lit([1, 2, 3])"
