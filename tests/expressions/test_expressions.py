from __future__ import annotations

import copy
from datetime import date, datetime, time, timedelta, timezone

import pytest
import pytz

import daft
from daft.datatype import DataType, TimeUnit
from daft.expressions import col, lit
from daft.expressions.testing import expr_structurally_equal
from daft.recordbatch import MicroPartition
from daft.series import Series


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
        (Series.from_pylist([1, 2, 3]), DataType.list(DataType.int64())),
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
    series = lit_table.get_column_by_name("literal")
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


def test_repr_functions_signum() -> None:
    a = col("a")
    y = a.signum()
    repr_out = repr(y)
    assert repr_out == "sign(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_negate() -> None:
    a = col("a")
    y = a.negate()
    repr_out = repr(y)
    assert repr_out == "negative(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_negative() -> None:
    a = col("a")
    y = a.negative()
    repr_out = repr(y)
    assert repr_out == "negative(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_round() -> None:
    a = col("a")
    y = a.round()
    repr_out = repr(y)
    assert repr_out == "round(col(a), lit(0))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_clip() -> None:
    a = col("a")
    b = col("b")
    c = col("c")
    y = a.clip(b, c)
    repr_out = repr(y)
    assert repr_out == "clip(col(a), col(b), col(c))"
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
    y = a.log(2)
    repr_out = repr(y)
    assert repr_out == "log(col(a), lit(2))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_ln() -> None:
    a = col("a")
    y = a.ln()
    repr_out = repr(y)
    assert repr_out == "ln(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_log1p() -> None:
    a = col("a")
    y = a.log1p()
    repr_out = repr(y)
    assert repr_out == "log1p(col(a))"
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
        "csc",
        "sec",
        "cot",
        "sinh",
        "cosh",
        "tanh",
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


def test_repr_functions_unix_date() -> None:
    a = col("a")
    y = a.dt.unix_date()
    repr_out = repr(y)
    assert repr_out == "unix_date(col(a))"
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


def test_repr_functions_quarter() -> None:
    a = col("a")
    y = a.dt.quarter()
    repr_out = repr(y)
    assert repr_out == "quarter(col(a))"
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


def test_repr_functions_day_of_month() -> None:
    a = col("a")
    y = a.dt.day_of_month()
    repr_out = repr(y)
    assert repr_out == "day_of_month(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_day_of_year() -> None:
    a = col("a")
    y = a.dt.day_of_year()
    repr_out = repr(y)
    assert repr_out == "day_of_year(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_week_of_year() -> None:
    a = col("a")
    y = a.dt.week_of_year()
    repr_out = repr(y)
    assert repr_out == "week_of_year(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_exp() -> None:
    a = col("a")
    y = a.exp()
    repr_out = repr(y)
    assert repr_out == "exp(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_expm1() -> None:
    a = col("a")
    y = a.expm1()
    repr_out = repr(y)
    assert repr_out == "expm1(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_sqrt() -> None:
    a = col("a")
    y = a.sqrt()
    repr_out = repr(y)
    assert repr_out == "sqrt(col(a))"
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_cbrt() -> None:
    a = col("a")
    y = a.cbrt()
    repr_out = repr(y)
    assert repr_out == "cbrt(col(a))"
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
    y = a.minhash(num_hashes=1, ngram_size=2)
    repr_out = repr(y)
    assert repr_out == 'minhash(col(a), lit(1), lit(2), lit(1), lit("murmurhash3"))'
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_minhash_2() -> None:
    a = col("a")
    y = a.minhash(num_hashes=1, ngram_size=2, seed=3)
    repr_out = repr(y)
    assert repr_out == 'minhash(col(a), lit(1), lit(2), lit(3), lit("murmurhash3"))'
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_tokenize_encode() -> None:
    a = col("a")
    y = a.str.tokenize_encode("cl100k_base")
    repr_out = repr(y)
    assert repr_out == 'tokenize_encode(col(a), lit("cl100k_base"))'
    copied = copy.deepcopy(y)
    assert repr_out == repr(copied)


def test_repr_functions_tokenize_decode() -> None:
    a = col("a")
    y = a.str.tokenize_decode("cl100k_base")
    repr_out = repr(y)
    assert repr_out == 'tokenize_decode(col(a), lit("cl100k_base"))'
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

    series = mp.get_column_by_name("dt")
    output = repr(series)

    # Series repr is very long, so we just check the last line which contains the timestamp
    # Also remove the │ │, which is what the [1:-1] and strip is doing
    timestamp_repr = output.split("\n")[-3][1:-1].strip()
    assert timestamp_repr == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            timedelta(days=1),
            "lit(1d)",
        ),
        (
            timedelta(days=1, hours=12, minutes=30, seconds=59),
            "lit(1d 12h 30m 59s)",
        ),
        (
            timedelta(days=1, hours=12, minutes=30, seconds=59, microseconds=123456),
            "lit(1d 12h 30m 59s 123456µs)",
        ),
        (
            timedelta(weeks=1, days=1, hours=12, minutes=30, seconds=59, microseconds=123456),
            "lit(8d 12h 30m 59s 123456µs)",
        ),
    ],
)
def test_duration_lit(input, expected) -> None:
    d = lit(input)
    output = repr(d)
    assert output == expected


def test_repr_series_lit() -> None:
    s = lit(Series.from_pylist([1, 2, 3]))
    output = repr(s)
    assert output == "lit([[1, 2, 3]])"


def test_list_value_counts():
    # Create a MicroPartition with a list column
    mp = MicroPartition.from_pydict(
        {"list_col": [["a", "b", "a", "c"], ["b", "b", "c"], ["a", "a", "a"], [], ["d", None, "d"]]}
    )

    # Apply list_value_counts operation
    result = mp.eval_expression_list([col("list_col").list.value_counts().alias("value_counts")])
    value_counts = result.to_pydict()["value_counts"]

    # Expected output
    expected = [[("a", 2), ("b", 1), ("c", 1)], [("b", 2), ("c", 1)], [("a", 3)], [], [("d", 2)]]

    # Check the result
    assert value_counts == expected

    # Test with empty input (no proper type -> should raise error)
    empty_mp = MicroPartition.from_pydict({"list_col": []})
    with pytest.raises(ValueError):
        empty_mp.eval_expression_list([col("list_col").list.value_counts().alias("value_counts")])

    # Test with empty input (no proper type -> should raise error)
    none_mp = MicroPartition.from_pydict({"list_col": [None, None, None]})
    with pytest.raises(ValueError):
        none_mp.eval_expression_list([col("list_col").list.value_counts().alias("value_counts")])


def test_list_value_counts_nested():
    # Create a MicroPartition with a nested list column
    mp = MicroPartition.from_pydict(
        {
            "nested_list_col": [
                [[1, 2], [3, 4]],
                [[1, 2], [5, 6]],
                [[3, 4], [1, 2]],
                [],
                None,
                [[1, 2], [1, 2]],
            ]
        }
    )

    # Apply list_value_counts operation and expect an exception
    with pytest.raises(daft.exceptions.DaftCoreException) as exc_info:
        mp.eval_expression_list([col("nested_list_col").list.value_counts().alias("value_counts")])

    # Check the exception message
    assert (
        'DaftError::ArrowError Invalid argument error: The data type type LargeList(Field { name: "item", data_type: Int64, is_nullable: true, metadata: {} }) has no natural order'
        in str(exc_info.value)
    )


def test_list_value_counts_fixed_size():
    # Create data with lists of fixed size
    data = {
        "fixed_list": [
            [1, 2, 3],
            [4, 3, 4],
            [4, 5, 6],
            [1, 2, 3],
            [7, 8, 9],
            None,
        ]
    }

    # Create DataFrame and cast the column to fixed size list
    df = daft.from_pydict(data).with_column(
        "fixed_list", daft.col("fixed_list").cast(DataType.fixed_size_list(DataType.int64(), 3))
    )

    df = df.with_column("fixed_list", col("fixed_list").cast(DataType.fixed_size_list(DataType.int64(), 3)))

    # Get value counts
    result = df.with_column("value_counts", col("fixed_list").list.value_counts())

    # Verify the value counts
    result_dict = result.to_pydict()
    assert result_dict["value_counts"] == [
        [(1, 1), (2, 1), (3, 1)],
        [(4, 2), (3, 1)],
        [(4, 1), (5, 1), (6, 1)],
        [(1, 1), (2, 1), (3, 1)],
        [(7, 1), (8, 1), (9, 1)],
        [],
    ]


def test_list_value_counts_degenerate():
    import pyarrow as pa

    # Create a MicroPartition with an empty list column of specified type
    empty_mp = MicroPartition.from_pydict({"empty_list_col": pa.array([], type=pa.list_(pa.string()))})

    # Apply list_value_counts operation
    result = empty_mp.eval_expression_list([col("empty_list_col").list.value_counts().alias("value_counts")])

    # Check the result
    assert result.to_pydict() == {"value_counts": []}

    # Test with null values
    null_mp = MicroPartition.from_pydict({"null_list_col": pa.array([None, None], type=pa.list_(pa.string()))})

    result_null = null_mp.eval_expression_list([col("null_list_col").list.value_counts().alias("value_counts")])

    # Check the result for null values
    assert result_null.to_pydict() == {"value_counts": [[], []]}


@pytest.mark.parametrize(
    # we don't need to test all types, just a few as a sanity check. The rest are tested in the sql tests
    "sql, actual",
    [
        ("int32", DataType.int32()),
        ("int64", DataType.int64()),
        ("float32", DataType.float32()),
        ("real", DataType.float32()),
        ("float(5)", DataType.float32()),
        ("float64", DataType.float64()),
        ("float", DataType.float64()),
        ("double", DataType.float64()),
        ("double 10", DataType.float64()),
        ("float(30)", DataType.float64()),
        ("text", DataType.string()),
        ("string", DataType.string()),
        ("varchar", DataType.string()),
    ],
)
def test_cast_sql_string(sql, actual):
    expr = col("a").cast(sql)
    actual = col("a").cast(actual)
    df = daft.from_pydict({"a": [1, 2, 3]}).select(expr)
    actual_df = daft.from_pydict({"a": [1, 2, 3]}).select(actual)
    assert df.schema() == actual_df.schema()


@pytest.mark.parametrize(
    # Some sql expressions were failing as documented previously
    "sql",
    ["char", "character", "char varying", "character varying"],
)
def test_cast_sql_string_failing(sql):
    with pytest.raises(Exception):
        expr = col("a").cast(sql)
        daft.from_pydict({"a": [1, 2, 3]}).select(expr)


def test_drop_nan():
    dd = {"n": [1.0, 2.0, None]}
    df = daft.from_pydict(dd)
    assert df.drop_nan().to_pydict() == {"n": [1.0, 2.0]}


def test_drop_nan_no_nans():
    dd = {"n": [1, 1, 2]}
    df = daft.from_pydict(dd)
    assert df.drop_nan().to_pydict() == dd
