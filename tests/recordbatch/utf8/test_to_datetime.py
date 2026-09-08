from __future__ import annotations

import datetime

import pyarrow as pa
import pytest

import daft
from daft import DataType
from daft.expressions import col
from daft.functions import to_datetime
from daft.recordbatch import MicroPartition
from daft.series import Series


def test_utf8_to_datetime():
    table = MicroPartition.from_pydict({"col": ["2021-01-01 00:00:00", None, "2021-01-02 00:00:00"]})
    result = table.eval_expression_list([col("col").to_datetime("%Y-%m-%d %H:%M:%S")])
    assert result.to_pydict() == {
        "col": [
            datetime.datetime(2021, 1, 1, 0, 0),
            None,
            datetime.datetime(2021, 1, 2, 0, 0),
        ]
    }


def _all_null_string_table(n: int) -> MicroPartition:
    s = Series.from_arrow(pa.array([None] * n, type=pa.string()), name="col")
    return MicroPartition.from_pydict({"col": s})


def test_utf8_to_datetime_offset_format_all_null():
    # https://github.com/Eventual-Inc/Daft/issues/7470
    # The output timezone is a function of the format, not the data, so an
    # all-null (or empty) input with an offset directive must still be UTC.
    table = _all_null_string_table(1)
    result = table.eval_expression_list([col("col").to_datetime("%Y-%m-%dT%H:%M:%S%z")])
    assert result.to_pydict() == {"col": [None]}
    assert result.schema()["col"].dtype == DataType.timestamp("us", "UTC")


def test_utf8_to_datetime_offset_format_empty():
    table = _all_null_string_table(0)
    result = table.eval_expression_list([col("col").to_datetime("%Y-%m-%dT%H:%M:%S%z")])
    assert result.to_pydict() == {"col": []}
    assert result.schema()["col"].dtype == DataType.timestamp("us", "UTC")


def test_utf8_to_datetime_offset_format_mixed():
    table = MicroPartition.from_pydict({"col": [None, "2020-01-01T01:02:03+0100"]})
    result = table.eval_expression_list([col("col").to_datetime("%Y-%m-%dT%H:%M:%S%z")])
    assert result.to_pydict() == {"col": [None, datetime.datetime(2020, 1, 1, 0, 2, 3, tzinfo=datetime.timezone.utc)]}
    assert result.schema()["col"].dtype == DataType.timestamp("us", "UTC")


def test_utf8_to_datetime_naive_format_all_null_stays_naive():
    table = _all_null_string_table(1)
    result = table.eval_expression_list([col("col").to_datetime("%Y-%m-%d %H:%M:%S")])
    assert result.to_pydict() == {"col": [None]}
    assert result.schema()["col"].dtype == DataType.timestamp("us")


def test_utf8_to_datetime_explicit_timezone_all_null():
    table = _all_null_string_table(1)
    result = table.eval_expression_list([col("col").to_datetime("%Y-%m-%d %H:%M:%S", timezone="Asia/Shanghai")])
    assert result.to_pydict() == {"col": [None]}
    assert result.schema()["col"].dtype == DataType.timestamp("us", "Asia/Shanghai")


def test_utf8_to_datetime_offset_format_explicit_null_timezone_all_null():
    # An explicit Null timezone literal must be indistinguishable from an absent one, in
    # both `get_return_field` and the kernel: the offset directive still coerces to UTC.
    # Guards against the planner and the kernel applying the rule under different conditions.
    table = _all_null_string_table(1)
    expr = to_datetime(col("col"), "%Y-%m-%dT%H:%M:%S%z", timezone=daft.lit(None))
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": [None]}
    assert result.schema()["col"].dtype == DataType.timestamp("us", "UTC")


def test_utf8_to_datetime_offset_format_explicit_null_timezone_with_values():
    table = MicroPartition.from_pydict({"col": [None, "2020-01-01T01:02:03+0100"]})
    expr = to_datetime(col("col"), "%Y-%m-%dT%H:%M:%S%z", timezone=daft.lit(None))
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": [None, datetime.datetime(2020, 1, 1, 0, 2, 3, tzinfo=datetime.timezone.utc)]}
    assert result.schema()["col"].dtype == DataType.timestamp("us", "UTC")


def test_utf8_to_datetime_offset_format_differing_offsets_after_null():
    # The parse path must not depend on row order or on which row is seen first: every row
    # normalises to UTC and the array dtype is Timestamp[us; UTC] regardless.
    table = MicroPartition.from_pydict(
        {
            "col": [
                None,
                "2021-01-01T00:00:00+0000",
                "2021-01-02T01:07:35+0100",
                "2021-01-03T12:30:00+0200",
            ]
        }
    )
    result = table.eval_expression_list([col("col").to_datetime("%Y-%m-%dT%H:%M:%S%z")])
    utc = datetime.timezone.utc
    assert result.to_pydict() == {
        "col": [
            None,
            datetime.datetime(2021, 1, 1, 0, 0, tzinfo=utc),
            datetime.datetime(2021, 1, 2, 0, 7, 35, tzinfo=utc),
            datetime.datetime(2021, 1, 3, 10, 30, tzinfo=utc),
        ]
    }
    assert result.schema()["col"].dtype == DataType.timestamp("us", "UTC")


def test_utf8_to_datetime_invalid_timezone_all_null_still_errors():
    # The timezone is parsed once up front, so an unparseable one fails the same way whether
    # or not the partition happens to contain a non-null value.
    table = _all_null_string_table(1)
    with pytest.raises(ValueError, match="failed to parse timezone"):
        table.eval_expression_list([col("col").to_datetime("%Y-%m-%d %H:%M:%S", timezone="Not/AZone")])


def test_utf8_to_datetime_null_dtype_input_rejected():
    # `to_datetime` used to have no input-dtype check (unlike `to_date`), so a Null-typed
    # column planned as Timestamp while `with_utf8_array` returned the Null series unchanged,
    # tripping the data type mismatch assert. It must be rejected at schema-resolution time.
    s = Series.from_arrow(pa.array([None, None], type=pa.null()), name="col")
    table = MicroPartition.from_pydict({"col": s})
    with pytest.raises(ValueError, match="Utf8"):
        table.eval_expression_list([col("col").to_datetime("%Y-%m-%d %H:%M:%S")])
