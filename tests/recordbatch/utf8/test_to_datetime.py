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
    # The planner treats an explicit `Null` timezone literal as an absent one.
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
    # The timezone is parsed up front, so this fails with or without a non-null value.
    table = _all_null_string_table(1)
    with pytest.raises(ValueError, match="failed to parse timezone"):
        table.eval_expression_list([col("col").to_datetime("%Y-%m-%d %H:%M:%S", timezone="Not/AZone")])


def _null_dtype_table(n: int) -> MicroPartition:
    s = Series.from_arrow(pa.array([None] * n, type=pa.null()), name="col")
    return MicroPartition.from_pydict({"col": s})


def test_utf8_to_datetime_null_dtype_input_offset_format():
    # A Null-dtype column (e.g. uncast `[None]`) yields an all-null Timestamp; the
    # kernel builds it directly since `with_utf8_array` would pass Null through as Null.
    table = _null_dtype_table(2)
    result = table.eval_expression_list([col("col").to_datetime("%Y-%m-%dT%H:%M:%S%z")])
    assert result.to_pydict() == {"col": [None, None]}
    assert result.schema()["col"].dtype == DataType.timestamp("us", "UTC")


def test_utf8_to_datetime_null_dtype_input_naive_format_stays_naive():
    table = _null_dtype_table(1)
    result = table.eval_expression_list([col("col").to_datetime("%Y-%m-%d %H:%M:%S")])
    assert result.to_pydict() == {"col": [None]}
    assert result.schema()["col"].dtype == DataType.timestamp("us")


def test_utf8_to_datetime_null_dtype_input_explicit_timezone():
    table = _null_dtype_table(1)
    result = table.eval_expression_list([col("col").to_datetime("%Y-%m-%d %H:%M:%S", timezone="Asia/Shanghai")])
    assert result.to_pydict() == {"col": [None]}
    assert result.schema()["col"].dtype == DataType.timestamp("us", "Asia/Shanghai")


def test_utf8_to_datetime_null_dtype_input_invalid_timezone_still_errors():
    table = _null_dtype_table(1)
    with pytest.raises(ValueError, match="failed to parse timezone"):
        table.eval_expression_list([col("col").to_datetime("%Y-%m-%d %H:%M:%S", timezone="Not/AZone")])


def test_utf8_to_datetime_null_dtype_input_non_string_still_rejected():
    s = Series.from_arrow(pa.array([1, 2], type=pa.int64()), name="col")
    table = MicroPartition.from_pydict({"col": s})
    with pytest.raises(ValueError, match="Utf8"):
        table.eval_expression_list([col("col").to_datetime("%Y-%m-%d %H:%M:%S")])
