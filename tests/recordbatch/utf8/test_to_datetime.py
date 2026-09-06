from __future__ import annotations

import datetime

import pyarrow as pa

from daft import DataType
from daft.expressions import col
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
