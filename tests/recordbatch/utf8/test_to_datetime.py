from __future__ import annotations

import datetime

import pyarrow as pa
import pytest

from daft import DataType
from daft.expressions import col
from daft.recordbatch import MicroPartition


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


@pytest.mark.parametrize(
    "format",
    [
        pytest.param("%Y-%m-%d %H:%M:%S %z", id="datetime with offset"),
        pytest.param("%Y-%m-%d %z", id="date-only with offset"),
    ],
)
def test_utf8_to_datetime_all_null_with_offset(format):
    # An all-null column with an offset directive must still resolve to Timestamp[us; UTC], the
    # type get_return_field planned. Deciding it from the data instead would build a naive array
    # here and fail eval_expression with a data type mismatch.
    table = MicroPartition.from_arrow(pa.table({"col": pa.array([None, None], type=pa.string())}))
    result = table.eval_expression_list([col("col").to_datetime(format)])
    assert result.to_pydict() == {"col": [None, None]}
    assert result.schema()["col"].dtype == DataType.timestamp("us", "UTC")


def test_utf8_to_datetime_agrees_with_to_date():
    # to_date has always accepted date-only formats strictly; to_datetime must accept the same
    # inputs and land on midnight of the same day.
    table = MicroPartition.from_pydict({"col": ["2021-01-01", None, "2021-01-02"]})
    result = table.eval_expression_list(
        [col("col").to_date("%Y-%m-%d").alias("d"), col("col").to_datetime("%Y-%m-%d").alias("ts")]
    ).to_pydict()
    assert [None if ts is None else ts.date() for ts in result["ts"]] == result["d"]
    assert all(ts is None or ts.time() == datetime.time(0, 0) for ts in result["ts"])
