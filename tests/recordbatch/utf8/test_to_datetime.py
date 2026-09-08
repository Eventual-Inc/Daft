from __future__ import annotations

import datetime

import pyarrow as pa

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


def test_utf8_to_datetime_all_null_with_offset():
    # An all-null column with an offset directive must still resolve to Timestamp[us; UTC],
    # matching get_return_field. Otherwise eval_expression panics with a data type mismatch.
    table = MicroPartition.from_arrow(pa.table({"col": pa.array([None, None], type=pa.string())}))
    result = table.eval_expression_list([col("col").to_datetime("%Y-%m-%d %H:%M:%S %z")])
    assert result.to_pydict() == {"col": [None, None]}
    assert result.schema()["col"].dtype == DataType.timestamp("us", "UTC")
