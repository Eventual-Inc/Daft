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


OFFSET_FMT = "%Y-%m-%dT%H:%M:%S%z"


@pytest.mark.parametrize("timezone", [None, daft.lit(None)], ids=["no_timezone", "null_timezone"])
def test_utf8_to_datetime_offset_format_leading_null(timezone):
    # https://github.com/Eventual-Inc/Daft/issues/7470
    # An explicit `Null` timezone literal behaves like an absent one: offsets coerce to UTC.
    table = MicroPartition.from_pydict({"col": [None, "2020-01-01T01:02:03+0100"]})
    result = table.eval_expression_list([to_datetime(col("col"), OFFSET_FMT, timezone=timezone)])
    assert result.to_pydict() == {"col": [None, datetime.datetime(2020, 1, 1, 0, 2, 3, tzinfo=datetime.timezone.utc)]}
    assert result.schema()["col"].dtype == DataType.timestamp("us", "UTC")


@pytest.mark.parametrize("input_type", [pa.string(), pa.null()], ids=["utf8", "null"])
def test_utf8_to_datetime_invalid_timezone_errors_without_data(input_type):
    s = Series.from_arrow(pa.array([None], type=input_type), name="col")
    table = MicroPartition.from_pydict({"col": s})
    with pytest.raises(ValueError, match="failed to parse timezone"):
        table.eval_expression_list([col("col").to_datetime("%Y-%m-%d %H:%M:%S", timezone="Not/AZone")])


def test_utf8_to_datetime_rejects_non_string_input():
    table = MicroPartition.from_pydict({"col": [1, 2]})
    with pytest.raises(ValueError, match="Utf8"):
        table.eval_expression_list([col("col").to_datetime("%Y-%m-%d %H:%M:%S")])
