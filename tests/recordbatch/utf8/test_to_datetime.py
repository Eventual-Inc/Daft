import datetime

from daft.expressions import col
from daft.recordbatch import MicroPartition


def test_utf8_to_datetime():
    table = MicroPartition.from_pydict({"col": ["2021-01-01 00:00:00", None, "2021-01-02 00:00:00"]})
    result = table.eval_expression_list([col("col").str.to_datetime("%Y-%m-%d %H:%M:%S")])
    assert result.to_pydict() == {
        "col": [
            datetime.datetime(2021, 1, 1, 0, 0),
            None,
            datetime.datetime(2021, 1, 2, 0, 0),
        ]
    }


def test_to_datetime_with_tz_offset():
    table = MicroPartition.from_pydict({"a": ["2020-01-01T00:00:00+01:00"]})
    result = table.eval_expression_list([col("a").str.to_datetime("%Y-%m-%dT%H:%M:%S%z").dt.day_of_year()])
    expected = {{"a": [365]}}

    assert result.to_pydict() == expected
