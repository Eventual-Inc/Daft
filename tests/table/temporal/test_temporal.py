import datetime

import pytest

from daft import col
from daft.table.micropartition import MicroPartition

DATETIMES_WITHOUT_NULL = [
    datetime.datetime(2021, 1, 1, 23, 59, 58),
    datetime.datetime(2021, 1, 2, 0, 0, 0),
]

DATETIMES_WITH_NULL = [None, datetime.datetime(2021, 1, 2, 1, 2, 3)]


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            DATETIMES_WITHOUT_NULL,
            [datetime.date(2021, 1, 1), datetime.date(2021, 1, 2)],
        ),
        (
            DATETIMES_WITH_NULL,
            [None, datetime.date(2021, 1, 2)],
        ),
    ],
)
def test_table_date(input, expected):
    table = MicroPartition.from_pydict({"datetime": input})
    dates = table.eval_expression_list([col("datetime").dt.date()])
    assert dates.get_column("datetime").to_pylist() == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            DATETIMES_WITHOUT_NULL,
            [1, 2],
        ),
        (
            DATETIMES_WITH_NULL,
            [None, 2],
        ),
    ],
)
def test_table_day(input, expected):
    table = MicroPartition.from_pydict({"datetime": input})
    dates = table.eval_expression_list([col("datetime").dt.day()])
    assert dates.get_column("datetime").to_pylist() == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            DATETIMES_WITHOUT_NULL,
            [23, 0],
        ),
        (
            DATETIMES_WITH_NULL,
            [None, 1],
        ),
    ],
)
def test_table_hour(input, expected):
    table = MicroPartition.from_pydict({"datetime": input})
    dates = table.eval_expression_list([col("datetime").dt.hour()])
    assert dates.get_column("datetime").to_pylist() == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            DATETIMES_WITHOUT_NULL,
            [59, 0],
        ),
        (
            DATETIMES_WITH_NULL,
            [None, 2],
        ),
    ],
)
def test_table_minute(input, expected):
    table = MicroPartition.from_pydict({"datetime": input})
    dates = table.eval_expression_list([col("datetime").dt.minute()])
    assert dates.get_column("datetime").to_pylist() == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            DATETIMES_WITHOUT_NULL,
            [58, 0],
        ),
        (
            DATETIMES_WITH_NULL,
            [None, 3],
        ),
    ],
)
def test_table_second(input, expected):
    table = MicroPartition.from_pydict({"datetime": input})
    dates = table.eval_expression_list([col("datetime").dt.second()])
    assert dates.get_column("datetime").to_pylist() == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            DATETIMES_WITHOUT_NULL,
            [datetime.time(23, 59, 58), datetime.time(0, 0, 0)],
        ),
        (
            DATETIMES_WITH_NULL,
            [None, datetime.time(1, 2, 3)],
        ),
    ],
)
def test_table_time(input, expected):
    table = MicroPartition.from_pydict({"datetime": input})
    dates = table.eval_expression_list([col("datetime").dt.time()])
    assert dates.get_column("datetime").to_pylist() == expected
