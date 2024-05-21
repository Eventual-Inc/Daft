from __future__ import annotations

import datetime

import pyarrow as pa
import pytest

from daft.expressions import col
from daft.table import MicroPartition


def test_map_get():
    data = pa.array([[(1, 2)], [], [(2, 1)]], type=pa.map_(pa.int64(), pa.int64()))
    table = MicroPartition.from_arrow(pa.table({"map_col": data}))

    result = table.eval_expression_list([col("map_col").map.get(1)])

    assert result.to_pydict() == {"value": [2, None, None]}


def test_map_get_broadcasted():
    data = pa.array([[(1, 2)], [], [(2, 1)]], type=pa.map_(pa.int64(), pa.int64()))
    keys = pa.array([1, 3, 2], type=pa.int64())
    table = MicroPartition.from_arrow(pa.table({"map_col": data, "key": keys}))

    result = table.eval_expression_list([col("map_col").map.get(col("key"))])

    assert result.to_pydict() == {"value": [2, None, 1]}


def test_map_get_duplicate_keys():
    # Only the first value is returned
    data = pa.array([[(1, 2), (1, 3)]], type=pa.map_(pa.int64(), pa.int64()))
    table = MicroPartition.from_arrow(pa.table({"map_col": data}))

    result = table.eval_expression_list([col("map_col").map.get(1)])

    assert result.to_pydict() == {"value": [2]}


def test_map_get_logical_type():
    data = pa.array(
        [
            [("foo", datetime.date(2022, 1, 1))],
            [("foo", datetime.date(2022, 1, 2))],
            [],
        ],
        type=pa.map_(pa.string(), pa.date32()),
    )
    table = MicroPartition.from_arrow(pa.table({"map_col": data}))

    result = table.eval_expression_list([col("map_col").map.get("foo")])

    assert result.to_pydict() == {"value": [datetime.date(2022, 1, 1), datetime.date(2022, 1, 2), None]}


def test_map_get_bad_field():
    data = pa.array([[(1, 2)], [(2, 3)]], type=pa.map_(pa.int64(), pa.int64()))
    table = MicroPartition.from_arrow(pa.table({"map_col": data}))

    with pytest.raises(ValueError):
        table.eval_expression_list([col("map_col").map.get("foo")])
