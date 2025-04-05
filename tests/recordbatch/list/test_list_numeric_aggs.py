from __future__ import annotations

import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.expressions import col
from daft.recordbatch import MicroPartition

table = MicroPartition.from_pydict({"a": [[1, 2], [3, 4], [5, None], [None, None], None]})
fixed_dtype = DataType.fixed_size_list(DataType.int64(), 2)
fixed_table = table.eval_expression_list([col("a").cast(fixed_dtype)])


@pytest.mark.parametrize("table", [table, fixed_table])
def test_list_sum(table):
    result = table.eval_expression_list([col("a").list.sum()])
    assert result.to_pydict() == {"a": [3, 7, 5, None, None]}


@pytest.mark.parametrize("table", [table, fixed_table])
def test_list_mean(table):
    result = table.eval_expression_list([col("a").list.mean()])
    assert result.to_pydict() == {"a": [1.5, 3.5, 5, None, None]}


@pytest.mark.parametrize("table", [table, fixed_table])
def test_list_min(table):
    result = table.eval_expression_list([col("a").list.min()])
    assert result.to_pydict() == {"a": [1, 3, 5, None, None]}


@pytest.mark.parametrize("table", [table, fixed_table])
def test_list_max(table):
    result = table.eval_expression_list([col("a").list.max()])
    assert result.to_pydict() == {"a": [2, 4, 5, None, None]}


def test_list_numeric_aggs_empty_table():
    empty_table = MicroPartition.from_pydict(
        {
            "col": pa.array([], type=pa.list_(pa.int64())),
            "fixed_col": pa.array([], type=pa.list_(pa.int64(), 2)),
        }
    )

    result = empty_table.eval_expression_list(
        [
            col("col").cast(DataType.list(DataType.int64())).list.sum().alias("col_sum"),
            col("col").list.mean().alias("col_mean"),
            col("col").list.min().alias("col_min"),
            col("col").list.max().alias("col_max"),
            col("fixed_col").list.sum().alias("fixed_col_sum"),
            col("fixed_col").list.mean().alias("fixed_col_mean"),
            col("fixed_col").list.min().alias("fixed_col_min"),
            col("fixed_col").list.max().alias("fixed_col_max"),
        ]
    )
    assert result.to_pydict() == {
        "col_sum": [[]],
        "col_mean": [[]],
        "col_min": [[]],
        "col_max": [[]],
        "fixed_col_sum": [[]],
        "fixed_col_mean": [[]],
        "fixed_col_min": [[]],
        "fixed_col_max": [[]],
    }
