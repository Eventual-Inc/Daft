from __future__ import annotations

import pytest

from daft import col
from daft.datatype import DataType
from daft.recordbatch import MicroPartition


def test_list_contains_scalar_and_broadcast():
    table = MicroPartition.from_pydict({"a": [[1, 2, 3], [4, 5, 6], [], [1]]})
    result = table.eval_expression_list([col("a").list_contains(1)])
    assert result.to_pydict()["a"] == [True, False, False, True]


def test_list_contains_column_items():
    table = MicroPartition.from_pydict({"lists": [[1, 2], [3, 4], [5, 6]], "items": [1, 4, 7]})
    result = table.eval_expression_list([col("lists").list_contains(col("items"))])
    assert result.to_pydict()["lists"] == [True, True, False]


def test_list_contains_null_list_and_item():
    table = MicroPartition.from_pydict({"lists": [[1, 2], None, [3, 4]], "items": [2, 2, None]})
    result = table.eval_expression_list([col("lists").list_contains(col("items"))])
    assert result.to_pydict()["lists"] == [True, None, None]


@pytest.mark.parametrize(
    "data,search_value,expected",
    [
        pytest.param([[1, 2, 3], [4, 5], [1]], 1, [True, False, True], id="int"),
        pytest.param([[1.0, 2.0], [3.0, 4.0]], 2.0, [True, False], id="float"),
        pytest.param([["a", "b"], ["c", "d"]], "a", [True, False], id="string"),
        pytest.param([[True, False], [False, False]], True, [True, False], id="bool"),
    ],
)
def test_list_contains_types(data, search_value, expected):
    table = MicroPartition.from_pydict({"a": data})
    result = table.eval_expression_list([col("a").list_contains(search_value)])
    assert result.to_pydict()["a"] == expected


def test_list_contains_nulls_in_list():
    table = MicroPartition.from_pydict({"a": [[1, None, 3], [None, None], [None, 5]]})
    result = table.eval_expression_list([col("a").list_contains(3)])
    assert result.to_pydict()["a"] == [True, False, False]


def test_list_contains_list_null_dtype():
    table = MicroPartition.from_pydict({"a": [[None, None], [None], []], "items": [1, None, 1]})
    result = table.eval_expression_list([col("a").list_contains(col("items"))])
    assert result.to_pydict()["a"] == [False, None, False]


def test_fixed_size_list_contains():
    table = MicroPartition.from_pydict({"col": [["a", "b"], ["c", "d"], ["a", "c"]]})
    fixed_dtype = DataType.fixed_size_list(DataType.string(), 2)
    table = table.eval_expression_list([col("col").cast(fixed_dtype)])

    result = table.eval_expression_list([col("col").list_contains("a")])
    assert result.to_pydict()["col"] == [True, False, True]


def test_list_contains_varying_lengths():
    table = MicroPartition.from_pydict({"a": [[1], [1, 2], [1, 2, 3], [1, 2, 3, 4]]})
    result = table.eval_expression_list([col("a").list_contains(3)])
    assert result.to_pydict()["a"] == [False, False, True, True]
