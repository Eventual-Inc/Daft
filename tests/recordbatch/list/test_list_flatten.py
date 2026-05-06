from __future__ import annotations

import pytest

from daft import col
from daft.datatype import DataType
from daft.recordbatch import MicroPartition


def test_list_flatten():
    table = MicroPartition.from_pydict(
        {
            "col": [
                [[1, 2], [3]],
                [[None], [4, None]],
                [[], [5], None],
                None,
                [],
            ]
        }
    )

    result = table.eval_expression_list([col("col").list_flatten()])

    assert result.to_pydict()["col"] == [
        [1, 2, 3],
        [None, 4, None],
        [5],
        None,
        [],
    ]


def test_list_flatten_fixed_size_outer():
    table = MicroPartition.from_pydict(
        {
            "col": [
                [[1], [2], None],
                [None, None, None],
                [[3, 4], [], [5]],
                None,
            ]
        }
    ).eval_expression_list([col("col").cast(DataType.fixed_size_list(DataType.list(DataType.int64()), 3))])

    result = table.eval_expression_list([col("col").list_flatten()])

    assert result.to_pydict()["col"] == [
        [1, 2],
        [],
        [3, 4, 5],
        None,
    ]


def test_list_flatten_one_level_only():
    table = MicroPartition.from_pydict(
        {
            "col": [
                [[[1], [2, 3]], [[4]]],
                [None, [[5]]],
                None,
            ]
        }
    )

    result = table.eval_expression_list([col("col").list_flatten()])

    assert result.to_pydict()["col"] == [
        [[1], [2, 3], [4]],
        [[5]],
        None,
    ]


def test_list_flatten_requires_nested_lists():
    table = MicroPartition.from_pydict({"col": [[1, 2], [3, None], None]})

    with pytest.raises(ValueError, match="list of lists"):
        table.eval_expression_list([col("col").list_flatten()])


def test_list_flatten_nested_fixed_size_lists():
    table = MicroPartition.from_pydict(
        {
            "col": [
                [[1, 2], [3, 4]],
                [[5, None], [7, 8]],
                None,
            ]
        }
    ).eval_expression_list(
        [
            col("col").cast(
                DataType.fixed_size_list(
                    DataType.fixed_size_list(DataType.int64(), 2),
                    2,
                )
            )
        ]
    )

    result = table.eval_expression_list([col("col").list_flatten()])

    assert result.to_pydict()["col"] == [
        [1, 2, 3, 4],
        [5, None, 7, 8],
        None,
    ]
