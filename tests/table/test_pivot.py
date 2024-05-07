from __future__ import annotations

import pytest

from daft.expressions.expressions import col
from daft.table.micropartition import MicroPartition


def test_pivot_empty_table() -> None:
    daft_table = MicroPartition.from_pydict(
        {
            "group": [],
            "pivot": [],
            "value": [],
        }
    )
    daft_table = daft_table.pivot([col("group")], col("pivot"), col("value"), [])

    expected = {
        "group": [],
    }
    assert daft_table.to_pydict() == expected


@pytest.mark.parametrize(
    "input",
    [
        MicroPartition.from_pydict(
            {
                "group": ["B", "A", "A", "B"],
                "pivot": [1, 2, 1, 2],
                "value": [1, 2, 3, 4],
            }
        ),
        MicroPartition.concat(
            [
                MicroPartition.from_pydict({"group": ["B"], "pivot": [1], "value": [1]}),
                MicroPartition.from_pydict({"group": ["A"], "pivot": [2], "value": [2]}),
                MicroPartition.from_pydict({"group": ["A"], "pivot": [1], "value": [3]}),
                MicroPartition.from_pydict({"group": ["B"], "pivot": [2], "value": [4]}),
            ]
        ),
    ],
)
def test_pivot_multipartition(input: MicroPartition) -> None:
    daft_table = input.pivot([col("group")], col("pivot"), col("value"), ["1", "2"])

    expected = {
        "group": ["A", "B"],
        "1": [3, 1],
        "2": [2, 4],
    }
    assert daft_table.to_pydict() == expected


def test_pivot_column_names_subset() -> None:
    daft_table = MicroPartition.from_pydict(
        {
            "group": ["A", "A", "B"],
            "pivot": [1, 2, 1],
            "value": [1, 2, 3],
        }
    )
    daft_table = daft_table.pivot([col("group")], col("pivot"), col("value"), ["1"])

    expected = {
        "group": ["A", "B"],
        "1": [1, 3],
    }
    assert daft_table.to_pydict() == expected


def test_pivot_column_names_superset() -> None:
    daft_table = MicroPartition.from_pydict(
        {
            "group": ["A", "A", "B"],
            "pivot": [1, 2, 1],
            "value": [1, 2, 3],
        }
    )
    daft_table = daft_table.pivot([col("group")], col("pivot"), col("value"), ["1", "2", "3"])

    expected = {
        "group": ["A", "B"],
        "1": [1, 3],
        "2": [2, None],
        "3": [None, None],
    }
    assert daft_table.to_pydict() == expected


def test_pivot_nulls_in_group() -> None:
    daft_table = MicroPartition.from_pydict(
        {
            "group": ["A", None, "B", None],
            "pivot": [1, 1, 2, 2],
            "value": [1, 2, 3, 4],
        }
    )
    daft_table = daft_table.pivot([col("group")], col("pivot"), col("value"), ["1", "2"])

    expected = {
        "group": ["A", "B", None],
        "1": [1, None, 2],
        "2": [None, 3, 4],
    }
    assert daft_table.to_pydict() == expected


def test_pivot_nulls_in_pivot() -> None:
    daft_table = MicroPartition.from_pydict(
        {
            "group": ["A", "A", "B", "B"],
            "pivot": [1, None, 1, None],
            "value": [1, 2, 3, 4],
        }
    )
    daft_table = daft_table.pivot([col("group")], col("pivot"), col("value"), ["1", "None"])

    expected = {
        "group": ["A", "B"],
        "1": [1, 3],
        "None": [2, 4],
    }
    assert daft_table.to_pydict() == expected
