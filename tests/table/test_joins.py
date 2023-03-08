from __future__ import annotations

import itertools

import pytest

from daft.datatype import DataType
from daft.expressions2 import col
from daft.table import Table

daft_int_types = [
    DataType.int8(),
    DataType.int16(),
    DataType.int32(),
    DataType.int64(),
    DataType.uint8(),
    DataType.uint16(),
    DataType.uint32(),
    DataType.uint64(),
]

daft_numeric_types = daft_int_types + [DataType.float32(), DataType.float64()]
daft_string_types = [DataType.string()]


@pytest.mark.parametrize(
    "dtype, data",
    itertools.product(
        daft_numeric_types + daft_string_types,
        [
            ([0, 1, 2, 3, None], [0, 1, 2, 3, None], [(0, 0), (1, 1), (2, 2), (3, 3)]),
            ([None, None, 3, 1, 2, 0], [0, 1, 2, 3, None], [(5, 0), (3, 1), (4, 2), (2, 3)]),
            ([None, 4, 5, 6, 7], [0, 1, 2, 3, None], []),
            ([None, 0, 0, 0, 1, None], [0, 1, 2, 3, None], [(1, 0), (2, 0), (3, 0), (4, 1)]),
            ([None, 0, 0, 1, 1, None], [0, 1, 2, 3, None], [(1, 0), (2, 0), (3, 1), (4, 1)]),
            ([None, 0, 0, 1, 1, None], [3, 1, 0, 2, None], [(1, 2), (2, 2), (3, 1), (4, 1)]),
        ],
    ),
)
def test_table_join_single_column(dtype, data) -> None:
    l, r, expected_pairs = data
    left_table = Table.from_pydict({"x": l, "x_ind": list(range(len(l)))}).eval_expression_list(
        [col("x").cast(dtype), col("x_ind")]
    )
    right_table = Table.from_pydict({"y": r, "y_ind": list(range(len(r)))})
    result_table = left_table.join(right_table, left_on=[col("x")], right_on=[col("y")], how="inner")

    assert result_table.column_names() == ["x", "x_ind", "y", "y_ind"]

    result_pairs = list(zip(result_table.get_column("x_ind").to_pylist(), result_table.get_column("y_ind").to_pylist()))
    assert sorted(expected_pairs) == sorted(result_pairs)
    casted_l = left_table.get_column("x").to_pylist()
    result_l = [casted_l[idx] for idx, _ in result_pairs]
    assert result_table.get_column("x").to_pylist() == result_l

    casted_r = right_table.get_column("y").to_pylist()
    result_r = [casted_r[idx] for _, idx in result_pairs]
    assert result_table.get_column("y").to_pylist() == result_r

    # make sure the result is the same with right table on left
    result_table = right_table.join(left_table, right_on=[col("x")], left_on=[col("y")], how="inner")

    assert result_table.column_names() == ["y", "y_ind", "x", "x_ind"]

    result_pairs = list(zip(result_table.get_column("x_ind").to_pylist(), result_table.get_column("y_ind").to_pylist()))
    assert sorted(expected_pairs) == sorted(result_pairs)
    casted_l = left_table.get_column("x").to_pylist()
    result_l = [casted_l[idx] for idx, _ in result_pairs]
    assert result_table.get_column("x").to_pylist() == result_l

    casted_r = right_table.get_column("y").to_pylist()
    result_r = [casted_r[idx] for _, idx in result_pairs]
    assert result_table.get_column("y").to_pylist() == result_r


def test_table_join_mismatch_column() -> None:
    left_table = Table.from_pydict({"x": [1, 2, 3, 4], "y": [2, 3, 4, 5]})
    right_table = Table.from_pydict({"a": [1, 2, 3, 4], "b": [2, 3, 4, 5]})

    with pytest.raises(ValueError, match="Mismatch of number of join keys"):
        left_table.join(right_table, left_on=[col("x"), col("y")], right_on=[col("a")])


def test_table_join_multicolumn() -> None:
    left_table = Table.from_pydict({"x": [1, 2, 3, 4], "y": [2, 3, 4, 5]})
    right_table = Table.from_pydict({"a": [1, 2, 3, 4], "b": [2, 3, 4, 5]})

    with pytest.raises(NotImplementedError, match="Multicolumn joins not implemented"):
        left_table.join(right_table, left_on=[col("x"), col("y")], right_on=[col("a"), col("b")])


def test_table_join_no_columns() -> None:
    left_table = Table.from_pydict({"x": [1, 2, 3, 4], "y": [2, 3, 4, 5]})
    right_table = Table.from_pydict({"a": [1, 2, 3, 4], "b": [2, 3, 4, 5]})

    with pytest.raises(ValueError, match="No columns were passed in to join on"):
        left_table.join(right_table, left_on=[], right_on=[])


def test_table_join_single_column_name_conflicts() -> None:
    left_table = Table.from_pydict({"x": [0, 1, 2, 3], "y": [2, 3, 4, 5]})
    right_table = Table.from_pydict({"x": [3, 2, 1, 0], "y": [6, 7, 8, 9]})

    result_table = left_table.join(right_table, left_on=[col("x")], right_on=[col("x")])
    assert result_table.column_names() == ["x", "y", "right.y"]
    result_sorted = result_table.sort([col("x")])
    assert result_sorted.get_column("y").to_pylist() == [2, 3, 4, 5]

    assert result_sorted.get_column("right.y").to_pylist() == [9, 8, 7, 6]


def test_table_join_single_column_name_multiple_conflicts() -> None:

    left_table = Table.from_pydict({"x": [0, 1, 2, 3], "y": [2, 3, 4, 5], "right.y": [6, 7, 8, 9]})
    right_table = Table.from_pydict({"x": [3, 2, 1, 0], "y": [10, 11, 12, 13]})

    result_table = left_table.join(right_table, left_on=[col("x")], right_on=[col("x")])
    assert result_table.column_names() == ["x", "y", "right.y", "right.right.y"]
    result_sorted = result_table.sort([col("x")])
    assert result_sorted.get_column("y").to_pylist() == [2, 3, 4, 5]

    assert result_sorted.get_column("right.y").to_pylist() == [6, 7, 8, 9]
    assert result_sorted.get_column("right.right.y").to_pylist() == [13, 12, 11, 10]


def test_table_join_single_column_name_boolean() -> None:
    left_table = Table.from_pydict({"x": [False, True, None], "y": [0, 1, 2]})
    right_table = Table.from_pydict({"x": [None, True, False, None], "y": [0, 1, 2, 3]})

    result_table = left_table.join(right_table, left_on=[col("x")], right_on=[col("x")])
    assert result_table.column_names() == ["x", "y", "right.y"]
    result_sorted = result_table.sort([col("x")])
    assert result_sorted.get_column("y").to_pylist() == [0, 1]
    assert result_sorted.get_column("right.y").to_pylist() == [2, 1]


def test_table_join_single_column_name_null() -> None:
    left_table = Table.from_pydict({"x": [None, None, None], "y": [0, 1, 2]})
    right_table = Table.from_pydict({"x": [None, None, None, None], "y": [0, 1, 2, 3]})

    result_table = left_table.join(right_table, left_on=[col("x")], right_on=[col("x")])
    assert result_table.column_names() == ["x", "y", "right.y"]
    result_sorted = result_table.sort([col("x")])
    assert result_sorted.get_column("y").to_pylist() == []
    assert result_sorted.get_column("right.y").to_pylist() == []
