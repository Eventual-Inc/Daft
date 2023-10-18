from __future__ import annotations

import itertools

import pytest

from daft import utils
from daft.daft import JoinType
from daft.datatype import DataType
from daft.expressions import col
from daft.series import Series
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
    result_table = left_table.join(right_table, left_on=[col("x")], right_on=[col("y")], how=JoinType.Inner)

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
    result_table = right_table.join(left_table, right_on=[col("x")], left_on=[col("y")], how=JoinType.Inner)

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


@pytest.mark.parametrize(
    "left",
    [
        {"a": [], "b": []},
        {"a": ["apple", "banana"], "b": [3, 4]},
    ],
)
@pytest.mark.parametrize(
    "right",
    [
        {"x": [], "y": []},
        {"x": ["banana", "apple"], "y": [3, 4]},
    ],
)
def test_table_join_multicolumn_empty_result(left, right) -> None:
    """Various multicol joins that should all produce an empty result."""
    left_table = Table.from_pydict(left).eval_expression_list(
        [col("a").cast(DataType.string()), col("b").cast(DataType.int32())]
    )
    right_table = Table.from_pydict(right).eval_expression_list(
        [col("x").cast(DataType.string()), col("y").cast(DataType.int32())]
    )

    result = left_table.join(right_table, left_on=[col("a"), col("b")], right_on=[col("x"), col("y")])
    assert result.to_pydict() == {"a": [], "b": [], "x": [], "y": []}


def test_table_join_multicolumn_nocross() -> None:
    """A multicol join that should produce two rows and no cross product results.

    Input has duplicate join values and overlapping single-column values,
    but there should only be two correct matches, both not cross.
    """
    left_table = Table.from_pydict(
        {
            "a": ["apple", "apple", "banana", "banana", "carrot"],
            "b": [1, 2, 2, 2, 3],
            "c": [1, 2, 3, 4, 5],
        }
    )
    right_table = Table.from_pydict(
        {
            "x": ["banana", "carrot", "apple", "banana", "apple", "durian"],
            "y": [1, 3, 2, 1, 3, 6],
            "z": [1, 2, 3, 4, 5, 6],
        }
    )

    result = left_table.join(right_table, left_on=[col("a"), col("b")], right_on=[col("x"), col("y")])
    assert set(utils.freeze(utils.pydict_to_rows(result.to_pydict()))) == set(
        utils.freeze(
            [
                {"a": "apple", "b": 2, "c": 2, "x": "apple", "y": 2, "z": 3},
                {"a": "carrot", "b": 3, "c": 5, "x": "carrot", "y": 3, "z": 2},
            ]
        )
    )


def test_table_join_multicolumn_cross() -> None:
    """A multicol join that should produce a cross product and a non-cross product."""

    left_table = Table.from_pydict(
        {
            "a": ["apple", "apple", "banana", "banana", "banana"],
            "b": [1, 0, 1, 1, 1],
            "c": [1, 2, 3, 4, 5],
        }
    )
    right_table = Table.from_pydict(
        {
            "x": ["apple", "apple", "banana", "banana", "banana"],
            "y": [1, 0, 1, 1, 0],
            "z": [1, 2, 3, 4, 5],
        }
    )

    result = left_table.join(right_table, left_on=[col("a"), col("b")], right_on=[col("x"), col("y")])
    assert set(utils.freeze(utils.pydict_to_rows(result.to_pydict()))) == set(
        utils.freeze(
            [
                {"a": "apple", "b": 1, "c": 1, "x": "apple", "y": 1, "z": 1},
                {"a": "apple", "b": 0, "c": 2, "x": "apple", "y": 0, "z": 2},
                {"a": "banana", "b": 1, "c": 3, "x": "banana", "y": 1, "z": 3},
                {"a": "banana", "b": 1, "c": 3, "x": "banana", "y": 1, "z": 4},
                {"a": "banana", "b": 1, "c": 4, "x": "banana", "y": 1, "z": 3},
                {"a": "banana", "b": 1, "c": 4, "x": "banana", "y": 1, "z": 4},
                {"a": "banana", "b": 1, "c": 5, "x": "banana", "y": 1, "z": 3},
                {"a": "banana", "b": 1, "c": 5, "x": "banana", "y": 1, "z": 4},
            ]
        )
    )


def test_table_join_multicolumn_all_nulls() -> None:
    left_table = Table.from_pydict(
        {
            "a": Series.from_pylist([None, None, None]).cast(DataType.int64()),
            "b": Series.from_pylist([None, None, None]).cast(DataType.string()),
            "c": [1, 2, 3],
        }
    )
    right_table = Table.from_pydict(
        {
            "x": Series.from_pylist([None, None, None]).cast(DataType.int64()),
            "y": Series.from_pylist([None, None, None]).cast(DataType.string()),
            "z": [1, 2, 3],
        }
    )

    result = left_table.join(right_table, left_on=[col("a"), col("b")], right_on=[col("x"), col("y")])
    assert set(utils.freeze(utils.pydict_to_rows(result.to_pydict()))) == set(utils.freeze([]))


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


def test_table_join_single_column_name_conflicts_different_named_join() -> None:
    left_table = Table.from_pydict({"x": [0, 1, 2, 3], "y": [2, 3, 4, 5]})
    right_table = Table.from_pydict({"y": [3, 2, 1, 0], "x": [6, 7, 8, 9]})

    result_table = left_table.join(right_table, left_on=[col("x")], right_on=[col("y")])

    # NOTE: right.y is not dropped because it has a different name from the corresponding left
    # column it is joined on, left_table["x"]
    assert result_table.column_names() == ["x", "y", "right.y", "right.x"]

    result_sorted = result_table.sort([col("x")])
    assert result_sorted.get_column("y").to_pylist() == [2, 3, 4, 5]
    assert result_sorted.get_column("right.x").to_pylist() == [9, 8, 7, 6]


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
