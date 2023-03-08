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

    result_pairs = list(zip(result_table.get_column("x_ind").to_pylist(), result_table.get_column("y_ind").to_pylist()))
    assert sorted(expected_pairs) == sorted(result_pairs)
    casted_l = left_table.get_column("x").to_pylist()
    result_l = [casted_l[idx] for idx, _ in result_pairs]
    assert result_table.get_column("x").to_pylist() == result_l

    casted_r = right_table.get_column("y").to_pylist()
    result_r = [casted_r[idx] for _, idx in result_pairs]
    assert result_table.get_column("y").to_pylist() == result_r
