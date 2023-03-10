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
    "size, k, dtype", itertools.product([0, 1, 10, 33, 100], [1, 2, 3, 10, 40], daft_numeric_types + daft_string_types)
)
def test_table_partition_by_hash_single_col(size, k, dtype) -> None:
    table = Table.from_pydict(
        {"x": [i % k for i in range(size)], "x_ind": [i for i in range(size)]}
    ).eval_expression_list([col("x").cast(dtype), col("x_ind")])
    split_tables = table.partition_by_hash([col("x")], k)
    seen_so_far = set()
    for st in split_tables:
        unique_to_table = set()
        for x, x_ind in zip(st.get_column("x").to_pylist(), st.get_column("x_ind").to_pylist()):
            assert (x_ind % k) == int(x)
            unique_to_table.add(x)
        for v in unique_to_table:
            assert v not in seen_so_far
            seen_so_far.add(v)


def test_table_quantiles_bad_input() -> None:
    # negative sample

    table = Table.from_pydict({"x": [1, 2, 3], "b": [0, 1, 2]})

    with pytest.raises(ValueError, match="negative number"):
        table.partition_by_hash([col("x")], -1)

    with pytest.raises(ValueError, match="0 partitions"):
        table.partition_by_hash([col("x")], 0)
