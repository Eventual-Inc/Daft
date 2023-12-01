from __future__ import annotations

import itertools

import pyarrow as pa
import pytest

from daft import col
from daft.logical.schema import Schema
from daft.series import Series
from daft.table import MicroPartition
from tests.table import daft_int_types, daft_numeric_types


@pytest.mark.parametrize(
    "mp",
    [
        MicroPartition.from_pydict({"a": pa.array([], type=pa.int64())}),  # 1 empty table
        MicroPartition.empty(Schema.from_pyarrow_schema(pa.schema({"a": pa.int64()}))),  # No tables
    ],
)
def test_micropartitions_take_empty(mp) -> None:
    assert len(mp) == 0

    indices = Series.from_arrow(pa.array([], type=pa.int64()))
    taken = mp.take(indices)
    assert len(taken) == 0
    assert taken.column_names() == ["a"]
    assert taken.to_pydict() == {"a": []}

    indices = Series.from_arrow(pa.array([1], type=pa.int64()))

    with pytest.raises(BaseException, match="index out of bounds"):
        taken = mp.take(indices)


@pytest.mark.parametrize(
    "mp",
    [
        MicroPartition.from_pydict({"a": [1, 2, 3, 4]}),  # 1 table
        MicroPartition.concat(
            [
                MicroPartition.from_pydict({"a": pa.array([], type=pa.int64())}),
                MicroPartition.from_pydict({"a": [1]}),
                MicroPartition.from_pydict({"a": [2, 3, 4]}),
            ]
        ),  # 3 tables
    ],
)
def test_micropartitions_take(mp: MicroPartition) -> None:
    assert mp.column_names() == ["a"]
    assert len(mp) == 4

    indices = Series.from_pylist([0, 1])
    taken = mp.take(indices)
    assert len(taken) == 2
    assert taken.column_names() == ["a"]
    assert taken.to_pydict() == {"a": [1, 2]}

    indices = Series.from_pylist([3, 2])
    taken = mp.take(indices)
    assert len(taken) == 2
    assert taken.column_names() == ["a"]
    assert taken.to_pydict() == {"a": [4, 3]}

    indices = Series.from_pylist([3, 2, 2, 2, 3])
    taken = mp.take(indices)
    assert len(taken) == 5
    assert taken.column_names() == ["a"]
    assert taken.to_pydict() == {"a": [4, 3, 3, 3, 4]}


@pytest.mark.parametrize("data_dtype, idx_dtype", itertools.product(daft_numeric_types, daft_int_types))
def test_table_take_numeric(data_dtype, idx_dtype) -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = MicroPartition.from_arrow(pa_table)
    daft_table = daft_table.eval_expression_list([col("a").cast(data_dtype), col("b")])

    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    indices = Series.from_pylist([0, 1]).cast(idx_dtype)

    taken = daft_table.take(indices)
    assert len(taken) == 2
    assert taken.column_names() == ["a", "b"]

    assert taken.to_pydict() == {"a": [1, 2], "b": [5, 6]}

    indices = Series.from_pylist([3, 2]).cast(idx_dtype)

    taken = daft_table.take(indices)
    assert len(taken) == 2
    assert taken.column_names() == ["a", "b"]

    assert taken.to_pydict() == {"a": [4, 3], "b": [8, 7]}

    indices = Series.from_pylist([3, 2, 2, 2, 3]).cast(idx_dtype)

    taken = daft_table.take(indices)
    assert len(taken) == 5
    assert taken.column_names() == ["a", "b"]

    assert taken.to_pydict() == {"a": [4, 3, 3, 3, 4], "b": [8, 7, 7, 7, 8]}


@pytest.mark.parametrize("idx_dtype", daft_int_types)
def test_table_take_str(idx_dtype) -> None:
    pa_table = pa.Table.from_pydict({"a": ["1", "2", "3", "4"], "b": ["5", "6", "7", "8"]})
    daft_table = MicroPartition.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    indices = Series.from_pylist([0, 1]).cast(idx_dtype)

    taken = daft_table.take(indices)
    assert len(taken) == 2
    assert taken.column_names() == ["a", "b"]

    assert taken.to_pydict() == {"a": ["1", "2"], "b": ["5", "6"]}

    indices = Series.from_pylist([3, 2]).cast(idx_dtype)

    taken = daft_table.take(indices)
    assert len(taken) == 2
    assert taken.column_names() == ["a", "b"]

    assert taken.to_pydict() == {"a": ["4", "3"], "b": ["8", "7"]}

    indices = Series.from_pylist([3, 2, 2, 2, 3]).cast(idx_dtype)

    taken = daft_table.take(indices)
    assert len(taken) == 5
    assert taken.column_names() == ["a", "b"]

    assert taken.to_pydict() == {"a": ["4", "3", "3", "3", "4"], "b": ["8", "7", "7", "7", "8"]}


@pytest.mark.parametrize("idx_dtype", daft_int_types)
def test_table_take_bool(idx_dtype) -> None:
    pa_table = pa.Table.from_pydict({"a": [False, True, False, True], "b": [True, False, True, False]})
    daft_table = MicroPartition.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    indices = Series.from_pylist([0, 1]).cast(idx_dtype)

    taken = daft_table.take(indices)
    assert len(taken) == 2
    assert taken.column_names() == ["a", "b"]

    assert taken.to_pydict() == {"a": [False, True], "b": [True, False]}

    indices = Series.from_pylist([3, 2]).cast(idx_dtype)

    taken = daft_table.take(indices)
    assert len(taken) == 2
    assert taken.column_names() == ["a", "b"]

    assert taken.to_pydict() == {"a": [True, False], "b": [False, True]}

    indices = Series.from_pylist([3, 2, 2, 2, 3]).cast(idx_dtype)

    taken = daft_table.take(indices)
    assert len(taken) == 5
    assert taken.column_names() == ["a", "b"]

    assert taken.to_pydict() == {"a": [True, False, False, False, True], "b": [False, True, True, True, False]}


@pytest.mark.parametrize("idx_dtype", daft_int_types)
def test_table_take_null(idx_dtype) -> None:
    pa_table = pa.Table.from_pydict({"a": [None, None, None, None], "b": [None, None, None, None]})
    daft_table = MicroPartition.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    indices = Series.from_pylist([0, 1]).cast(idx_dtype)

    taken = daft_table.take(indices)
    assert len(taken) == 2
    assert taken.column_names() == ["a", "b"]

    assert taken.to_pydict() == {"a": [None, None], "b": [None, None]}


def test_table_take_pyobject() -> None:
    objects = [object(), None, object(), object()]
    daft_table = MicroPartition.from_pydict({"objs": objects})
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["objs"]

    indices = Series.from_pylist([0, 1])

    taken = daft_table.take(indices)
    assert len(taken) == 2
    assert taken.column_names() == ["objs"]

    assert taken.to_pydict()["objs"] == objects[:2]

    indices = Series.from_pylist([3, 2])

    taken = daft_table.take(indices)
    assert len(taken) == 2
    assert taken.column_names() == ["objs"]

    assert taken.to_pydict()["objs"] == [objects[3], objects[2]]

    indices = Series.from_pylist([3, 2, 2, 2, 3])

    taken = daft_table.take(indices)
    assert len(taken) == 5
    assert taken.column_names() == ["objs"]

    assert taken.to_pydict()["objs"] == [objects[3], objects[2], objects[2], objects[2], objects[3]]


@pytest.mark.parametrize("idx_dtype", daft_int_types)
def test_table_take_fixed_size_list(idx_dtype) -> None:
    pa_table = pa.Table.from_pydict(
        {
            "a": pa.array([[1, 2], [3, None], None, [None, None]], type=pa.list_(pa.int64(), 2)),
            "b": pa.array([[4, 5], [6, None], None, [None, None]], type=pa.list_(pa.int64(), 2)),
        }
    )
    daft_table = MicroPartition.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    indices = Series.from_pylist([0, 1]).cast(idx_dtype)

    taken = daft_table.take(indices)
    assert len(taken) == 2
    assert taken.column_names() == ["a", "b"]

    assert taken.to_pydict() == {"a": [[1, 2], [3, None]], "b": [[4, 5], [6, None]]}

    indices = Series.from_pylist([3, 2]).cast(idx_dtype)

    taken = daft_table.take(indices)
    assert len(taken) == 2
    assert taken.column_names() == ["a", "b"]

    assert taken.to_pydict() == {"a": [[None, None], None], "b": [[None, None], None]}

    indices = Series.from_pylist([3, 2, 2, 2, 3]).cast(idx_dtype)

    taken = daft_table.take(indices)
    assert len(taken) == 5
    assert taken.column_names() == ["a", "b"]

    assert taken.to_pydict() == {
        "a": [[None, None], None, None, None, [None, None]],
        "b": [[None, None], None, None, None, [None, None]],
    }
