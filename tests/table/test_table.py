from __future__ import annotations

import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.expressions2 import col
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


def test_from_arrow_round_trip() -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = Table.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]
    read_back = daft_table.to_arrow()
    assert pa_table == read_back


def test_table_head() -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = Table.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    # subslice
    headed = daft_table.head(3)
    assert len(headed) == 3
    assert headed.column_names() == ["a", "b"]
    pa_headed = headed.to_arrow()
    assert pa_table[:3] == pa_headed

    # overslice
    headed = daft_table.head(5)
    assert len(headed) == 4
    assert headed.column_names() == ["a", "b"]
    pa_headed = headed.to_arrow()
    assert pa_table == pa_headed

    # negative slice
    with pytest.raises(ValueError, match="negative number"):
        headed = daft_table.head(-1)


def test_table_eval_expressions() -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = Table.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    exprs = [col("a") + col("b"), col("b") * 2]
    new_table = daft_table.eval_expression_list(exprs)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]
    result = new_table.to_pydict()
    assert result["a"] == [6, 8, 10, 12]
    assert result["b"] == [10, 12, 14, 16]


def test_table_eval_expressions_conflict() -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = Table.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    exprs = [col("a") + col("b"), col("a") * 2]

    with pytest.raises(ValueError, match="Duplicate name"):
        daft_table.eval_expression_list(exprs)


@pytest.mark.parametrize("dtype", daft_int_types)
def test_table_take(dtype) -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = Table.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    indices = Series.from_pylist([0, 1]).cast(dtype)

    taken = daft_table.take(indices)
    assert len(taken) == 2
    assert taken.column_names() == ["a", "b"]

    assert taken.to_pydict() == {"a": [1, 2], "b": [5, 6]}

    indices = Series.from_pylist([3, 2]).cast(dtype)

    taken = daft_table.take(indices)
    assert len(taken) == 2
    assert taken.column_names() == ["a", "b"]

    assert taken.to_pydict() == {"a": [4, 3], "b": [8, 7]}

    indices = Series.from_pylist([3, 2, 2, 2, 3]).cast(dtype)

    taken = daft_table.take(indices)
    assert len(taken) == 5
    assert taken.column_names() == ["a", "b"]

    assert taken.to_pydict() == {"a": [4, 3, 3, 3, 4], "b": [8, 7, 7, 7, 8]}
