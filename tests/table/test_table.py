from __future__ import annotations

import itertools

import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.expressions2 import col, lit
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


@pytest.mark.parametrize("data_dtype, idx_dtype", itertools.product(daft_numeric_types, daft_int_types))
def test_table_take_numeric(data_dtype, idx_dtype) -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = Table.from_arrow(pa_table)
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
    daft_table = Table.from_arrow(pa_table)
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
    daft_table = Table.from_arrow(pa_table)
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
    daft_table = Table.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    indices = Series.from_pylist([0, 1]).cast(idx_dtype)

    taken = daft_table.take(indices)
    assert len(taken) == 2
    assert taken.column_names() == ["a", "b"]

    assert taken.to_pydict() == {"a": [None, None], "b": [None, None]}


import operator as ops

OPS = [ops.add, ops.sub, ops.mul, ops.truediv, ops.mod, ops.lt, ops.le, ops.eq, ops.ne, ops.ge, ops.gt]


@pytest.mark.parametrize("data_dtype, op", itertools.product(daft_numeric_types, OPS))
def test_table_numeric_expressions(data_dtype, op) -> None:

    a, b = [5, 6, 7, 8], [1, 2, 3, 4]
    pa_table = pa.Table.from_pydict({"a": a, "b": b})

    daft_table = Table.from_arrow(pa_table)
    daft_table = daft_table.eval_expression_list(
        [op(col("a").cast(data_dtype), col("b").cast(data_dtype)).alias("result")]
    )

    assert len(daft_table) == 4
    assert daft_table.column_names() == ["result"]
    pyresult = [op(l, r) for l, r in zip(a, b)]
    assert daft_table.get_column("result").to_pylist() == pyresult


@pytest.mark.parametrize("data_dtype, op", itertools.product(daft_numeric_types, OPS))
def test_table_numeric_expressions_with_nulls(data_dtype, op) -> None:
    a, b = [5, 6, None, 8, None], [1, 2, 3, None, None]
    pa_table = pa.Table.from_pydict({"a": a, "b": b})

    daft_table = Table.from_arrow(pa_table)
    daft_table = daft_table.eval_expression_list(
        [op(col("a").cast(data_dtype), col("b").cast(data_dtype)).alias("result")]
    )

    assert len(daft_table) == 5
    assert daft_table.column_names() == ["result"]
    pyresult = [op(l, r) for l, r in zip(a[:2], b[:2])]
    assert daft_table.get_column("result").to_pylist()[:2] == pyresult

    assert daft_table.get_column("result").to_pylist()[2:] == [None, None, None]


def test_table_filter_all_pass() -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = Table.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    exprs = [col("a") < col("b"), col("a") < 5]
    new_table = daft_table.filter(exprs)
    assert len(new_table) == 4
    assert new_table.column_names() == ["a", "b"]
    result = new_table.to_pydict()
    assert result["a"] == [1, 2, 3, 4]
    assert result["b"] == [5, 6, 7, 8]

    exprs = [lit(True), lit(True)]
    new_table = daft_table.filter(exprs)
    assert len(new_table) == 4
    assert new_table.column_names() == ["a", "b"]
    result = new_table.to_pydict()
    assert result["a"] == [1, 2, 3, 4]
    assert result["b"] == [5, 6, 7, 8]


def test_table_filter_some_pass() -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = Table.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    exprs = [((col("a") * 4) < col("b")) | (col("b") == 8)]
    new_table = daft_table.filter(exprs)
    assert len(new_table) == 2
    assert new_table.column_names() == ["a", "b"]
    result = new_table.to_pydict()
    assert result["a"] == [1, 4]
    assert result["b"] == [5, 8]

    exprs = [(col("b") / col("a")) >= 3]
    new_table = daft_table.filter(exprs)
    assert len(new_table) == 2
    assert new_table.column_names() == ["a", "b"]
    result = new_table.to_pydict()
    assert result["a"] == [1, 2]
    assert result["b"] == [5, 6]


def test_table_filter_none_pass() -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = Table.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    exprs = [col("a") < col("b"), col("a") > 5]
    new_table = daft_table.filter(exprs)
    assert len(new_table) == 0
    assert new_table.column_names() == ["a", "b"]
    result = new_table.to_pydict()
    assert result["a"] == []
    assert result["b"] == []

    exprs = [col("a") < col("b"), lit(False)]
    new_table = daft_table.filter(exprs)
    assert len(new_table) == 0
    assert new_table.column_names() == ["a", "b"]
    result = new_table.to_pydict()
    assert result["a"] == []
    assert result["b"] == []


def test_table_filter_bad_expression() -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = Table.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    exprs = [col("a") + 1]

    with pytest.raises(ValueError, match="Boolean Series"):
        daft_table.filter(exprs)


@pytest.mark.parametrize(
    "sort_dtype, value_dtype, first_col",
    itertools.product(daft_numeric_types + daft_string_types, daft_numeric_types + daft_string_types, [False, True]),
)
def test_table_single_col_sorting(sort_dtype, value_dtype, first_col) -> None:
    pa_table = pa.Table.from_pydict({"a": [None, 4, 2, 1, 5], "b": [0, 1, 2, 3, None]})

    argsort_order = Series.from_pylist([3, 2, 1, 4, 0])

    daft_table = Table.from_arrow(pa_table)

    if first_col:
        daft_table = daft_table.eval_expression_list([col("a").cast(sort_dtype), col("b").cast(value_dtype)])
    else:
        daft_table = daft_table.eval_expression_list([col("b").cast(value_dtype), col("a").cast(sort_dtype)])

    assert len(daft_table) == 5
    if first_col:
        assert daft_table.column_names() == ["a", "b"]
    else:
        assert daft_table.column_names() == ["b", "a"]

    sorted_table = daft_table.sort([col("a")])

    assert len(sorted_table) == 5

    if first_col:
        assert sorted_table.column_names() == ["a", "b"]
    else:
        assert sorted_table.column_names() == ["b", "a"]

    assert sorted_table.get_column("a").datatype() == daft_table.get_column("a").datatype()
    assert sorted_table.get_column("b").datatype() == daft_table.get_column("b").datatype()

    assert sorted_table.get_column("a").to_pylist() == daft_table.get_column("a").take(argsort_order).to_pylist()
    assert sorted_table.get_column("b").to_pylist() == daft_table.get_column("b").take(argsort_order).to_pylist()

    assert daft_table.argsort([col("a")]).to_pylist() == argsort_order.to_pylist()

    # Descending

    sorted_table = daft_table.sort([col("a")], descending=True)

    assert len(sorted_table) == 5
    if first_col:
        assert sorted_table.column_names() == ["a", "b"]
    else:
        assert sorted_table.column_names() == ["b", "a"]

    assert sorted_table.get_column("a").datatype() == daft_table.get_column("a").datatype()
    assert sorted_table.get_column("b").datatype() == daft_table.get_column("b").datatype()

    assert sorted_table.get_column("a").to_pylist() == daft_table.get_column("a").take(argsort_order).to_pylist()[::-1]
    assert sorted_table.get_column("b").to_pylist() == daft_table.get_column("b").take(argsort_order).to_pylist()[::-1]

    assert daft_table.argsort([col("a")], descending=True).to_pylist() == argsort_order.to_pylist()[::-1]
