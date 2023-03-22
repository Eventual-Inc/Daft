from __future__ import annotations

import datetime
import itertools

import numpy as np
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
daft_nonnull_types = daft_numeric_types + daft_string_types + [DataType.bool(), DataType.binary(), DataType.date()]


def test_from_pydict_list() -> None:
    daft_table = Table.from_pydict({"a": [1, 2, 3]})
    assert "a" in daft_table.column_names()
    assert daft_table.to_arrow()["a"].combine_chunks() == pa.array([1, 2, 3], type=pa.int64())


def test_from_pydict_np() -> None:
    daft_table = Table.from_pydict({"a": np.array([1, 2, 3], dtype=np.int64)})
    assert "a" in daft_table.column_names()
    assert daft_table.to_arrow()["a"].combine_chunks() == pa.array([1, 2, 3], type=pa.int64())


def test_from_pydict_arrow() -> None:
    daft_table = Table.from_pydict({"a": pa.array([1, 2, 3], type=pa.int8())})
    assert "a" in daft_table.column_names()
    assert daft_table.to_arrow()["a"].combine_chunks() == pa.array([1, 2, 3], type=pa.int8())


def test_from_pydict_series() -> None:
    daft_table = Table.from_pydict({"a": Series.from_arrow(pa.array([1, 2, 3], type=pa.int8()))})
    assert "a" in daft_table.column_names()
    assert daft_table.to_arrow()["a"].combine_chunks() == pa.array([1, 2, 3], type=pa.int8())


def test_from_arrow_round_trip() -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    daft_table = Table.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]
    read_back = daft_table.to_arrow()
    assert pa_table == read_back


def test_from_pydict_bad_input() -> None:
    with pytest.raises(ValueError, match="Mismatch in Series lengths"):
        Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7]})


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


def test_table_sample() -> None:
    pa_table = pa.Table.from_pydict({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
    source_pairs = {(1, 5), (2, 6), (3, 7), (4, 8)}

    daft_table = Table.from_arrow(pa_table)
    assert len(daft_table) == 4
    assert daft_table.column_names() == ["a", "b"]

    # subsample
    sampled = daft_table.sample(3)
    assert len(sampled) == 3
    assert sampled.column_names() == ["a", "b"]
    assert all(
        pair in source_pairs for pair in zip(sampled.get_column("a").to_pylist(), sampled.get_column("b").to_pylist())
    )

    # oversample
    sampled = daft_table.sample(5)
    assert len(sampled) == 4
    assert sampled.column_names() == ["a", "b"]
    assert all(
        pair in source_pairs for pair in zip(sampled.get_column("a").to_pylist(), sampled.get_column("b").to_pylist())
    )

    # negative sample
    with pytest.raises(ValueError, match="negative number"):
        daft_table.sample(-1)


@pytest.mark.parametrize("size, k", itertools.product([0, 1, 10, 33, 100, 101], [0, 1, 2, 3, 100, 101, 200]))
def test_table_quantiles(size, k) -> None:
    first = np.arange(size)

    second = 2 * first

    daft_table = Table.from_pydict({"a": first, "b": second})
    assert len(daft_table) == size
    assert daft_table.column_names() == ["a", "b"]

    # sub
    quantiles = daft_table.quantiles(k)

    if size > 0:
        assert len(quantiles) == max(k - 1, 0)
    else:
        assert len(quantiles) == 0

    assert quantiles.column_names() == ["a", "b"]
    ind = quantiles.get_column("a").to_pylist()

    if k > 0:
        assert np.all(np.diff(ind) >= 0)
        expected_delta = size / k
        assert np.all(np.abs(np.diff(ind) - expected_delta) <= 1)
    else:
        assert len(ind) == 0


def test_table_quantiles_bad_input() -> None:
    # negative sample

    first = np.arange(10)

    second = 2 * first

    pa_table = pa.Table.from_pydict({"a": first, "b": second})

    daft_table = Table.from_arrow(pa_table)

    with pytest.raises(ValueError, match="negative number"):
        daft_table.quantiles(-1)


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


test_table_count_cases = [
    ([], {"count": [0]}),
    ([None], {"count": [0]}),
    ([None, None, None], {"count": [0]}),
    ([0], {"count": [1]}),
    ([None, 0, None, 0, None], {"count": [2]}),
]


@pytest.mark.parametrize("idx_dtype", daft_nonnull_types, ids=[f"{_}" for _ in daft_nonnull_types])
@pytest.mark.parametrize("case", test_table_count_cases, ids=[f"{_}" for _ in test_table_count_cases])
def test_table_count(idx_dtype, case) -> None:
    input, expected = case
    if idx_dtype == DataType.date():
        input = [datetime.datetime.utcfromtimestamp(_) if _ is not None else None for _ in input]
    daft_table = Table.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").cast(idx_dtype)])
    daft_table = daft_table.eval_expression_list([col("input").alias("count")._count()])

    res = daft_table.to_pydict()
    assert res == expected


@pytest.mark.parametrize("length", [1, 10])
def test_table_count_nulltype(length) -> None:
    """Count on NullType in Arrow counts the nulls (instead of ignoring them)."""
    daft_table = Table.from_pydict({"input": [None] * length})
    daft_table = daft_table.eval_expression_list([col("input").cast(DataType.null())])
    daft_table = daft_table.eval_expression_list([col("input").alias("count")._count()])

    res = daft_table.to_pydict()["count"]
    assert res == [length]


test_table_minmax_numerics_cases = [
    ([], {"min": [None], "max": [None]}),
    ([None], {"min": [None], "max": [None]}),
    ([None, None, None], {"min": [None], "max": [None]}),
    ([5], {"min": [5], "max": [5]}),
    ([None, 5], {"min": [5], "max": [5]}),
    ([None, 1, 21, None, 21, 1, None], {"min": [1], "max": [21]}),
]


@pytest.mark.parametrize("idx_dtype", daft_numeric_types)
@pytest.mark.parametrize(
    "case", test_table_minmax_numerics_cases, ids=[f"{_}" for _ in test_table_minmax_numerics_cases]
)
def test_table_minmax_numerics(idx_dtype, case) -> None:
    input, expected = case
    daft_table = Table.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").cast(idx_dtype)])
    daft_table = daft_table.eval_expression_list(
        [
            col("input").alias("min")._min(),
            col("input").alias("max")._max(),
        ]
    )

    res = daft_table.to_pydict()
    assert res == expected


test_table_minmax_string_cases = [
    ([], {"min": [None], "max": [None]}),
    ([None], {"min": [None], "max": [None]}),
    ([None, None, None], {"min": [None], "max": [None]}),
    (["abc"], {"min": ["abc"], "max": ["abc"]}),
    ([None, "abc"], {"min": ["abc"], "max": ["abc"]}),
    ([None, "aaa", None, "b", "c", "aaa", None], {"min": ["aaa"], "max": ["c"]}),
]


@pytest.mark.parametrize("idx_dtype", daft_string_types)
@pytest.mark.parametrize("case", test_table_minmax_string_cases, ids=[f"{_}" for _ in test_table_minmax_string_cases])
def test_table_minmax_string(idx_dtype, case) -> None:
    input, expected = case
    daft_table = Table.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").cast(idx_dtype)])
    daft_table = daft_table.eval_expression_list(
        [
            col("input").alias("min")._min(),
            col("input").alias("max")._max(),
        ]
    )

    res = daft_table.to_pydict()
    assert res == expected


test_table_minmax_bool_cases = [
    ([], {"min": [None], "max": [None]}),
    ([None], {"min": [None], "max": [None]}),
    ([None, None, None], {"min": [None], "max": [None]}),
    ([False, True], {"min": [False], "max": [True]}),
    ([None, None, True, None], {"min": [True], "max": [True]}),
]


@pytest.mark.parametrize("case", test_table_minmax_bool_cases, ids=[f"{_}" for _ in test_table_minmax_bool_cases])
def test_table_minmax_bool(case) -> None:
    input, expected = case
    daft_table = Table.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").cast(DataType.bool())])
    daft_table = daft_table.eval_expression_list(
        [
            col("input").alias("min")._min(),
            col("input").alias("max")._max(),
        ]
    )

    res = daft_table.to_pydict()
    assert res == expected


test_table_sum_mean_cases = [
    ([], {"sum": [None], "mean": [None]}),
    ([None], {"sum": [None], "mean": [None]}),
    ([None, None, None], {"sum": [None], "mean": [None]}),
    ([0], {"sum": [0], "mean": [0]}),
    ([1], {"sum": [1], "mean": [1]}),
    ([None, 3, None, None, 1, 2, 0, None], {"sum": [6], "mean": [1.5]}),
]


@pytest.mark.parametrize("idx_dtype", daft_numeric_types, ids=[f"{_}" for _ in daft_numeric_types])
@pytest.mark.parametrize("case", test_table_sum_mean_cases, ids=[f"{_}" for _ in test_table_sum_mean_cases])
def test_table_sum_mean(idx_dtype, case) -> None:
    input, expected = case
    daft_table = Table.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").cast(idx_dtype)])
    daft_table = daft_table.eval_expression_list(
        [
            col("input").alias("sum")._sum(),
            col("input").alias("mean")._mean(),
        ]
    )

    res = daft_table.to_pydict()
    assert res == expected


@pytest.mark.parametrize("nptype", [np.uint8, np.uint16, np.uint32, np.int8, np.int16, np.int32])
def test_table_sum_upcast(nptype) -> None:
    """Tests correctness, including type upcasting, of sum aggregations."""
    daft_table = Table.from_pydict(
        {
            "maxes": np.full(128, fill_value=np.iinfo(nptype).max, dtype=nptype),
            "mins": np.full(128, fill_value=np.iinfo(nptype).min, dtype=nptype),
        }
    )
    daft_table = daft_table.eval_expression_list([col("maxes")._sum(), col("mins")._sum()])
    pydict = daft_table.to_pydict()
    assert pydict["maxes"] == [128 * np.iinfo(nptype).max]
    assert pydict["mins"] == [128 * np.iinfo(nptype).min]


def test_table_sum_badtype() -> None:
    daft_table = Table.from_pydict({"a": ["str1", "str2"]})
    with pytest.raises(ValueError):
        daft_table = daft_table.eval_expression_list([col("a")._sum()])


@pytest.mark.parametrize(
    "input,expr,expected",
    [
        pytest.param([True, False, None], ~col("input"), [False, True, None], id="BooleanColumn"),
        pytest.param(["apple", None, "banana"], ~(col("input") != "banana"), [False, None, True], id="BooleanExpr"),
        pytest.param([], ~(col("input").cast(DataType.bool())), [], id="EmptyColumn"),
    ],
)
def test_table_expr_not(input, expr, expected) -> None:
    """Test logical not expression."""
    daft_table = Table.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([expr])
    pydict = daft_table.to_pydict()

    assert pydict["input"] == expected


def test_table_expr_not_wrong() -> None:
    daft_table = Table.from_pydict({"input": [None, 0, 1]})

    with pytest.raises(ValueError):
        daft_table = daft_table.eval_expression_list([~col("input")])


@pytest.mark.parametrize(
    "input,expected",
    [
        pytest.param([True, False, None], [False, False, True], id="BooleanColumn"),
        pytest.param(["a", None, "b", "c", None], [False, True, False, False, True], id="StringColumn"),
        pytest.param([None, None], [True, True], id="NullColumn"),
        pytest.param([], [], id="EmptyColumn"),
    ],
)
def test_table_expr_is_null(input, expected) -> None:
    """Test logical not expression."""
    daft_table = Table.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").is_null()])
    pydict = daft_table.to_pydict()

    assert pydict["input"] == expected


test_table_agg_global_cases = [
    (
        [],
        {
            "count": [0],
            "sum": [None],
            "mean": [None],
            "min": [None],
            "max": [None],
        },
    ),
    (
        [None],
        {
            "count": [0],
            "sum": [None],
            "mean": [None],
            "min": [None],
            "max": [None],
        },
    ),
    (
        [None, None, None],
        {
            "count": [0],
            "sum": [None],
            "mean": [None],
            "min": [None],
            "max": [None],
        },
    ),
    (
        [None, 3, None, None, 1, 2, 0, None],
        {
            "count": [4],
            "sum": [6],
            "mean": [1.5],
            "min": [0],
            "max": [3],
        },
    ),
]


@pytest.mark.parametrize("case", test_table_agg_global_cases, ids=[f"{_}" for _ in test_table_agg_global_cases])
def test_table_agg_global(case) -> None:
    """Test that global aggregation works at the API layer."""
    input, expected = case
    daft_table = Table.from_pydict({"input": input})
    daft_table = daft_table.agg(
        [
            (col("input").cast(DataType.int32()).alias("count"), "count"),
            (col("input").cast(DataType.int32()).alias("sum"), "sum"),
            (col("input").cast(DataType.int32()).alias("mean"), "mean"),
            (col("input").cast(DataType.int32()).alias("min"), "min"),
            (col("input").cast(DataType.int32()).alias("max"), "max"),
        ]
    )

    result = daft_table.to_pydict()
    for key, value in expected.items():
        assert result[key] == value


@pytest.mark.parametrize(
    "groups_and_aggs",
    [
        (["col_A"], ["col_B"]),
        (["col_A", "col_B"], []),
    ],
)
def test_table_agg_groupby_empty(groups_and_aggs) -> None:
    groups, aggs = groups_and_aggs
    daft_table = Table.from_pydict({"col_A": [], "col_B": []})
    daft_table = daft_table.agg(
        [(col(a), "count") for a in aggs],
        [col(g).cast(DataType.int32()) for g in groups],
    )
    res = daft_table.to_pydict()

    assert res == {"col_A": [], "col_B": []}


test_table_agg_groupby_cases = [
    {
        # Group by strings.
        "groups": ["name"],
        "aggs": [("cookies", "sum"), ("name", "count")],
        "expected": {"name": ["Alice", "Bob", None], "sum": [None, 10, 7], "count": [4, 4, 0]},
    },
    {
        # Group by numbers.
        "groups": ["cookies"],
        "aggs": [("name", "count")],
        "expected": {"cookies": [2, 5, None], "count": [0, 2, 6]},
    },
    {
        # Group by multicol.
        "groups": ["name", "cookies"],
        "aggs": [("name", "count")],
        "expected": {
            "name": ["Alice", "Bob", "Bob", None, None, None],
            "cookies": [None, 5, None, 2, 5, None],
            "count": [4, 2, 2, 0, 0, 0],
        },
    },
]


@pytest.mark.parametrize(
    "case", test_table_agg_groupby_cases, ids=[f"{case['groups']}" for case in test_table_agg_groupby_cases]
)
def test_table_agg_groupby(case) -> None:
    values = [
        ("Bob", None),
        ("Bob", None),
        ("Bob", 5),
        ("Bob", 5),
        (None, None),
        (None, 5),
        (None, None),
        (None, 2),
        ("Alice", None),
        ("Alice", None),
        ("Alice", None),
        ("Alice", None),
    ]
    daft_table = Table.from_pydict(
        {
            "name": [_[0] for _ in values],
            "cookies": [_[1] for _ in values],
        }
    )
    daft_table = daft_table.agg(
        [(col(aggcol).alias(aggfn), aggfn) for aggcol, aggfn in case["aggs"]],
        [col(group) for group in case["groups"]],
    )
    assert daft_table.to_pydict() == case["expected"]


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


@pytest.mark.parametrize(
    "sort_dtype, value_dtype, data",
    itertools.product(
        daft_numeric_types + daft_string_types,
        daft_numeric_types + daft_string_types,
        [
            ([None, 4, 2, 1, 5], [0, 1, 2, 3, None], False, False, [3, 2, 1, 4, 0]),
            ([None, 4, 2, 1, 5], [0, 1, 2, 3, None], False, True, [3, 2, 1, 4, 0]),
            ([1, 1, 1, 1, 1], [None, 3, 1, 2, 0], False, False, [4, 2, 3, 1, 0]),
            ([1, 1, 1, 1, 1], [None, 3, 1, 2, 0], True, False, [4, 2, 3, 1, 0]),
            ([None, None, None, None, None], [None, 3, 1, 2, 0], False, False, [4, 2, 3, 1, 0]),
            ([None, None, None, None, None], [None, 3, 1, 2, 0], True, False, [4, 2, 3, 1, 0]),
            ([None, 4, 2, 1, 5], [None, None, None, None, None], False, False, [3, 2, 1, 4, 0]),
            ([None, 4, 2, 1, 5], [None, None, None, None, None], False, True, [3, 2, 1, 4, 0]),
        ],
    ),
)
def test_table_multiple_col_sorting(sort_dtype, value_dtype, data) -> None:
    a, b, a_desc, b_desc, expected = data
    pa_table = pa.Table.from_pydict({"a": a, "b": b})

    argsort_order = Series.from_pylist(expected)

    daft_table = Table.from_arrow(pa_table)

    daft_table = daft_table.eval_expression_list([col("a").cast(sort_dtype), col("b").cast(value_dtype)])

    assert len(daft_table) == 5
    assert daft_table.column_names() == ["a", "b"]

    sorted_table = daft_table.sort([col("a"), col("b")], descending=[a_desc, b_desc])

    assert len(sorted_table) == 5

    assert sorted_table.column_names() == ["a", "b"]

    assert sorted_table.get_column("a").datatype() == daft_table.get_column("a").datatype()
    assert sorted_table.get_column("b").datatype() == daft_table.get_column("b").datatype()

    assert sorted_table.get_column("a").to_pylist() == daft_table.get_column("a").take(argsort_order).to_pylist()
    assert sorted_table.get_column("b").to_pylist() == daft_table.get_column("b").take(argsort_order).to_pylist()

    assert (
        daft_table.argsort([col("a"), col("b")], descending=[a_desc, b_desc]).to_pylist() == argsort_order.to_pylist()
    )

    # Descending

    sorted_table = daft_table.sort([col("a"), col("b")], descending=[not a_desc, not b_desc])

    assert len(sorted_table) == 5

    assert sorted_table.column_names() == ["a", "b"]

    assert sorted_table.get_column("a").datatype() == daft_table.get_column("a").datatype()
    assert sorted_table.get_column("b").datatype() == daft_table.get_column("b").datatype()

    assert sorted_table.get_column("a").to_pylist() == daft_table.get_column("a").take(argsort_order).to_pylist()[::-1]
    assert sorted_table.get_column("b").to_pylist() == daft_table.get_column("b").take(argsort_order).to_pylist()[::-1]

    assert (
        daft_table.argsort([col("a"), col("b")], descending=[not a_desc, not b_desc]).to_pylist()
        == argsort_order.to_pylist()[::-1]
    )


@pytest.mark.parametrize(
    "second_dtype, data",
    itertools.product(
        daft_numeric_types + daft_string_types,
        [
            ([None, True, False, True, False], [0, 1, 2, 3, None], False, False, [2, 4, 1, 3, 0]),
            ([None, True, False, True, False], [0, 1, 2, 3, None], True, False, [0, 1, 3, 2, 4]),
            ([True, True, True, True, True], [None, 3, 1, 2, 0], False, False, [4, 2, 3, 1, 0]),
            ([True, True, True, True, True], [None, 3, 1, 2, 0], True, False, [4, 2, 3, 1, 0]),
            ([None, None, None, None, None], [None, 3, 1, 2, 0], False, False, [4, 2, 3, 1, 0]),
            ([None, None, None, None, None], [None, 3, 1, 2, 0], True, False, [4, 2, 3, 1, 0]),
        ],
    ),
)
def test_table_boolean_multiple_col_sorting(second_dtype, data) -> None:
    a, b, a_desc, b_desc, expected = data
    pa_table = pa.Table.from_pydict({"a": a, "b": b})
    argsort_order = Series.from_pylist(expected)

    daft_table = Table.from_arrow(pa_table)

    daft_table = daft_table.eval_expression_list([col("a"), col("b").cast(second_dtype)])

    assert len(daft_table) == 5
    assert daft_table.column_names() == ["a", "b"]

    sorted_table = daft_table.sort([col("a"), col("b")], descending=[a_desc, b_desc])

    assert len(sorted_table) == 5

    assert sorted_table.column_names() == ["a", "b"]

    assert sorted_table.get_column("a").datatype() == daft_table.get_column("a").datatype()
    assert sorted_table.get_column("b").datatype() == daft_table.get_column("b").datatype()

    assert sorted_table.get_column("a").to_pylist() == daft_table.get_column("a").take(argsort_order).to_pylist()
    assert sorted_table.get_column("b").to_pylist() == daft_table.get_column("b").take(argsort_order).to_pylist()

    assert (
        daft_table.argsort([col("a"), col("b")], descending=[a_desc, b_desc]).to_pylist() == argsort_order.to_pylist()
    )

    # Descending

    sorted_table = daft_table.sort([col("a"), col("b")], descending=[not a_desc, not b_desc])

    assert len(sorted_table) == 5

    assert sorted_table.column_names() == ["a", "b"]

    assert sorted_table.get_column("a").datatype() == daft_table.get_column("a").datatype()
    assert sorted_table.get_column("b").datatype() == daft_table.get_column("b").datatype()

    assert sorted_table.get_column("a").to_pylist() == daft_table.get_column("a").take(argsort_order).to_pylist()[::-1]
    assert sorted_table.get_column("b").to_pylist() == daft_table.get_column("b").take(argsort_order).to_pylist()[::-1]

    assert (
        daft_table.argsort([col("a"), col("b")], descending=[not a_desc, not b_desc]).to_pylist()
        == argsort_order.to_pylist()[::-1]
    )


def test_table_size_bytes() -> None:
    data = Table.from_pydict({"a": [1, 2, 3, 4, None], "b": [False, True, False, True, None]}).eval_expression_list(
        [col("a").cast(DataType.int64()), col("b")]
    )
    assert data.size_bytes() == (5 * 8 + 1) + (1 + 1)


def test_table_numeric_abs() -> None:
    table = Table.from_pydict({"a": [None, -1.0, 0, 2, 3, None], "b": [-1, -2, 3, 4, None, None]})

    abs_table = table.eval_expression_list([abs(col("a")), col("b").abs()])

    assert [abs(v) if v is not None else v for v in table.get_column("a").to_pylist()] == abs_table.get_column(
        "a"
    ).to_pylist()
    assert [abs(v) if v is not None else v for v in table.get_column("b").to_pylist()] == abs_table.get_column(
        "b"
    ).to_pylist()


def test_table_abs_bad_input() -> None:
    table = Table.from_pydict({"a": ["a", "b", "c"]})

    with pytest.raises(ValueError, match="Expected input to abs to be numeric"):
        table.eval_expression_list([abs(col("a"))])


def test_table_concat() -> None:
    tables = [
        Table.from_pydict({"x": [1, 2, 3], "y": ["a", "b", "c"]}),
        Table.from_pydict({"x": [4, 5, 6], "y": ["d", "e", "f"]}),
    ]

    result = Table.concat(tables)
    assert result.to_pydict() == {"x": [1, 2, 3, 4, 5, 6], "y": ["a", "b", "c", "d", "e", "f"]}

    tables = [
        Table.from_pydict({"x": [], "y": []}),
        Table.from_pydict({"x": [], "y": []}),
    ]

    result = Table.concat(tables)
    assert result.to_pydict() == {"x": [], "y": []}


def test_table_concat_bad_input() -> None:
    mix_types_table = [Table.from_pydict({"x": [1, 2, 3]}), []]
    with pytest.raises(TypeError, match="Expected a Table for concat"):
        Table.concat(mix_types_table)

    with pytest.raises(ValueError, match="Need at least 1 table"):
        Table.concat([])


def test_table_concat_schema_mismatch() -> None:
    mix_types_table = [
        Table.from_pydict({"x": [1, 2, 3]}),
        Table.from_pydict({"y": [1, 2, 3]}),
    ]

    with pytest.raises(ValueError, match="Table concat requires all schemas to match"):
        Table.concat(mix_types_table)

    mix_types_table = [
        Table.from_pydict({"x": [1, 2, 3]}),
        Table.from_pydict({"x": [1.0, 2.0, 3.0]}),
    ]

    with pytest.raises(ValueError, match="Table concat requires all schemas to match"):
        Table.concat(mix_types_table)

    mix_types_table = [
        Table.from_pydict({"x": [1, 2, 3]}),
        Table.from_pydict({"x": [1, 2, 3], "y": [2, 3, 4]}),
    ]

    with pytest.raises(ValueError, match="Table concat requires all schemas to match"):
        Table.concat(mix_types_table)


def test_string_table_sorting():
    daft_table = Table.from_pydict(
        {
            "firstname": [
                "bob",
                "alice",
                "eve",
                None,
                None,
                "bob",
                "alice",
            ],
            "lastname": ["a", "a", "a", "bond", None, None, "a"],
        }
    )
    sorted_table = daft_table.sort([col("firstname"), col("lastname")])
    assert sorted_table.to_pydict() == {
        "firstname": ["alice", "alice", "bob", "bob", "eve", None, None],
        "lastname": ["a", "a", "a", None, "a", "bond", None],
    }


def test_table_filter_with_dates() -> None:
    from datetime import date

    def date_maker(d):
        if d is None:
            return None
        return date(2023, 1, d)

    days = list(map(date_maker, [5, 4, 1, None, 2, None]))
    pa_table = pa.Table.from_pydict({"days": days, "enum": [0, 1, 2, 3, 4, 5]})
    daft_table = Table.from_arrow(pa_table)
    assert len(daft_table) == 6
    assert daft_table.column_names() == ["days", "enum"]

    exprs = [(col("days") > date(2023, 1, 2)) & (col("enum") > 0)]
    new_table = daft_table.filter(exprs)
    assert len(new_table) == 1
    assert new_table.column_names() == ["days", "enum"]
    result = new_table.to_pydict()
    assert result["days"] == [date(2023, 1, 4)]
    assert result["enum"] == [1]


def test_table_filter_with_date_years() -> None:
    from datetime import date

    def date_maker(y):
        if y is None:
            return None
        return date(y, 1, 1)

    days = list(map(date_maker, [5, 4000, 1, None, 2022, None]))
    pa_table = pa.Table.from_pydict({"days": days, "enum": [0, 1, 2, 3, 4, 5]})
    daft_table = Table.from_arrow(pa_table)
    assert len(daft_table) == 6
    assert daft_table.column_names() == ["days", "enum"]

    exprs = [col("days").dt.year() > 2000]
    new_table = daft_table.filter(exprs)
    assert len(new_table) == 2
    assert new_table.column_names() == ["days", "enum"]
    result = new_table.to_pydict()
    assert result["days"] == [date(4000, 1, 1), date(2022, 1, 1)]
    assert result["enum"] == [1, 4]
