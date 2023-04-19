from __future__ import annotations

import datetime

import numpy as np
import pytest

from daft import DataType, col, utils
from daft.table import Table
from tests.table import daft_nonnull_types, daft_numeric_types, daft_string_types

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
        input = [datetime.date(2020 + x, 1 + x, 1 + x) if x is not None else None for x in input]
    daft_table = Table.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").cast(idx_dtype)])
    daft_table = daft_table.eval_expression_list([col("input").alias("count")._count()])

    res = daft_table.to_pydict()
    assert res == expected


@pytest.mark.parametrize("length", [0, 1, 10])
def test_table_count_nulltype(length) -> None:
    daft_table = Table.from_pydict({"input": [None] * length})
    daft_table = daft_table.eval_expression_list([col("input").cast(DataType.null())])
    daft_table = daft_table.eval_expression_list([col("input").alias("count")._count()])

    res = daft_table.to_pydict()["count"]
    assert res == [0]


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
            col("input").cast(DataType.int32()).alias("count")._count(),
            col("input").cast(DataType.int32()).alias("sum")._sum(),
            col("input").cast(DataType.int32()).alias("mean")._mean(),
            col("input").cast(DataType.int32()).alias("min")._min(),
            col("input").cast(DataType.int32()).alias("max")._max(),
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
        [col(a)._count() for a in aggs],
        [col(g).cast(DataType.int32()) for g in groups],
    )
    res = daft_table.to_pydict()

    assert res == {"col_A": [], "col_B": []}


test_table_agg_groupby_cases = [
    {
        # Group by strings.
        "groups": ["name"],
        "aggs": [col("cookies").alias("sum")._sum(), col("name").alias("count")._count()],
        "expected": {"name": ["Alice", "Bob", None], "sum": [None, 10, 7], "count": [4, 4, 0]},
    },
    {
        # Group by numbers.
        "groups": ["cookies"],
        "aggs": [col("name").alias("count")._count()],
        "expected": {"cookies": [2, 5, None], "count": [0, 2, 6]},
    },
    {
        # Group by multicol.
        "groups": ["name", "cookies"],
        "aggs": [col("name").alias("count")._count()],
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
        [aggexpr for aggexpr in case["aggs"]],
        [col(group) for group in case["groups"]],
    )
    assert set(utils.freeze(utils.pydict_to_rows(daft_table.to_pydict()))) == set(
        utils.freeze(utils.pydict_to_rows(case["expected"]))
    )
