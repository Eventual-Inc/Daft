from __future__ import annotations

import datetime
import math

import numpy as np
import pytest

from daft import DataType, col, utils
from daft.series import Series
from daft.table import Table
from tests.table import (
    daft_comparable_types,
    daft_floating_types,
    daft_nonnull_types,
    daft_null_types,
    daft_numeric_types,
    daft_string_types,
)

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


def test_table_count_pyobject() -> None:
    daft_table = Table.from_pydict({"objs": [object(), object(), None, object(), None]})
    daft_table = daft_table.eval_expression_list([col("objs").alias("count")._count()])

    res = daft_table.to_pydict()["count"]
    assert res == [3]


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
        {"count": [0], "sum": [None], "mean": [None], "min": [None], "max": [None], "list": [[]]},
    ),
    (
        [None],
        {"count": [0], "sum": [None], "mean": [None], "min": [None], "max": [None], "list": [[None]]},
    ),
    (
        [None, None, None],
        {"count": [0], "sum": [None], "mean": [None], "min": [None], "max": [None], "list": [[None, None, None]]},
    ),
    (
        [None, 3, None, None, 1, 2, 0, None],
        {
            "count": [4],
            "sum": [6],
            "mean": [1.5],
            "min": [0],
            "max": [3],
            "list": [[None, 3, None, None, 1, 2, 0, None]],
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
            col("input").cast(DataType.int32()).alias("list")._agg_list(),
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
        "aggs": [
            col("cookies").alias("sum")._sum(),
            col("name").alias("count")._count(),
            col("cookies").alias("list")._agg_list(),
        ],
        "expected": {
            "name": ["Alice", "Bob", None],
            "sum": [None, 10, 7],
            "count": [4, 4, 0],
            "list": [[None] * 4, [None, None, 5, 5], [None, 5, None, 2]],
        },
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


@pytest.mark.parametrize("dtype", daft_comparable_types, ids=[f"{_}" for _ in daft_comparable_types])
def test_groupby_all_nulls(dtype) -> None:
    daft_table = Table.from_pydict(
        {
            "group": Series.from_pylist([None, None, None]).cast(dtype),
            "cookies": [1, 2, 3],
        }
    )
    result_table = daft_table.agg([col("cookies")._sum()], group_by=[col("group")])
    assert result_table.to_pydict() == {"group": [None], "cookies": [6]}


@pytest.mark.parametrize(
    "dtype",
    daft_numeric_types + daft_string_types + [DataType.bool()],
    ids=[f"{_}" for _ in daft_numeric_types + daft_string_types + [DataType.bool()]],
)
def test_groupby_numeric_string_bool_some_nulls(dtype) -> None:
    daft_table = Table.from_pydict(
        {
            "group": Series.from_pylist([1, 1, None]).cast(dtype),
            "cookies": [2, 2, 3],
        }
    )
    result_table = daft_table.agg([col("cookies")._sum()], group_by=[col("group")])
    expected_table = Table.from_pydict(
        {
            "group": Series.from_pylist([1, None]).cast(dtype),
            "cookies": [4, 3],
        }
    )

    assert set(utils.freeze(utils.pydict_to_rows(result_table.to_pydict()))) == set(
        utils.freeze(utils.pydict_to_rows(expected_table.to_pydict()))
    )


@pytest.mark.parametrize(
    "dtype",
    daft_numeric_types + daft_string_types + [DataType.bool()],
    ids=[f"{_}" for _ in daft_numeric_types + daft_string_types + [DataType.bool()]],
)
def test_groupby_numeric_string_bool_no_nulls(dtype) -> None:
    daft_table = Table.from_pydict(
        {
            "group": Series.from_pylist([1, 0, 1, 0]).cast(dtype),
            "cookies": [1, 2, 2, 3],
        }
    )
    result_table = daft_table.agg([col("cookies")._sum()], group_by=[col("group")])
    expected_table = Table.from_pydict(
        {
            "group": Series.from_pylist([0, 1]).cast(dtype),
            "cookies": [5, 3],
        }
    )

    assert set(utils.freeze(utils.pydict_to_rows(result_table.to_pydict()))) == set(
        utils.freeze(utils.pydict_to_rows(expected_table.to_pydict()))
    )


@pytest.mark.parametrize(
    "dtype",
    daft_floating_types,
    ids=[f"{_}" for _ in daft_floating_types],
)
def test_groupby_floats_nan(dtype) -> None:
    NAN = float("nan")
    INF = float("inf")

    daft_table = Table.from_pydict(
        {
            "group": Series.from_pylist([None, 1.0, NAN, 5 * NAN, -1 * NAN, -NAN, 1.0, None, INF, -INF, INF]).cast(
                dtype
            ),
            "cookies": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )
    result_table = daft_table.agg([col("cookies")._count()], group_by=[col("group")])
    expected_table = Table.from_pydict(
        {
            "group": Series.from_pylist([None, 1.0, NAN, -INF, INF]).cast(dtype),
            "cookies": [2, 2, 4, 1, 2],
        }
    )
    # have to sort and compare since `utils.pydict_to_rows` doesnt work on NaNs
    for result_col, expected_col in zip(
        result_table.sort([col("group")]).to_pydict(), expected_table.sort([col("group")]).to_pydict()
    ):
        for r, e in zip(result_col, expected_col):
            assert (r == e) or (math.isnan(r) and math.isnan(e))


@pytest.mark.parametrize(
    "dtype", daft_nonnull_types + daft_null_types, ids=[f"{_}" for _ in daft_nonnull_types + daft_null_types]
)
def test_global_list_aggs(dtype) -> None:
    input = [None, 0, 1, 2, None, 4]
    if dtype == DataType.date():
        input = [datetime.date(2020 + x, 1 + x, 1 + x) if x is not None else None for x in input]
    daft_table = Table.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").cast(dtype)])
    result = daft_table.eval_expression_list([col("input").alias("list")._agg_list()])
    assert result.get_column("list").datatype() == DataType.list(dtype)
    assert result.to_pydict() == {"list": [daft_table.to_pydict()["input"]]}


def test_global_pyobj_list_aggs() -> None:
    input = [object(), object(), object()]
    table = Table.from_pydict({"input": input})
    result = table.eval_expression_list([col("input").alias("list")._agg_list()])
    assert result.get_column("list").datatype() == DataType.python()
    assert result.to_pydict()["list"][0] == input


@pytest.mark.parametrize(
    "dtype", daft_nonnull_types + daft_null_types, ids=[f"{_}" for _ in daft_nonnull_types + daft_null_types]
)
def test_grouped_list_aggs(dtype) -> None:
    groups = [None, 1, None, 1, 2, 2]
    input = [None, 0, 1, 2, None, 4]
    expected_idx = [[1, 3], [4, 5], [0, 2]]

    if dtype == DataType.date():
        input = [datetime.date(2020 + x, 1 + x, 1 + x) if x is not None else None for x in input]
    daft_table = Table.from_pydict({"groups": groups, "input": input})
    daft_table = daft_table.eval_expression_list([col("groups"), col("input").cast(dtype)])
    result = daft_table.agg([col("input").alias("list")._agg_list()], group_by=[col("groups")]).sort([col("groups")])
    assert result.get_column("list").datatype() == DataType.list(dtype)

    input_as_dtype = daft_table.get_column("input").to_pylist()
    expected_groups = [[input_as_dtype[i] for i in group] for group in expected_idx]

    assert result.to_pydict() == {"groups": [1, 2, None], "list": expected_groups}


def test_grouped_pyobj_list_aggs() -> None:
    groups = [None, 1, None, 1, 2, 2]
    input = [None, object(), object(), object(), None, object()]
    expected_idx = [[1, 3], [4, 5], [0, 2]]

    daft_table = Table.from_pydict({"groups": groups, "input": input})
    result = daft_table.agg([col("input").alias("list")._agg_list()], group_by=[col("groups")]).sort([col("groups")])
    expected_groups = [[input[i] for i in group] for group in expected_idx]
    assert result.to_pydict() == {"groups": [1, 2, None], "list": expected_groups}


def test_list_aggs_empty() -> None:

    daft_table = Table.from_pydict({"col_A": [], "col_B": []})
    daft_table = daft_table.agg(
        [col("col_A").cast(DataType.int32()).alias("list")._agg_list()],
        group_by=[col("col_B")],
    )
    assert daft_table.get_column("list").datatype() == DataType.list(DataType.int32())
    res = daft_table.to_pydict()

    assert res == {"col_B": [], "list": []}


@pytest.mark.parametrize(
    "dtype", daft_nonnull_types + daft_null_types, ids=[f"{_}" for _ in daft_nonnull_types + daft_null_types]
)
@pytest.mark.parametrize("with_null", [False, True])
def test_global_concat_aggs(dtype, with_null) -> None:
    input = [None, 0, 1, 2, None, 4]

    if dtype == DataType.date():
        input = [datetime.date(2020 + x, 1 + x, 1 + x) if x is not None else None for x in input]

    input = [[x] for x in input]
    if with_null:
        input += [None]

    daft_table = Table.from_pydict({"input": input}).eval_expression_list([col("input").cast(DataType.list(dtype))])
    concated = daft_table.agg([col("input").alias("concat")._agg_concat()])
    assert concated.get_column("concat").datatype() == DataType.list(dtype)

    input_as_dtype = daft_table.get_column("input").to_pylist()
    # We should ignore Null Array elements when performing the concat agg
    expected = [[val[0] for val in input_as_dtype if val is not None]]
    assert concated.to_pydict() == {"concat": expected}


def test_global_concat_aggs_pyobj() -> None:
    expected = [object(), object(), None, None, object()]
    input = [
        [expected[0], expected[1]],
        None,
        [expected[2]],
        [expected[3], expected[4]],
    ]

    table = Table.from_pydict({"input": input})
    concatted = table.agg([col("input").alias("concat")._agg_concat()])
    assert concatted.get_column("concat").datatype() == DataType.python()
    assert concatted.to_pydict()["concat"] == [expected]


@pytest.mark.parametrize(
    "dtype", daft_nonnull_types + daft_null_types, ids=[f"{_}" for _ in daft_nonnull_types + daft_null_types]
)
def test_grouped_concat_aggs(dtype) -> None:
    input = [None, 0, 1, 2, None, 4]

    if dtype == DataType.date():
        input = [datetime.date(2020 + x, 1 + x, 1 + x) if x is not None else None for x in input]

    input = [[x] for x in input] + [None]
    groups = [1, 2, 3, 4, 5, 6, 7]
    daft_table = Table.from_pydict({"groups": groups, "input": input}).eval_expression_list(
        [col("groups"), col("input").cast(DataType.list(dtype))]
    )
    concat_grouped = daft_table.agg([col("input").alias("concat")._agg_concat()], group_by=[col("groups") % 2]).sort(
        [col("groups")]
    )
    assert concat_grouped.get_column("concat").datatype() == DataType.list(dtype)

    input_as_dtype = daft_table.get_column("input").to_pylist()
    # We should ignore Null Array elements when performing the concat agg
    expected_groups = [
        [input_as_dtype[i][0] for i in group if input_as_dtype[i] is not None] for group in [[1, 3, 5], [0, 2, 4, 6]]
    ]
    assert concat_grouped.to_pydict() == {"groups": [0, 1], "concat": expected_groups}


def test_grouped_concat_aggs_pyobj() -> None:
    objects = [object(), object(), object(), object()]
    input = [
        [objects[0], objects[1]],
        None,
        None,
        [objects[2]],
        [None, objects[3]],
    ]

    table = Table.from_pydict({"input": input, "groups": [1, 2, 3, 3, 4]})
    concatted = table.agg([col("input").alias("concat")._agg_concat()], group_by=[col("groups")]).sort([col("groups")])
    assert concatted.get_column("concat").datatype() == DataType.python()
    assert concatted.to_pydict() == {
        "groups": [1, 2, 3, 4],
        "concat": [
            [objects[0], objects[1]],
            [],
            [objects[2]],
            [None, objects[3]],
        ],
    }


def test_concat_aggs_empty() -> None:

    daft_table = Table.from_pydict({"col_A": [], "col_B": []})
    daft_table = daft_table.agg(
        [col("col_A").cast(DataType.list(DataType.int32())).alias("concat")._agg_concat()],
        group_by=[col("col_B")],
    )

    assert daft_table.get_column("concat").datatype() == DataType.list(DataType.int32())
    res = daft_table.to_pydict()

    assert res == {"col_B": [], "concat": []}
