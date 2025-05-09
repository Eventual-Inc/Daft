from __future__ import annotations

import datetime
import math

import numpy as np
import pyarrow as pa
import pytest

from daft import DataType, col, from_pydict, utils
from daft.logical.schema import Schema
from daft.recordbatch import MicroPartition
from daft.series import Series
from tests.dataframe.test_aggregations import _assert_all_hashable
from tests.recordbatch import (
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


@pytest.mark.parametrize(
    "mp",
    [
        MicroPartition.from_pydict(
            {"a": pa.array([], type=pa.int64()), "b": pa.array([], type=pa.string())}
        ),  # 1 empty table
        MicroPartition.empty(Schema.from_pyarrow_schema(pa.schema({"a": pa.int64(), "b": pa.string()}))),  # No tables
    ],
)
def test_multipartition_count_empty(mp):
    counted = mp.agg([col("a").count()])
    assert len(counted) == 1
    assert counted.to_pydict() == {"a": [0]}

    counted = mp.agg([col("a").count()], group_by=[col("b")])
    assert len(counted) == 0
    assert counted.to_pydict() == {"b": [], "a": []}


@pytest.mark.parametrize(
    "mp",
    [
        MicroPartition.from_pydict({"a": [1, None, 3, None], "b": ["a", "a", "b", "b"]}),  # 1 table
        MicroPartition.concat(
            [
                MicroPartition.from_pydict({"a": np.array([]).astype(np.int64), "b": pa.array([], type=pa.string())}),
                MicroPartition.from_pydict({"a": [1], "b": ["a"]}),
                MicroPartition.from_pydict({"a": [None, 3, None], "b": ["a", "b", "b"]}),
            ]
        ),  # 3 tables
    ],
)
def test_multipartition_count(mp):
    counted = mp.agg([col("a").count()])
    assert len(counted) == 1
    assert counted.to_pydict() == {"a": [2]}

    counted = mp.agg([col("a").count()], group_by=[col("b")])
    assert len(counted) == 2
    assert counted.to_pydict() == {"b": ["a", "b"], "a": [1, 1]}


@pytest.mark.parametrize("idx_dtype", daft_nonnull_types, ids=[f"{_}" for _ in daft_nonnull_types])
@pytest.mark.parametrize("case", test_table_count_cases, ids=[f"{_}" for _ in test_table_count_cases])
def test_table_count(idx_dtype, case) -> None:
    input, expected = case
    if idx_dtype == DataType.date():
        input = [datetime.date(2020 + x, 1 + x, 1 + x) if x is not None else None for x in input]
    daft_recordbatch = MicroPartition.from_pydict({"input": input})
    daft_recordbatch = daft_recordbatch.eval_expression_list([col("input").cast(idx_dtype)])
    daft_recordbatch = daft_recordbatch.eval_expression_list([col("input").alias("count").count()])

    res = daft_recordbatch.to_pydict()
    assert res == expected


@pytest.mark.parametrize("length", [0, 1, 10])
def test_table_count_nulltype(length) -> None:
    daft_recordbatch = MicroPartition.from_pydict({"input": [None] * length})
    daft_recordbatch = daft_recordbatch.eval_expression_list([col("input").cast(DataType.null())])
    daft_recordbatch = daft_recordbatch.eval_expression_list([col("input").alias("count").count()])

    res = daft_recordbatch.to_pydict()["count"]
    assert res == [0]


def test_table_count_pyobject() -> None:
    daft_recordbatch = MicroPartition.from_pydict({"objs": [object(), object(), None, object(), None]})
    daft_recordbatch = daft_recordbatch.eval_expression_list([col("objs").alias("count").count()])

    res = daft_recordbatch.to_pydict()["count"]
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
    daft_recordbatch = MicroPartition.from_pydict({"input": input})
    daft_recordbatch = daft_recordbatch.eval_expression_list([col("input").cast(idx_dtype)])
    daft_recordbatch = daft_recordbatch.eval_expression_list(
        [
            col("input").alias("min").min(),
            col("input").alias("max").max(),
        ]
    )

    res = daft_recordbatch.to_pydict()
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
    daft_recordbatch = MicroPartition.from_pydict({"input": input})
    daft_recordbatch = daft_recordbatch.eval_expression_list([col("input").cast(idx_dtype)])
    daft_recordbatch = daft_recordbatch.eval_expression_list(
        [
            col("input").alias("min").min(),
            col("input").alias("max").max(),
        ]
    )

    res = daft_recordbatch.to_pydict()
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
    daft_recordbatch = MicroPartition.from_pydict({"input": input})
    daft_recordbatch = daft_recordbatch.eval_expression_list([col("input").cast(DataType.bool())])
    daft_recordbatch = daft_recordbatch.eval_expression_list(
        [
            col("input").alias("min").min(),
            col("input").alias("max").max(),
        ]
    )

    res = daft_recordbatch.to_pydict()
    assert res == expected


test_table_minmax_binary_cases = [
    ([], {"min": [None], "max": [None]}),
    ([None], {"min": [None], "max": [None]}),
    ([None, None, None], {"min": [None], "max": [None]}),
    ([b"1"], {"min": [b"1"], "max": [b"1"]}),
    ([None, b"1"], {"min": [b"1"], "max": [b"1"]}),
    ([b"a", b"b", b"c", b"a"], {"min": [b"a"], "max": [b"c"]}),
]


@pytest.mark.parametrize("case", test_table_minmax_binary_cases, ids=[f"{_}" for _ in test_table_minmax_binary_cases])
@pytest.mark.parametrize("type", [pa.binary(), pa.binary(1)])
def test_table_minmax_binary(case, type) -> None:
    input, expected = case
    daft_recordbatch = MicroPartition.from_arrow(pa.table({"input": pa.array(input, type=type)}))
    daft_recordbatch = daft_recordbatch.eval_expression_list(
        [
            col("input").alias("min").min(),
            col("input").alias("max").max(),
        ]
    )

    res = daft_recordbatch.to_pydict()
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
    daft_recordbatch = MicroPartition.from_pydict({"input": input})
    daft_recordbatch = daft_recordbatch.eval_expression_list([col("input").cast(idx_dtype)])
    daft_recordbatch = daft_recordbatch.eval_expression_list(
        [
            col("input").alias("sum").sum(),
            col("input").alias("mean").mean(),
        ]
    )

    res = daft_recordbatch.to_pydict()
    assert res == expected


@pytest.mark.parametrize("nptype", [np.uint8, np.uint16, np.uint32, np.int8, np.int16, np.int32])
def test_table_sum_upcast(nptype) -> None:
    """Tests correctness, including type upcasting, of sum aggregations."""
    daft_recordbatch = MicroPartition.from_pydict(
        {
            "maxes": np.full(128, fill_value=np.iinfo(nptype).max, dtype=nptype),
            "mins": np.full(128, fill_value=np.iinfo(nptype).min, dtype=nptype),
        }
    )
    daft_recordbatch = daft_recordbatch.eval_expression_list([col("maxes").sum(), col("mins").sum()])
    pydict = daft_recordbatch.to_pydict()
    assert pydict["maxes"] == [128 * np.iinfo(nptype).max]
    assert pydict["mins"] == [128 * np.iinfo(nptype).min]


def test_table_sum_badtype() -> None:
    daft_recordbatch = MicroPartition.from_pydict({"a": ["str1", "str2"]})
    with pytest.raises(ValueError):
        daft_recordbatch = daft_recordbatch.eval_expression_list([col("a").sum()])


test_micropartition_any_value_cases = [
    (
        MicroPartition.from_pydict({"a": [None, 1, None, None], "b": ["a", "a", "b", "b"]}),  # 1 table
        {"a": True, "b": True},
        {"a": False, "b": True},
    ),
    (
        MicroPartition.concat(
            [
                MicroPartition.from_pydict({"a": np.array([]).astype(np.int64), "b": pa.array([], type=pa.string())}),
                MicroPartition.from_pydict({"a": [None, 3, None], "b": ["a", "b", "b"]}),
            ]
        ),  # 2 tables
        {"a": True, "b": True},
        {"a": True, "b": False},
    ),
    (
        MicroPartition.concat(
            [
                MicroPartition.from_pydict({"a": np.array([]).astype(np.int64), "b": pa.array([], type=pa.string())}),
                MicroPartition.from_pydict({"a": [None, 3, None], "b": ["a", "b", "b"]}),
                MicroPartition.from_pydict({"a": [1], "b": ["a"]}),
            ]
        ),  # 3 tables
        {"a": True, "b": True},
        {"a": False, "b": False},
    ),
]


@pytest.mark.parametrize("mp,expected_nulls,expected_no_nulls", test_micropartition_any_value_cases)
def test_micropartition_any_value(mp, expected_nulls, expected_no_nulls):
    any_values = mp.agg([col("a").any_value(False)], group_by=[col("b")]).to_pydict()
    assert len(any_values["b"]) == len(expected_nulls)
    for k, v in zip(any_values["b"], any_values["a"]):
        assert expected_nulls[k] or v is not None

    any_values = mp.agg([col("a").any_value(True)], group_by=[col("b")]).to_pydict()
    assert len(any_values["b"]) == len(expected_no_nulls)
    for k, v in zip(any_values["b"], any_values["a"]):
        assert expected_no_nulls[k] or v is not None


test_table_any_value_cases = [
    ({"a": [1], "b": ["a"]}, {"a": False}, {"a": False}),
    ({"a": [None], "b": ["a"]}, {"a": True}, {"a": True}),
    ({"a": [None, 1], "b": ["a", "a"]}, {"a": True}, {"a": False}),
    ({"a": [1, None, 2], "b": ["a", "b", "b"]}, {"a": False, "b": True}, {"a": False, "b": False}),
]


@pytest.mark.parametrize("case,expected_nulls,expected_no_nulls", test_table_any_value_cases)
def test_table_any_value(case, expected_nulls, expected_no_nulls):
    daft_recordbatch = MicroPartition.from_pydict(case)

    any_values = daft_recordbatch.agg([col("a").any_value(False)], group_by=[col("b")]).to_pydict()
    assert len(any_values["b"]) == len(expected_nulls)
    for k, v in zip(any_values["b"], any_values["a"]):
        assert expected_nulls[k] or v is not None

    any_values = daft_recordbatch.agg([col("a").any_value(True)], group_by=[col("b")]).to_pydict()
    assert len(any_values["b"]) == len(expected_no_nulls)
    for k, v in zip(any_values["b"], any_values["a"]):
        assert expected_no_nulls[k] or v is not None


test_table_agg_global_cases = [
    (
        [],
        {
            "count": [0],
            "sum": [None],
            "mean": [None],
            "min": [None],
            "max": [None],
            "list": [[]],
            "set": [[]],
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
            "list": [[None]],
            "set": [[]],
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
            "list": [[None, None, None]],
            "set": [[]],
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
            "list": [[None, 3, None, None, 1, 2, 0, None]],
            "set": [[0, 1, 2, 3]],
        },
    ),
]


@pytest.mark.parametrize("case", test_table_agg_global_cases, ids=[f"{_}" for _ in test_table_agg_global_cases])
def test_table_agg_global(case) -> None:
    """Test that global aggregation works at the API layer."""
    input, expected = case
    daft_recordbatch = MicroPartition.from_pydict({"input": input})
    daft_recordbatch = daft_recordbatch.agg(
        [
            col("input").cast(DataType.int32()).alias("count").count(),
            col("input").cast(DataType.int32()).alias("sum").sum(),
            col("input").cast(DataType.int32()).alias("mean").mean(),
            col("input").cast(DataType.int32()).alias("min").min(),
            col("input").cast(DataType.int32()).alias("max").max(),
            col("input").cast(DataType.int32()).alias("list").agg_list(),
            col("input").cast(DataType.int32()).alias("set").agg_set(),
        ]
    )

    result = daft_recordbatch.to_pydict()

    # Handle list and set results separately
    res_list = result.pop("list")
    exp_list = expected.pop("list")
    res_set = result.pop("set")
    exp_set = expected.pop("set")

    # Check regular aggregations
    for key, value in expected.items():
        assert result[key] == value

    # Check list result
    assert len(res_list) == 1
    assert res_list[0] == exp_list[0]

    # Check set without nulls
    assert len(res_set) == 1
    _assert_all_hashable(res_set[0], "test_table_agg_global")
    assert len(res_set[0]) == len(set(x for x in res_set[0] if x is not None)), "Result should contain no duplicates"
    assert set(x for x in res_set[0] if x is not None) == set(
        x for x in exp_set[0] if x is not None
    ), "Sets should contain same non-null elements"
    assert None not in res_set[0], "Result should not contain nulls"


@pytest.mark.parametrize(
    "groups_and_aggs",
    [
        (["col_A"], ["col_B"]),
        (["col_A", "col_B"], []),
    ],
)
def test_table_agg_groupby_empty(groups_and_aggs) -> None:
    groups, aggs = groups_and_aggs
    daft_recordbatch = MicroPartition.from_pydict({"col_A": [], "col_B": []})
    daft_recordbatch = daft_recordbatch.agg(
        [col(a).count() for a in aggs],
        [col(g).cast(DataType.int32()) for g in groups],
    )
    res = daft_recordbatch.to_pydict()

    assert res == {"col_A": [], "col_B": []}


test_table_agg_groupby_cases = [
    {
        # Group by strings.
        "groups": ["name"],
        "aggs": [
            col("cookies").alias("sum").sum(),
            col("name").alias("count").count(),
            col("cookies").alias("list").agg_list(),
            col("cookies").alias("set").agg_set(),
        ],
        "expected": {
            "name": ["Alice", "Bob", None],
            "sum": [None, 10, 7],
            "count": [4, 4, 0],
            "list": [[None] * 4, [None, None, 5, 5], [None, 5, None, 2]],
            "set": [set(), {5}, {2, 5}],
        },
    },
    {
        # Group by numbers.
        "groups": ["cookies"],
        "aggs": [col("name").alias("count").count()],
        "expected": {"cookies": [2, 5, None], "count": [0, 2, 6]},
    },
    {
        # Group by multicol.
        "groups": ["name", "cookies"],
        "aggs": [col("name").alias("count").count()],
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
    daft_recordbatch = MicroPartition.from_pydict(
        {
            "name": [_[0] for _ in values],
            "cookies": [_[1] for _ in values],
        }
    )
    # Sort by grouping columns after aggregation
    daft_recordbatch = daft_recordbatch.agg(
        [aggexpr for aggexpr in case["aggs"]],
        [col(group) for group in case["groups"]],
    )
    result = daft_recordbatch.sort([col(group) for group in case["groups"]]).to_pydict()
    expected = case["expected"]

    # Compare non-set columns normally
    for key in result:
        if key == "set":
            # Compare set columns by converting to sets
            assert len(result[key]) == len(expected[key]), f"Length mismatch in column {key}"
            for res, exp in zip(result[key], expected[key]):
                _assert_all_hashable(res, "test_table_agg_groupby")
                assert set(res) == exp, f"Set mismatch in column {key}"
        else:
            assert result[key] == expected[key], f"Mismatch in column {key}"


@pytest.mark.parametrize("dtype", daft_comparable_types, ids=[f"{_}" for _ in daft_comparable_types])
def test_groupby_all_nulls(dtype) -> None:
    daft_recordbatch = MicroPartition.from_pydict(
        {
            "group": Series.from_pylist([None, None, None]).cast(dtype),
            "cookies": [1, 2, 3],
        }
    )
    result_table = daft_recordbatch.agg([col("cookies").sum()], group_by=[col("group")])
    assert result_table.to_pydict() == {"group": [None], "cookies": [6]}


@pytest.mark.parametrize(
    "dtype",
    daft_numeric_types + daft_string_types + [DataType.bool()],
    ids=[f"{_}" for _ in daft_numeric_types + daft_string_types + [DataType.bool()]],
)
def test_groupby_numeric_string_bool_some_nulls(dtype) -> None:
    daft_recordbatch = MicroPartition.from_pydict(
        {
            "group": Series.from_pylist([1, 1, None]).cast(dtype),
            "cookies": [2, 2, 3],
        }
    )
    result_table = daft_recordbatch.agg([col("cookies").sum()], group_by=[col("group")])
    expected_table = MicroPartition.from_pydict(
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
    daft_recordbatch = MicroPartition.from_pydict(
        {
            "group": Series.from_pylist([1, 0, 1, 0]).cast(dtype),
            "cookies": [1, 2, 2, 3],
        }
    )
    result_table = daft_recordbatch.agg([col("cookies").sum()], group_by=[col("group")])
    expected_table = MicroPartition.from_pydict(
        {
            "group": Series.from_pylist([0, 1]).cast(dtype),
            "cookies": [5, 3],
        }
    )

    assert set(utils.freeze(utils.pydict_to_rows(result_table.to_pydict()))) == set(
        utils.freeze(utils.pydict_to_rows(expected_table.to_pydict()))
    )


@pytest.mark.parametrize("type", [pa.binary(), pa.binary(1)])
@pytest.mark.parametrize(
    "agg, expected",
    [
        (col("cookies").max(), [b"2", b"4"]),
        (col("cookies").min(), [b"1", b"3"]),
    ],
)
def test_groupby_binary_bool_some_nulls(type, agg, expected) -> None:
    daft_recordbatch = MicroPartition.from_pydict(
        {
            "group": Series.from_arrow(pa.array([b"1", b"1", None, None], type=type)),
            "cookies": Series.from_arrow(pa.array([b"1", b"2", b"3", b"4"], type=type)),
        }
    )
    result_table = daft_recordbatch.agg([agg], group_by=[col("group")])
    expected_table = MicroPartition.from_pydict(
        {
            "group": Series.from_pylist([b"1", None]),
            "cookies": expected,
        }
    )

    assert set(utils.freeze(utils.pydict_to_rows(result_table.to_pydict()))) == set(
        utils.freeze(utils.pydict_to_rows(expected_table.to_pydict()))
    )


@pytest.mark.parametrize("type", [pa.binary(), pa.binary(1)])
@pytest.mark.parametrize(
    "agg, expected",
    [
        (col("cookies").max(), [b"4", b"3"]),
        (col("cookies").min(), [b"2", b"1"]),
    ],
)
def test_groupby_binary_no_nulls(type, agg, expected) -> None:
    daft_recordbatch = MicroPartition.from_pydict(
        {
            "group": Series.from_arrow(pa.array([b"1", b"0", b"1", b"0"], type=type)),
            "cookies": Series.from_arrow(pa.array([b"1", b"2", b"3", b"4"], type=type)),
        }
    )
    result_table = daft_recordbatch.agg([agg], group_by=[col("group")])
    expected_table = MicroPartition.from_pydict(
        {
            "group": Series.from_pylist([b"0", b"1"]),
            "cookies": expected,
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

    daft_recordbatch = MicroPartition.from_pydict(
        {
            "group": Series.from_pylist([None, 1.0, NAN, 5 * NAN, -1 * NAN, -NAN, 1.0, None, INF, -INF, INF]).cast(
                dtype
            ),
            "cookies": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )
    result_table = daft_recordbatch.agg([col("cookies").count()], group_by=[col("group")])
    expected_table = MicroPartition.from_pydict(
        {
            "group": Series.from_pylist([None, 1.0, NAN, -INF, INF]).cast(dtype),
            "cookies": [2, 2, 4, 1, 2],
        }
    )
    # have to sort and compare since `utils.pydict_to_rows` doesn't work on NaNs
    for result_col, expected_col in zip(
        result_table.sort([col("group")]).to_pydict(), expected_table.sort([col("group")]).to_pydict()
    ):
        for r, e in zip(result_col, expected_col):
            assert (r == e) or (math.isnan(r) and math.isnan(e))


def test_groupby_timestamp() -> None:
    daft_recordbatch = MicroPartition.from_pydict(
        {
            "group": Series.from_pylist(
                [
                    datetime.datetime(2020, 1, 1, 0, 0, 0),
                    datetime.datetime(2020, 1, 1, 0, 30, 0),
                    datetime.datetime(2020, 1, 1, 0, 59, 59),
                    datetime.datetime(2020, 1, 1, 1, 0, 0),
                    datetime.datetime(2020, 1, 1, 1, 30, 0),
                    datetime.datetime(2020, 1, 1, 1, 59, 59),
                    datetime.datetime(2020, 1, 1, 2, 0, 0),
                    datetime.datetime(2020, 1, 1, 2, 30, 0),
                    datetime.datetime(2020, 1, 1, 2, 59, 59),
                ]
            ),
            "value": [1, 1, 1, 2, 2, 2, 3, 3, 3],
        }
    )
    result_table = daft_recordbatch.agg([col("value").sum()], group_by=[col("group").dt.truncate("1 hour")])
    expected_table = MicroPartition.from_pydict(
        {
            "group": Series.from_pylist(
                [
                    datetime.datetime(2020, 1, 1, 0, 0, 0),
                    datetime.datetime(2020, 1, 1, 1, 0, 0),
                    datetime.datetime(2020, 1, 1, 2, 0, 0),
                ]
            ),
            "value": [3, 6, 9],
        }
    )

    assert set(utils.freeze(utils.pydict_to_rows(result_table.to_pydict()))) == set(
        utils.freeze(utils.pydict_to_rows(expected_table.to_pydict()))
    )


@pytest.mark.parametrize(
    "dtype", daft_nonnull_types + daft_null_types, ids=[f"{_}" for _ in daft_nonnull_types + daft_null_types]
)
def test_global_list_aggs(dtype) -> None:
    input = [None, 0, 1, 2, None, 4]
    if dtype == DataType.date():
        input = [datetime.date(2020 + x, 1 + x, 1 + x) if x is not None else None for x in input]
    daft_recordbatch = MicroPartition.from_pydict({"input": input})
    daft_recordbatch = daft_recordbatch.eval_expression_list([col("input").cast(dtype)])
    result = daft_recordbatch.eval_expression_list([col("input").alias("list").agg_list()])
    assert result.get_column_by_name("list").datatype() == DataType.list(dtype)
    assert result.to_pydict() == {"list": [daft_recordbatch.to_pydict()["input"]]}


def test_global_pyobj_list_aggs() -> None:
    input = [object(), object(), object()]
    table = MicroPartition.from_pydict({"input": input})
    result = table.eval_expression_list([col("input").alias("list").agg_list()])
    assert result.get_column_by_name("list").datatype() == DataType.python()
    assert result.to_pydict()["list"][0] == input


def test_global_list_list_aggs() -> None:
    input = [[1], [2, 3, 4], [5, None], [], None]
    table = MicroPartition.from_pydict({"input": input})
    result = table.eval_expression_list([col("input").alias("list").agg_list()])
    assert result.get_column_by_name("list").datatype() == DataType.list(DataType.list(DataType.int64()))
    assert result.to_pydict()["list"][0] == input


def test_global_fixed_size_list_list_aggs() -> None:
    input = Series.from_pylist([[1, 2], [3, 4], [5, None], None]).cast(DataType.fixed_size_list(DataType.int64(), 2))
    table = MicroPartition.from_pydict({"input": input})
    result = table.eval_expression_list([col("input").alias("list").agg_list()])
    assert result.get_column_by_name("list").datatype() == DataType.list(DataType.fixed_size_list(DataType.int64(), 2))
    assert result.to_pydict()["list"][0] == [[1, 2], [3, 4], [5, None], None]


def test_global_struct_list_aggs() -> None:
    input = [{"a": 1, "b": 2}, {"a": 3, "b": None}, None]
    table = MicroPartition.from_pydict({"input": input})
    result = table.eval_expression_list([col("input").alias("list").agg_list()])
    assert result.get_column_by_name("list").datatype() == DataType.list(
        DataType.struct({"a": DataType.int64(), "b": DataType.int64()})
    )
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
    daft_recordbatch = MicroPartition.from_pydict({"groups": groups, "input": input})
    daft_recordbatch = daft_recordbatch.eval_expression_list([col("groups"), col("input").cast(dtype)])
    result = daft_recordbatch.agg([col("input").alias("list").agg_list()], group_by=[col("groups")]).sort(
        [col("groups")]
    )
    assert result.get_column_by_name("list").datatype() == DataType.list(dtype)

    input_as_dtype = daft_recordbatch.get_column_by_name("input").to_pylist()
    expected_groups = [[input_as_dtype[i] for i in group] for group in expected_idx]

    assert result.to_pydict() == {"groups": [1, 2, None], "list": expected_groups}


def test_grouped_pyobj_list_aggs() -> None:
    groups = [None, 1, None, 1, 2, 2]
    input = [None, object(), object(), object(), None, object()]
    expected_idx = [[1, 3], [4, 5], [0, 2]]

    daft_recordbatch = MicroPartition.from_pydict({"groups": groups, "input": input})
    result = daft_recordbatch.agg([col("input").alias("list").agg_list()], group_by=[col("groups")]).sort(
        [col("groups")]
    )
    expected_groups = [[input[i] for i in group] for group in expected_idx]
    assert result.to_pydict() == {"groups": [1, 2, None], "list": expected_groups}


def test_grouped_list_list_aggs() -> None:
    groups = [None, 1, None, 1, 2, 2]
    input = [[1], [2, 3, 4], [5, None], None, [], [8, 9]]
    expected_idx = [[1, 3], [4, 5], [0, 2]]

    daft_recordbatch = MicroPartition.from_pydict({"groups": groups, "input": input})
    daft_recordbatch = daft_recordbatch.eval_expression_list([col("groups"), col("input")])
    result = daft_recordbatch.agg([col("input").alias("list").agg_list()], group_by=[col("groups")]).sort(
        [col("groups")]
    )
    assert result.get_column_by_name("list").datatype() == DataType.list(DataType.list(DataType.int64()))

    input_as_dtype = daft_recordbatch.get_column_by_name("input").to_pylist()
    expected_groups = [[input_as_dtype[i] for i in group] for group in expected_idx]

    assert result.to_pydict() == {"groups": [1, 2, None], "list": expected_groups}


def test_grouped_fixed_size_list_list_aggs() -> None:
    groups = [None, 1, None, 1, 2, 2]
    input = Series.from_pylist([[1, 2], [3, 4], [5, None], None, [6, 7], [8, 9]]).cast(
        DataType.fixed_size_list(DataType.int64(), 2)
    )
    expected_idx = [[1, 3], [4, 5], [0, 2]]

    daft_recordbatch = MicroPartition.from_pydict({"groups": groups, "input": input})
    daft_recordbatch = daft_recordbatch.eval_expression_list([col("groups"), col("input")])
    result = daft_recordbatch.agg([col("input").alias("list").agg_list()], group_by=[col("groups")]).sort(
        [col("groups")]
    )
    assert result.get_column_by_name("list").datatype() == DataType.list(DataType.fixed_size_list(DataType.int64(), 2))

    input_as_dtype = daft_recordbatch.get_column_by_name("input").to_pylist()
    expected_groups = [[input_as_dtype[i] for i in group] for group in expected_idx]

    assert result.to_pydict() == {"groups": [1, 2, None], "list": expected_groups}


def test_grouped_struct_list_aggs() -> None:
    groups = [None, 1, None, 1, 2, 2]
    input = [{"x": 1, "y": 2}, {"x": 3, "y": 4}, {"x": 5, "y": None}, None, {"x": 6, "y": 7}, {"x": 8, "y": 9}]
    expected_idx = [[1, 3], [4, 5], [0, 2]]

    daft_recordbatch = MicroPartition.from_pydict({"groups": groups, "input": input})
    daft_recordbatch = daft_recordbatch.eval_expression_list([col("groups"), col("input")])
    result = daft_recordbatch.agg([col("input").alias("list").agg_list()], group_by=[col("groups")]).sort(
        [col("groups")]
    )
    assert result.get_column_by_name("list").datatype() == DataType.list(
        DataType.struct({"x": DataType.int64(), "y": DataType.int64()})
    )

    input_as_dtype = daft_recordbatch.get_column_by_name("input").to_pylist()
    expected_groups = [[input_as_dtype[i] for i in group] for group in expected_idx]

    assert result.to_pydict() == {"groups": [1, 2, None], "list": expected_groups}


def test_list_aggs_empty() -> None:
    daft_recordbatch = MicroPartition.from_pydict({"col_A": [], "col_B": []})
    daft_recordbatch = daft_recordbatch.agg(
        [col("col_A").cast(DataType.int32()).alias("list").agg_list()],
        group_by=[col("col_B")],
    )
    assert daft_recordbatch.get_column_by_name("list").datatype() == DataType.list(DataType.int32())
    res = daft_recordbatch.to_pydict()

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

    daft_recordbatch = MicroPartition.from_pydict({"input": input}).eval_expression_list(
        [col("input").cast(DataType.list(dtype))]
    )
    concated = daft_recordbatch.agg([col("input").alias("concat").agg_concat()])
    assert concated.get_column_by_name("concat").datatype() == DataType.list(dtype)

    input_as_dtype = daft_recordbatch.get_column_by_name("input").to_pylist()
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

    table = MicroPartition.from_pydict({"input": input})
    concatted = table.agg([col("input").alias("concat").agg_concat()])
    assert concatted.get_column_by_name("concat").datatype() == DataType.python()
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
    daft_recordbatch = MicroPartition.from_pydict({"groups": groups, "input": input}).eval_expression_list(
        [col("groups"), col("input").cast(DataType.list(dtype))]
    )
    concat_grouped = daft_recordbatch.agg(
        [col("input").alias("concat").agg_concat()], group_by=[col("groups") % 2]
    ).sort([col("groups")])
    assert concat_grouped.get_column_by_name("concat").datatype() == DataType.list(dtype)

    input_as_dtype = daft_recordbatch.get_column_by_name("input").to_pylist()
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

    table = MicroPartition.from_pydict({"input": input, "groups": [1, 2, 3, 3, 4]})
    concatted = table.agg([col("input").alias("concat").agg_concat()], group_by=[col("groups")]).sort([col("groups")])
    assert concatted.get_column_by_name("concat").datatype() == DataType.python()
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
    daft_recordbatch = MicroPartition.from_pydict({"col_A": [], "col_B": []})
    daft_recordbatch = daft_recordbatch.agg(
        [col("col_A").cast(DataType.list(DataType.int32())).alias("concat").agg_concat()],
        group_by=[col("col_B")],
    )

    assert daft_recordbatch.get_column_by_name("concat").datatype() == DataType.list(DataType.int32())
    res = daft_recordbatch.to_pydict()

    assert res == {"col_B": [], "concat": []}


@pytest.mark.parametrize("dtype", daft_numeric_types)
def test_groupby_list(dtype) -> None:
    df = from_pydict(
        {
            "a": [[1, 2, 3], [1, 2, 3], [1, 2], [], [1, 2, 3], [], [1, 2]],
            "b": [0, 1, 2, 3, 4, 5, 6],
        }
    ).with_column("a", col("a").cast(DataType.list(dtype)))
    res = df.groupby("a").agg_list("b").to_pydict()
    expected = [[0, 1, 4], [2, 6], [3, 5]]
    for lt in expected:
        assert lt in res["b"]


@pytest.mark.parametrize("dtype", daft_numeric_types)
def test_groupby_fixed_size_list(dtype) -> None:
    df = from_pydict(
        {
            "a": [
                [1, 2, 3],
                [1, 2, 3],
                [1, 2, 4],
                [3, 2, 1],
                [1, 2, 3],
                [3, 2, 1],
                [1, 2, 4],
            ],
            "b": [0, 1, 2, 3, 4, 5, 6],
        }
    ).with_column("a", col("a").cast(DataType.fixed_size_list(dtype, 3)))
    res = df.groupby("a").agg_list("b").to_pydict()
    expected = [[0, 1, 4], [2, 6], [3, 5]]
    for lt in expected:
        assert lt in res["b"]


@pytest.mark.parametrize("dtype", daft_numeric_types)
def test_groupby_struct(dtype) -> None:
    df = from_pydict(
        {
            "a": [
                {"c": 1, "d": "hi"},
                {"c": 1, "d": "hi"},
                {"c": 1, "d": "hello"},
                {"c": 2, "d": "hello"},
                {"c": 1, "d": "hi"},
                {"c": 2, "d": "hello"},
                {"c": 1, "d": "hello"},
            ],
            "b": [0, 1, 2, 3, 4, 5, 6],
        }
    ).with_column("a", col("a").cast(DataType.struct({"c": dtype, "d": DataType.string()})))
    res = df.groupby("a").agg_list("b").to_pydict()
    expected = [[0, 1, 4], [2, 6], [3, 5]]
    for lt in expected:
        assert lt in res["b"]


def test_agg_concat_on_string() -> None:
    df3 = from_pydict({"a": ["the", " quick", " brown", " fox"]})
    res = df3.agg(col("a").agg_concat()).to_pydict()
    assert res["a"] == ["the quick brown fox"]


def test_agg_concat_on_string_groupby() -> None:
    df3 = from_pydict({"a": ["the", " quick", " brown", " fox"], "b": [1, 2, 1, 2]})
    res = df3.groupby("b").agg_concat("a").to_pydict()
    expected = ["the brown", " quick fox"]
    for txt in expected:
        assert txt in res["a"]


def test_agg_concat_on_string_null() -> None:
    df3 = from_pydict({"a": ["the", " quick", None, " fox"]})
    res = df3.agg(col("a").agg_concat()).to_pydict()
    expected = ["the quick fox"]
    assert res["a"] == expected


def test_agg_concat_on_string_groupby_null() -> None:
    df3 = from_pydict({"a": ["the", " quick", None, " fox"], "b": [1, 2, 1, 2]})
    res = df3.groupby("b").agg_concat("a").to_pydict()
    expected = ["the", " quick fox"]
    for txt in expected:
        assert txt in res["a"]


def test_agg_concat_on_string_null_list() -> None:
    df3 = from_pydict({"a": [None, None, None, None], "b": [1, 2, 1, 2]}).with_column(
        "a", col("a").cast(DataType.string())
    )
    res = df3.agg(col("a").agg_concat()).to_pydict()
    expected = [None]
    assert res["a"] == expected
    assert len(res["a"]) == 1


def test_agg_concat_on_string_groupby_null_list() -> None:
    df3 = from_pydict({"a": [None, None, None, None], "b": [1, 2, 1, 2]}).with_column(
        "a", col("a").cast(DataType.string())
    )
    res = df3.groupby("b").agg_concat("a").to_pydict()
    expected = [None, None]
    assert res["a"] == expected
    assert len(res["a"]) == len(expected)


@pytest.mark.parametrize(
    "dtype", daft_nonnull_types + daft_null_types, ids=[f"{_}" for _ in daft_nonnull_types + daft_null_types]
)
def test_global_set_aggs(dtype) -> None:
    input = [None, 0, 1, 2, None, 4, 2, 1, None]
    if dtype == DataType.date():
        input = [datetime.date(2020 + x, 1 + x, 1 + x) if x is not None else None for x in input]
    elif dtype == DataType.bool():
        input = [bool(x) if x is not None else None for x in input]
    elif dtype == DataType.string():
        input = [str(x) if x is not None else None for x in input]
    elif dtype == DataType.binary():
        input = [bytes(x) if x is not None else None for x in input]
    elif dtype == DataType.null():
        input = [None for _ in input]
    daft_table = MicroPartition.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").cast(dtype)])

    # Test without nulls
    result = daft_table.eval_expression_list([col("input").alias("set").agg_set()])
    assert result.get_column_by_name("set").datatype() == DataType.list(dtype)
    expected = [x for x in set(input) if x is not None]
    result_set = result.to_pydict()["set"][0]
    _assert_all_hashable(result_set, "test_global_set_aggs")
    # Check length
    assert len(result_set) == len(expected)
    # Convert both to sets to ignore order
    assert set(result_set) == set(expected)


def test_global_pyobj_set_aggs() -> None:
    pytest.skip(reason="Skipping because Python objects are not supported for set aggregation")
    obj1, obj2, obj3 = object(), object(), object()
    input = [obj1, obj2, None, obj3, obj1, None, obj2]
    table = MicroPartition.from_pydict({"input": input})

    # Should panic because Python objects are not implemented
    with pytest.raises(Exception, match="Python not implemented"):
        table.eval_expression_list([col("input").alias("set").agg_set()])


@pytest.mark.parametrize(
    "dtype", daft_nonnull_types + daft_null_types, ids=[f"{_}" for _ in daft_nonnull_types + daft_null_types]
)
def test_grouped_set_aggs(dtype) -> None:
    groups = [1, 2, 3, 1, 2, 3, 1, 2, 3, 1]
    input = [None, 0, 1, 2, 0, 2, None, 1, None, 3]

    if dtype == DataType.date():
        input = [datetime.date(2020 + x, 1 + x, 1 + x) if x is not None else None for x in input]
    elif dtype == DataType.bool():
        input = [bool(x) if x is not None else None for x in input]
    elif dtype == DataType.string():
        input = [str(x) if x is not None else None for x in input]
    elif dtype == DataType.binary():
        input = [bytes(x) if x is not None else None for x in input]
    elif dtype == DataType.null():
        input = [None for _ in input]

    daft_table = MicroPartition.from_pydict({"groups": groups, "input": input})
    daft_table = daft_table.eval_expression_list([col("groups"), col("input").cast(dtype)])
    input_as_dtype = daft_table.get_column_by_name("input").to_pylist()

    result = daft_table.agg([col("input").alias("set").agg_set()], group_by=[col("groups")]).sort([col("groups")])
    assert result.get_column_by_name("set").datatype() == DataType.list(dtype)

    result_dict = result.to_pydict()
    assert sorted(result_dict["groups"]) == [1, 2, 3]

    for i, group_set in enumerate(result_dict["set"]):
        _assert_all_hashable(group_set, f"test_grouped_set_aggs (group {result_dict['groups'][i]})")

    group1_set = set(result_dict["set"][0])
    group2_set = set(result_dict["set"][1])
    group3_set = set(result_dict["set"][2])

    group1_expected = {input_as_dtype[i] for i in [3, 9]}
    group2_expected = {input_as_dtype[i] for i in [1, 4, 7]}
    group3_expected = {input_as_dtype[i] for i in [2, 5]}

    if dtype == DataType.null():
        group1_expected = set()
        group2_expected = set()
        group3_expected = set()

    assert group1_set == group1_expected, f"Group 1 set incorrect. Expected {group1_expected}, got {group1_set}"
    assert group2_set == group2_expected, f"Group 2 set incorrect. Expected {group2_expected}, got {group2_set}"
    assert group3_set == group3_expected, f"Group 3 set incorrect. Expected {group3_expected}, got {group3_set}"
    assert None not in group1_set and None not in group2_set and None not in group3_set


def test_grouped_pyobj_set_aggs() -> None:
    pytest.skip(reason="Skipping because Python objects are not supported for set aggregation")
    obj1, obj2, obj3 = object(), object(), object()
    groups = [1, 2, 1, 2, 1, 2]
    input = [obj1, obj2, None, obj3, obj1, None]

    table = MicroPartition.from_pydict({"groups": groups, "input": input})

    # Should error because Python objects are not hashable
    with pytest.raises(ValueError, match="Cannot perform set aggregation on elements that are not hashable"):
        table.agg([col("input").alias("set").agg_set()], group_by=[col("groups")])


def test_grouped_list_set_aggs() -> None:
    pytest.skip(reason="Skipping because list set aggregation is not yet implemented")
    groups = [None, 1, None, 1, 2, 2, 1]
    input = [[1], [2, 3, 4], [5, None], None, [], [8, 9], [2, 3, 4]]  # Added duplicate list
    expected_idx = [[1, 3, 6], [4, 5], [0, 2]]

    daft_table = MicroPartition.from_pydict({"groups": groups, "input": input})
    daft_table = daft_table.eval_expression_list([col("groups"), col("input")])

    # Test without nulls
    result = daft_table.agg([col("input").alias("set").agg_set()], group_by=[col("groups")]).sort([col("groups")])
    assert result.get_column_by_name("set").datatype() == DataType.list(DataType.list(DataType.int64()))
    input_as_dtype = daft_table.get_column_by_name("input").to_pylist()

    # Convert lists to tuples for hashing
    def to_hashable(lst):
        return tuple(lst) if lst is not None else None

    expected_groups_no_nulls = []
    for group in expected_idx:
        group_values = [input_as_dtype[i] for i in group if input_as_dtype[i] is not None]
        # First collect unique values
        unique_values = set(to_hashable(x) for x in group_values)
        # Sort non-None values only
        sorted_values = sorted(v for v in unique_values if v is not None)
        # Convert back to lists for comparison
        expected_groups_no_nulls.append([list(x) for x in sorted_values])

    assert result.to_pydict() == {"groups": [1, 2, None], "set": expected_groups_no_nulls}


def test_grouped_struct_set_aggs() -> None:
    pytest.skip(reason="Skipping because struct set aggregation is not yet implemented")
    groups = [None, 1, None, 1, 2, 2, 1]
    input = [
        {"x": 1, "y": 2},
        {"x": 3, "y": 4},
        {"x": 5, "y": None},
        None,
        {"x": 6, "y": 7},
        {"x": 8, "y": 9},
        {"x": 3, "y": 4},  # Added duplicate struct
    ]
    expected_idx = [[1, 3, 6], [4, 5], [0, 2]]

    daft_table = MicroPartition.from_pydict({"groups": groups, "input": input})
    daft_table = daft_table.eval_expression_list([col("groups"), col("input")])

    # Test without nulls
    result = daft_table.agg([col("input").alias("set").agg_set()], group_by=[col("groups")]).sort([col("groups")])
    assert result.get_column_by_name("set").datatype() == DataType.list(
        DataType.struct({"x": DataType.int64(), "y": DataType.int64()})
    )
    input_as_dtype = daft_table.get_column_by_name("input").to_pylist()

    # Convert dicts to tuples for hashing
    def to_hashable(d):
        return tuple(sorted(d.items())) if d is not None else None

    expected_groups_no_nulls = []
    for group in expected_idx:
        group_values = [input_as_dtype[i] for i in group if input_as_dtype[i] is not None]
        # First collect unique values
        unique_values = set(to_hashable(x) for x in group_values)
        # Sort non-None values only
        sorted_values = sorted(v for v in unique_values if v is not None)
        # Convert back to dicts for comparison
        expected_groups_no_nulls.append([dict(x) for x in sorted_values])

    assert result.to_pydict() == {"groups": [1, 2, None], "set": expected_groups_no_nulls}


def test_set_aggs_empty() -> None:
    daft_table = MicroPartition.from_pydict({"col_A": [], "col_B": []})

    # Test without nulls
    result = daft_table.agg(
        [col("col_A").cast(DataType.int32()).alias("set").agg_set()],
        group_by=[col("col_B")],
    )
    assert result.get_column_by_name("set").datatype() == DataType.list(DataType.int32())
    assert result.to_pydict() == {"col_B": [], "set": []}


test_table_bool_agg_cases = [
    ([], {"bool_and": [None], "bool_or": [None]}),
    ([None], {"bool_and": [None], "bool_or": [None]}),
    ([None, None, None], {"bool_and": [None], "bool_or": [None]}),
    ([True], {"bool_and": [True], "bool_or": [True]}),
    ([False], {"bool_and": [False], "bool_or": [False]}),
    ([True, True], {"bool_and": [True], "bool_or": [True]}),
    ([False, False], {"bool_and": [False], "bool_or": [False]}),
    ([True, False], {"bool_and": [False], "bool_or": [True]}),
    ([None, True], {"bool_and": [True], "bool_or": [True]}),
    ([None, False], {"bool_and": [False], "bool_or": [False]}),
    ([True, None, True], {"bool_and": [True], "bool_or": [True]}),
    ([False, None, False], {"bool_and": [False], "bool_or": [False]}),
    ([True, None, False], {"bool_and": [False], "bool_or": [True]}),
]


@pytest.mark.parametrize("case", test_table_bool_agg_cases, ids=[f"{_}" for _ in test_table_bool_agg_cases])
def test_table_bool_agg(case) -> None:
    input, expected = case
    daft_table = MicroPartition.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").cast(DataType.bool())])
    daft_table = daft_table.eval_expression_list(
        [
            col("input").alias("bool_and").bool_and(),
            col("input").alias("bool_or").bool_or(),
        ]
    )

    res = daft_table.to_pydict()
    assert res == expected
