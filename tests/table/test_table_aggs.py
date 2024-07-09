from __future__ import annotations

import datetime
import math

import numpy as np
import pyarrow as pa
import pytest

from daft import DataType, col, from_pydict, utils
from daft.logical.schema import Schema
from daft.series import Series
from daft.table import MicroPartition
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
    daft_table = MicroPartition.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").cast(idx_dtype)])
    daft_table = daft_table.eval_expression_list([col("input").alias("count").count()])

    res = daft_table.to_pydict()
    assert res == expected


@pytest.mark.parametrize("length", [0, 1, 10])
def test_table_count_nulltype(length) -> None:
    daft_table = MicroPartition.from_pydict({"input": [None] * length})
    daft_table = daft_table.eval_expression_list([col("input").cast(DataType.null())])
    daft_table = daft_table.eval_expression_list([col("input").alias("count").count()])

    res = daft_table.to_pydict()["count"]
    assert res == [0]


def test_table_count_pyobject() -> None:
    daft_table = MicroPartition.from_pydict({"objs": [object(), object(), None, object(), None]})
    daft_table = daft_table.eval_expression_list([col("objs").alias("count").count()])

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
    daft_table = MicroPartition.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").cast(idx_dtype)])
    daft_table = daft_table.eval_expression_list(
        [
            col("input").alias("min").min(),
            col("input").alias("max").max(),
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
    daft_table = MicroPartition.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").cast(idx_dtype)])
    daft_table = daft_table.eval_expression_list(
        [
            col("input").alias("min").min(),
            col("input").alias("max").max(),
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
    daft_table = MicroPartition.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").cast(DataType.bool())])
    daft_table = daft_table.eval_expression_list(
        [
            col("input").alias("min").min(),
            col("input").alias("max").max(),
        ]
    )

    res = daft_table.to_pydict()
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
    daft_table = MicroPartition.from_arrow(pa.table({"input": pa.array(input, type=type)}))
    daft_table = daft_table.eval_expression_list(
        [
            col("input").alias("min").min(),
            col("input").alias("max").max(),
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
    daft_table = MicroPartition.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").cast(idx_dtype)])
    daft_table = daft_table.eval_expression_list(
        [
            col("input").alias("sum").sum(),
            col("input").alias("mean").mean(),
        ]
    )

    res = daft_table.to_pydict()
    assert res == expected


@pytest.mark.parametrize("nptype", [np.uint8, np.uint16, np.uint32, np.int8, np.int16, np.int32])
def test_table_sum_upcast(nptype) -> None:
    """Tests correctness, including type upcasting, of sum aggregations."""
    daft_table = MicroPartition.from_pydict(
        {
            "maxes": np.full(128, fill_value=np.iinfo(nptype).max, dtype=nptype),
            "mins": np.full(128, fill_value=np.iinfo(nptype).min, dtype=nptype),
        }
    )
    daft_table = daft_table.eval_expression_list([col("maxes").sum(), col("mins").sum()])
    pydict = daft_table.to_pydict()
    assert pydict["maxes"] == [128 * np.iinfo(nptype).max]
    assert pydict["mins"] == [128 * np.iinfo(nptype).min]


def test_table_sum_badtype() -> None:
    daft_table = MicroPartition.from_pydict({"a": ["str1", "str2"]})
    with pytest.raises(ValueError):
        daft_table = daft_table.eval_expression_list([col("a").sum()])


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
    daft_table = MicroPartition.from_pydict(case)

    any_values = daft_table.agg([col("a").any_value(False)], group_by=[col("b")]).to_pydict()
    assert len(any_values["b"]) == len(expected_nulls)
    for k, v in zip(any_values["b"], any_values["a"]):
        assert expected_nulls[k] or v is not None

    any_values = daft_table.agg([col("a").any_value(True)], group_by=[col("b")]).to_pydict()
    assert len(any_values["b"]) == len(expected_no_nulls)
    for k, v in zip(any_values["b"], any_values["a"]):
        assert expected_no_nulls[k] or v is not None


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
    daft_table = MicroPartition.from_pydict({"input": input})
    daft_table = daft_table.agg(
        [
            col("input").cast(DataType.int32()).alias("count").count(),
            col("input").cast(DataType.int32()).alias("sum").sum(),
            col("input").cast(DataType.int32()).alias("mean").mean(),
            col("input").cast(DataType.int32()).alias("min").min(),
            col("input").cast(DataType.int32()).alias("max").max(),
            col("input").cast(DataType.int32()).alias("list").agg_list(),
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
    daft_table = MicroPartition.from_pydict({"col_A": [], "col_B": []})
    daft_table = daft_table.agg(
        [col(a).count() for a in aggs],
        [col(g).cast(DataType.int32()) for g in groups],
    )
    res = daft_table.to_pydict()

    assert res == {"col_A": [], "col_B": []}


test_table_agg_groupby_cases = [
    {
        # Group by strings.
        "groups": ["name"],
        "aggs": [
            col("cookies").alias("sum").sum(),
            col("name").alias("count").count(),
            col("cookies").alias("list").agg_list(),
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
    daft_table = MicroPartition.from_pydict(
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
    daft_table = MicroPartition.from_pydict(
        {
            "group": Series.from_pylist([None, None, None]).cast(dtype),
            "cookies": [1, 2, 3],
        }
    )
    result_table = daft_table.agg([col("cookies").sum()], group_by=[col("group")])
    assert result_table.to_pydict() == {"group": [None], "cookies": [6]}


@pytest.mark.parametrize(
    "dtype",
    daft_numeric_types + daft_string_types + [DataType.bool()],
    ids=[f"{_}" for _ in daft_numeric_types + daft_string_types + [DataType.bool()]],
)
def test_groupby_numeric_string_bool_some_nulls(dtype) -> None:
    daft_table = MicroPartition.from_pydict(
        {
            "group": Series.from_pylist([1, 1, None]).cast(dtype),
            "cookies": [2, 2, 3],
        }
    )
    result_table = daft_table.agg([col("cookies").sum()], group_by=[col("group")])
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
    daft_table = MicroPartition.from_pydict(
        {
            "group": Series.from_pylist([1, 0, 1, 0]).cast(dtype),
            "cookies": [1, 2, 2, 3],
        }
    )
    result_table = daft_table.agg([col("cookies").sum()], group_by=[col("group")])
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
    daft_table = MicroPartition.from_pydict(
        {
            "group": Series.from_arrow(pa.array([b"1", b"1", None, None], type=type)),
            "cookies": Series.from_arrow(pa.array([b"1", b"2", b"3", b"4"], type=type)),
        }
    )
    result_table = daft_table.agg([agg], group_by=[col("group")])
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
    daft_table = MicroPartition.from_pydict(
        {
            "group": Series.from_arrow(pa.array([b"1", b"0", b"1", b"0"], type=type)),
            "cookies": Series.from_arrow(pa.array([b"1", b"2", b"3", b"4"], type=type)),
        }
    )
    result_table = daft_table.agg([agg], group_by=[col("group")])
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

    daft_table = MicroPartition.from_pydict(
        {
            "group": Series.from_pylist([None, 1.0, NAN, 5 * NAN, -1 * NAN, -NAN, 1.0, None, INF, -INF, INF]).cast(
                dtype
            ),
            "cookies": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        }
    )
    result_table = daft_table.agg([col("cookies").count()], group_by=[col("group")])
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
    daft_table = MicroPartition.from_pydict(
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
    result_table = daft_table.agg([col("value").sum()], group_by=[col("group").dt.truncate("1 hour")])
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
    daft_table = MicroPartition.from_pydict({"input": input})
    daft_table = daft_table.eval_expression_list([col("input").cast(dtype)])
    result = daft_table.eval_expression_list([col("input").alias("list").agg_list()])
    assert result.get_column("list").datatype() == DataType.list(dtype)
    assert result.to_pydict() == {"list": [daft_table.to_pydict()["input"]]}


def test_global_pyobj_list_aggs() -> None:
    input = [object(), object(), object()]
    table = MicroPartition.from_pydict({"input": input})
    result = table.eval_expression_list([col("input").alias("list").agg_list()])
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
    daft_table = MicroPartition.from_pydict({"groups": groups, "input": input})
    daft_table = daft_table.eval_expression_list([col("groups"), col("input").cast(dtype)])
    result = daft_table.agg([col("input").alias("list").agg_list()], group_by=[col("groups")]).sort([col("groups")])
    assert result.get_column("list").datatype() == DataType.list(dtype)

    input_as_dtype = daft_table.get_column("input").to_pylist()
    expected_groups = [[input_as_dtype[i] for i in group] for group in expected_idx]

    assert result.to_pydict() == {"groups": [1, 2, None], "list": expected_groups}


def test_grouped_pyobj_list_aggs() -> None:
    groups = [None, 1, None, 1, 2, 2]
    input = [None, object(), object(), object(), None, object()]
    expected_idx = [[1, 3], [4, 5], [0, 2]]

    daft_table = MicroPartition.from_pydict({"groups": groups, "input": input})
    result = daft_table.agg([col("input").alias("list").agg_list()], group_by=[col("groups")]).sort([col("groups")])
    expected_groups = [[input[i] for i in group] for group in expected_idx]
    assert result.to_pydict() == {"groups": [1, 2, None], "list": expected_groups}


def test_list_aggs_empty() -> None:
    daft_table = MicroPartition.from_pydict({"col_A": [], "col_B": []})
    daft_table = daft_table.agg(
        [col("col_A").cast(DataType.int32()).alias("list").agg_list()],
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

    daft_table = MicroPartition.from_pydict({"input": input}).eval_expression_list(
        [col("input").cast(DataType.list(dtype))]
    )
    concated = daft_table.agg([col("input").alias("concat").agg_concat()])
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

    table = MicroPartition.from_pydict({"input": input})
    concatted = table.agg([col("input").alias("concat").agg_concat()])
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
    daft_table = MicroPartition.from_pydict({"groups": groups, "input": input}).eval_expression_list(
        [col("groups"), col("input").cast(DataType.list(dtype))]
    )
    concat_grouped = daft_table.agg([col("input").alias("concat").agg_concat()], group_by=[col("groups") % 2]).sort(
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

    table = MicroPartition.from_pydict({"input": input, "groups": [1, 2, 3, 3, 4]})
    concatted = table.agg([col("input").alias("concat").agg_concat()], group_by=[col("groups")]).sort([col("groups")])
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
    daft_table = MicroPartition.from_pydict({"col_A": [], "col_B": []})
    daft_table = daft_table.agg(
        [col("col_A").cast(DataType.list(DataType.int32())).alias("concat").agg_concat()],
        group_by=[col("col_B")],
    )

    assert daft_table.get_column("concat").datatype() == DataType.list(DataType.int32())
    res = daft_table.to_pydict()

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
