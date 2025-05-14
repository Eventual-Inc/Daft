from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import pyarrow as pa
import pytest

import daft
from daft import col
from daft.context import get_context
from daft.datatype import DataType
from daft.errors import ExpressionTypeError
from daft.utils import freeze
from tests.utils import sort_arrow_table


def _assert_all_hashable(values, test_name=""):
    """Helper function to check if all elements in an iterable are hashable.

    Args:
        values: Iterable of values to check
        test_name: Name of the test for better error messages

    Raises:
        AssertionError: If any elements are not hashable, with a descriptive message showing all unhashable values
    """
    unhashable = []
    for val in values:
        if val is not None:  # Skip None values as they are always hashable
            try:
                hash(val)
            except TypeError:
                unhashable.append((type(val), val))

    if unhashable:
        details = "\n".join(f"  - {t.__name__}: {v}" for t, v in unhashable)
        raise AssertionError(
            f"{test_name}: Found {len(unhashable)} unhashable value(s) in values: {values}\n"
            f"Unhashable values:\n{details}\n"
            "Set aggregation requires all elements to be hashable."
        )


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_agg_global(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id": [1, 2, 3],
            "values": [1, None, 2],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.agg(
        [
            col("values").sum().alias("sum"),
            col("values").mean().alias("mean"),
            col("values").min().alias("min"),
            col("values").max().alias("max"),
            col("values").count().alias("count"),
            col("values").agg_list().alias("list"),
            col("values").agg_set().alias("set"),
        ]
    )
    expected = {
        "sum": [3],
        "mean": [1.5],
        "min": [1],
        "max": [2],
        "count": [2],
        "list": [[1, None, 2]],
        "set": [[1, 2]],
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    res_list = daft_cols.pop("list")
    exp_list = expected.pop("list")
    res_set = daft_cols.pop("set")
    exp_set = expected.pop("set")

    assert pa.Table.from_pydict(daft_cols) == pa.Table.from_pydict(expected)

    # Check list agg
    assert len(res_list) == 1
    assert set(res_list[0]) == set(exp_list[0])

    # Check set agg without nulls
    assert len(res_set) == 1
    _assert_all_hashable(res_set[0], "test_agg_global")
    assert len(res_set[0]) == len(set(x for x in res_set[0] if x is not None)), "Result should contain no duplicates"
    assert set(x for x in res_set[0] if x is not None) == set(
        x for x in exp_set[0] if x is not None
    ), "Sets should contain same non-null elements"


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_agg_global_all_null(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id": [0, 1, 2, 3],
            "values": [1, None, None, None],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.where(col("id") != 0).agg(
        [
            col("values").sum().alias("sum"),
            col("values").mean().alias("mean"),
            col("values").min().alias("min"),
            col("values").max().alias("max"),
            col("values").count().alias("count"),
            col("values").agg_list().alias("list"),
            col("values").agg_set().alias("set"),
        ]
    )
    expected = {
        "sum": [None],
        "mean": [None],
        "min": [None],
        "max": [None],
        "count": [0],
        "list": [[None, None, None]],
        "set": [[]],  # Empty list since no non-null values
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    # Handle list and set results separately
    res_list = daft_cols.pop("list")
    exp_list = expected.pop("list")
    res_set = daft_cols.pop("set")
    exp_set = expected.pop("set")

    # Check regular aggregations
    assert pa.Table.from_pydict(daft_cols) == pa.Table.from_pydict(expected)

    # Check list result
    assert len(res_list) == 1
    assert res_list[0] == exp_list[0]

    # Check set without nulls
    assert len(res_set) == 1
    assert res_set[0] == exp_set[0], "Should be empty list when no non-null values exist"


def test_agg_global_empty(make_df):
    daft_df = make_df(
        {
            "id": [0],
            "values": [1],
        },
        repartition=2,
    )
    daft_df = daft_df.where(col("id") != 0).agg(
        [
            col("values").sum().alias("sum"),
            col("values").mean().alias("mean"),
            col("values").min().alias("min"),
            col("values").max().alias("max"),
            col("values").count().alias("count"),
            col("values").agg_list().alias("list"),
            col("values").agg_set().alias("set"),
        ]
    )
    expected = {
        "sum": [None],
        "mean": [None],
        "min": [None],
        "max": [None],
        "count": [0],
        "list": [[]],
        "set": [[]],  # Empty list since DataFrame is empty
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    # Handle list and set results separately
    res_list = daft_cols.pop("list")
    exp_list = expected.pop("list")
    res_set = daft_cols.pop("set")
    exp_set = expected.pop("set")

    # Check regular aggregations
    assert pa.Table.from_pydict(daft_cols) == pa.Table.from_pydict(expected)

    # Check list result
    assert len(res_list) == 1
    assert res_list[0] == exp_list[0], "List should be empty for empty DataFrame"

    # Check set without nulls
    assert len(res_set) == 1
    assert res_set[0] == exp_set[0], "Set should be empty for empty DataFrame"


@pytest.mark.parametrize("repartition_nparts", [1, 2, 7])
def test_agg_groupby(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "group": [1, 1, 1, 2, 2, 2],
            "values": [1, None, 2, 2, None, 4],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.groupby("group").agg(
        [
            col("values").sum().alias("sum"),
            col("values").mean().alias("mean"),
            col("values").min().alias("min"),
            col("values").max().alias("max"),
            col("values").count().alias("count"),
            col("values").agg_list().alias("list"),
            col("values").agg_set().alias("set"),
        ]
    )
    expected = {
        "group": [1, 2],
        "sum": [3, 6],
        "mean": [1.5, 3],
        "min": [1, 2],
        "max": [2, 4],
        "count": [2, 2],
        "list": [[1, None, 2], [2, None, 4]],
        "set": [[1, 2], [2, 4]],  # Only non-null values, preserving order
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    # Handle list and set results separately
    res_list = daft_cols.pop("list")
    exp_list = expected.pop("list")
    res_set = daft_cols.pop("set")
    exp_set = expected.pop("set")

    # Check regular aggregations
    assert sort_arrow_table(pa.Table.from_pydict(daft_cols), "group") == sort_arrow_table(
        pa.Table.from_pydict(expected), "group"
    )

    # Sort results by group for comparison
    arg_sort = np.argsort(daft_cols["group"])

    # Check list results
    assert freeze([list(map(set, res_list))[i] for i in arg_sort]) == freeze(list(map(set, exp_list)))

    # Check set without nulls
    sorted_res = [res_set[i] for i in arg_sort]
    sorted_exp = exp_set
    for res, exp in zip(sorted_res, sorted_exp):
        _assert_all_hashable(res, "test_agg_groupby")
        assert len(res) == len(set(x for x in res if x is not None)), "Result should contain no duplicates"
        assert set(x for x in res if x is not None) == set(x for x in exp if x is not None), "Sets should match"


@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
def test_agg_groupby_all_null(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id": [0, 1, 2, 3, 4],
            "group": [0, 1, 1, 2, 2],
            "values": [1, None, None, None, None],
        },
        repartition=repartition_nparts,
    )
    # Remove the first row so that all values are Null
    daft_df = daft_df.where(col("id") != 0)
    daft_df = daft_df.groupby(col("group")).agg(
        [
            col("values").sum().alias("sum"),
            col("values").mean().alias("mean"),
            col("values").min().alias("min"),
            col("values").max().alias("max"),
            col("values").count().alias("count"),
            col("values").agg_list().alias("list"),
            col("values").agg_set().alias("set"),
        ]
    )

    expected = {
        "group": [1, 2],
        "sum": [None, None],
        "mean": [None, None],
        "min": [None, None],
        "max": [None, None],
        "count": [0, 0],
        "list": [[None, None], [None, None]],
        "set": [[], []],  # Empty lists since no non-null values in any group
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    # Handle list and set results separately
    res_list = daft_cols.pop("list")
    exp_list = expected.pop("list")
    res_set = daft_cols.pop("set")
    exp_set = expected.pop("set")

    # Check regular aggregations
    assert sort_arrow_table(pa.Table.from_pydict(daft_cols), "group") == sort_arrow_table(
        pa.Table.from_pydict(expected), "group"
    )

    # Sort results by group for comparison
    arg_sort = np.argsort(daft_cols["group"])

    # Check list results
    sorted_res_list = [res_list[i] for i in arg_sort]
    assert sorted_res_list == exp_list

    # Check set without nulls
    sorted_res = [res_set[i] for i in arg_sort]
    for res, exp in zip(sorted_res, exp_set):
        assert res == exp, "Should be empty list when no non-null values exist"
        assert len(res) == 0, "Should be empty list when no non-null values exist"


def test_agg_groupby_null_type_column(make_df):
    daft_df = make_df(
        {
            "id": [1, 2, 3, 4],
            "group": [1, 1, 2, 2],
            "values": [None, None, None, None],
        }
    )
    daft_df = daft_df.groupby(col("group"))

    with pytest.raises(ValueError):
        daft_df.agg(col("values").sum().alias("sum"))


@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
def test_null_groupby_keys(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id": [0, 1, 2, 3, 4],
            "group": [0, 1, None, 2, None],
            "values": [0, 1, 3, 2, 3],
        },
        repartition=repartition_nparts,
    )

    daft_df = daft_df.groupby(col("group")).agg(col("values").mean().alias("mean"))

    expected = {
        "group": [0, 1, 2, None],
        "mean": [0.0, 1.0, 2.0, 3.0],
    }
    daft_cols = daft_df.to_pydict()
    assert sort_arrow_table(pa.Table.from_pydict(daft_cols), "group") == sort_arrow_table(
        pa.Table.from_pydict(expected), "group"
    )


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_all_null_groupby_keys(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id": [0, 1, 2],
            "group": [None, None, None],
            "values": [1, 2, 3],
        },
        repartition=repartition_nparts,
    )

    daft_df = (
        daft_df.with_column("group", daft_df["group"].cast(DataType.int64()))
        .groupby(col("group"))
        .agg(
            col("values").agg_list().alias("list"),
            col("values").mean().alias("mean"),
            col("values").agg_set().alias("set"),
        )
    )

    daft_cols = daft_df.to_pydict()

    # Check group and mean
    assert daft_cols["group"] == [None]
    assert daft_cols["mean"] == [2.0]

    # Check list result
    assert len(daft_cols["list"]) == 1
    assert set(daft_cols["list"][0]) == {1, 2, 3}

    # Check set without nulls (should be same as with nulls since no nulls in values)
    assert len(daft_cols["set"]) == 1
    _assert_all_hashable(daft_cols["set"][0], "test_all_null_groupby_keys")
    assert len(daft_cols["set"][0]) == 3, "Should contain all unique non-null values"
    assert set(daft_cols["set"][0]) == {1, 2, 3}, "Should contain all unique values"


def test_null_type_column_groupby_keys(make_df):
    daft_df = make_df(
        {
            "id": [0, 1, 2],
            "group": [None, None, None],
            "values": [1, 2, 3],
        }
    )

    with pytest.raises(ExpressionTypeError):
        daft_df.groupby(col("group"))


def test_agg_groupby_empty(make_df):
    daft_df = make_df(
        {
            "id": [0],
            "group": [0],
            "values": [1],
        },
        repartition=2,
    )
    # Remove the first row so that dataframe is empty
    daft_df = daft_df.where(col("id") != 0)
    daft_df = daft_df.groupby(col("group")).agg(
        [
            col("values").sum().alias("sum"),
            col("values").mean().alias("mean"),
            col("values").min().alias("min"),
            col("values").max().alias("max"),
            col("values").count().alias("count"),
            col("values").agg_list().alias("list"),
            col("values").agg_set().alias("set"),
        ]
    )

    expected = {
        "group": [],
        "sum": [],
        "mean": [],
        "min": [],
        "max": [],
        "count": [],
        "list": [],
        "set": [],  # Empty since DataFrame is empty
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    # All columns should be empty lists since DataFrame is empty
    for col_name, values in daft_cols.items():
        assert values == [], f"Column {col_name} should be empty list for empty DataFrame"

    assert sort_arrow_table(pa.Table.from_pydict(daft_cols), "group") == sort_arrow_table(
        pa.Table.from_pydict(expected), "group"
    )


@pytest.mark.parametrize("repartition_nparts", [1, 2, 7])
def test_agg_groupby_with_alias(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "group": [1, 1, 1, 2, 2, 2],
            "values": [1, None, 2, 2, None, 4],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.groupby(daft_df["group"].alias("group_alias")).agg(
        [
            col("values").sum().alias("sum"),
            col("values").mean().alias("mean"),
            col("values").min().alias("min"),
            col("values").max().alias("max"),
            col("values").count().alias("count"),
            col("values").agg_list().alias("list"),
            col("values").agg_set().alias("set"),
        ]
    )
    expected = {
        "group_alias": [1, 2],
        "sum": [3, 6],
        "mean": [1.5, 3],
        "min": [1, 2],
        "max": [2, 4],
        "count": [2, 2],
        "list": [[1, None, 2], [2, None, 4]],
        "set": [[1, 2], [2, 4]],  # Only non-null values, preserving order
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    # Handle list and set results separately
    res_list = daft_cols.pop("list")
    exp_list = expected.pop("list")
    res_set = daft_cols.pop("set")
    exp_set = expected.pop("set")

    # Check regular aggregations
    assert sort_arrow_table(pa.Table.from_pydict(daft_cols), "group_alias") == sort_arrow_table(
        pa.Table.from_pydict(expected), "group_alias"
    )

    # Sort results by group for comparison
    arg_sort = np.argsort(daft_cols["group_alias"])

    # Check list results
    assert freeze([list(map(set, res_list))[i] for i in arg_sort]) == freeze(list(map(set, exp_list)))

    # Check set without nulls
    sorted_res = [res_set[i] for i in arg_sort]
    sorted_exp = exp_set
    for res, exp in zip(sorted_res, sorted_exp):
        _assert_all_hashable(res, "test_agg_groupby_with_alias")
        assert len(res) == len(set(x for x in res if x is not None)), "Result should contain no duplicates"
        assert set(x for x in res if x is not None) == set(
            x for x in exp if x is not None
        ), "Sets should contain same non-null elements"
        assert None not in res, "Result should not contain nulls"


@dataclass
class CustomObject:
    val: int

    def __hash__(self):
        return hash(self.val)


def test_agg_pyobjects_list():
    objects = [CustomObject(val=0), None, CustomObject(val=1)]
    df = daft.from_pydict({"objs": objects})
    df = df.into_partitions(2)
    df = df.agg(
        [
            col("objs").count().alias("count"),
            col("objs").agg_list().alias("list"),
        ]
    )
    df.collect()
    result = df.to_pydict()

    assert result["count"] == [2]
    assert set(result["list"][0]) == set(objects)


def test_groupby_agg_pyobjects_list():
    objects = [CustomObject(val=0), CustomObject(val=1), None, None, CustomObject(val=2)]
    df = daft.from_pydict({"objects": objects, "groups": [1, 2, 1, 2, 1]})
    df = df.into_partitions(2)
    df = (
        df.groupby(col("groups"))
        .agg(
            [
                col("objects").count().alias("count"),
                col("objects").agg_list().alias("list"),
            ]
        )
        .sort(col("groups"))
    )

    df.collect()
    res = df.to_pydict()
    assert res["groups"] == [1, 2]
    assert res["count"] == [2, 1]
    assert set(res["list"][0]) == set([objects[0], objects[2], objects[4]])
    assert set(res["list"][1]) == set([objects[1], objects[3]])


@pytest.mark.parametrize("shuffle_aggregation_default_partitions", [None, 20])
def test_groupby_result_partitions_smaller_than_input(shuffle_aggregation_default_partitions, with_morsel_size):
    if shuffle_aggregation_default_partitions is None:
        min_partitions = get_context().daft_execution_config.shuffle_aggregation_default_partitions
    else:
        min_partitions = shuffle_aggregation_default_partitions

    with daft.execution_config_ctx(shuffle_aggregation_default_partitions=shuffle_aggregation_default_partitions):
        for partition_size in [1, min_partitions, min_partitions + 1]:
            df = daft.from_pydict(
                {"group": [i for i in range(min_partitions + 1)], "value": [i for i in range(min_partitions + 1)]}
            )
            df = df.into_partitions(partition_size)

            df = df.groupby(col("group")).agg(
                [
                    col("value").sum().alias("sum"),
                    col("value").mean().alias("mean"),
                    col("value").min().alias("min"),
                ]
            )

            df = df.collect()

            assert df.num_partitions() == min(min_partitions, partition_size)


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_agg_any_value(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "group": [1, 1, 1, 2, 2, 2],
            "values": [1, 5, 2, 3, 6, 4],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.groupby("group").agg(col("values").any_value().alias("any_value"))

    daft_df.collect()
    res = daft_df.to_pydict()
    vals = [[], [1, 5, 2], [3, 6, 4]]
    assert res["any_value"][0] in vals[res["group"][0]]
    assert res["any_value"][1] in vals[res["group"][1]]


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_agg_any_value_ignore_nulls(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "group": [1, 1, 1, 2, 2, 2, 3, 3, 3],
            "values": [None, None, 2, None, None, 4, None, None, None],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.groupby("group").agg(col("values").any_value(True).alias("any_value"))

    daft_df.collect()
    res = daft_df.to_pydict()
    mapping = {res["group"][i]: res["any_value"][i] for i in range(len(res["group"]))}
    assert mapping == {1: 2, 2: 4, 3: None}


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_agg_skew(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "group": [1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4],
            "values": [1, 3, 2, 4, None, 4, 1, 1, 2, None, None, None],
        },
        repartition=repartition_nparts,
    )
    daft_df = daft_df.groupby("group").agg(col("values").skew().alias("skew"))

    daft_df.collect()
    res = daft_df.to_pydict()
    mapping = {res["group"][i]: res["skew"][i] for i in range(len(res["group"]))}
    assert len(mapping) == 4
    assert mapping[1] == 0.0
    assert math.isnan(mapping[2])
    assert math.isclose(mapping[3], 0.70710678, rel_tol=1e-5)
    assert mapping[4] is None


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_agg_with_non_agg_expr_global(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id": [1, 2, 3],
            "values": [4, 5, 6],
        },
        repartition=repartition_nparts,
    )

    daft_df = daft_df.agg(
        col("id").sum(),
        col("values").mean().alias("values_mean"),
        (col("id").mean() + col("values").mean()).alias("sum_of_means"),
    )

    res = daft_df.to_pydict()
    assert res == {"id": [6], "values_mean": [5], "sum_of_means": [7]}


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_agg_with_non_agg_expr_groupby(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "group": [1, 1, 1, 2, 2, 2, 3, 3, 3],
            "id": [1, 2, 3, 2, 3, 4, 3, 4, 5],
            "values": [4, 5, 6, 5, 6, 7, 6, 7, 8],
        },
        repartition=repartition_nparts,
    )

    daft_df = (
        daft_df.groupby("group")
        .agg(
            col("id").sum(),
            col("values").mean().alias("values_mean"),
            (col("id").mean() + col("values").mean()).alias("sum_of_means"),
        )
        .sort("group")
    )

    res = daft_df.to_pydict()
    assert res == {"group": [1, 2, 3], "id": [6, 9, 12], "values_mean": [5, 6, 7], "sum_of_means": [7, 9, 11]}


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_agg_with_literal_global(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "id": [1, 2, 3],
            "values": [4, 5, 6],
        },
        repartition=repartition_nparts,
    )

    daft_df = daft_df.agg(
        col("id").sum(),
        col("values").mean().alias("values_mean"),
        (col("id").sum() + 1).alias("sum_plus_1"),
        (col("id") + 1).sum().alias("1_plus_sum"),
    )

    res = daft_df.to_pydict()
    assert res == {"id": [6], "values_mean": [5], "sum_plus_1": [7], "1_plus_sum": [9]}


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_agg_with_literal_groupby(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "group": [1, 1, 1, 2, 2, 2, 3, 3, 3],
            "id": [1, 2, 3, 2, 3, 4, 3, 4, 5],
            "values": [4, 5, 6, 5, 6, 7, 6, 7, 8],
        },
        repartition=repartition_nparts,
    )

    daft_df = (
        daft_df.groupby("group")
        .agg(
            col("id").sum(),
            col("values").mean().alias("values_mean"),
            (col("id").sum() + 1).alias("sum_plus_1"),
            (col("id") + 1).sum().alias("1_plus_sum"),
        )
        .sort("group")
    )

    res = daft_df.to_pydict()
    assert res == {
        "group": [1, 2, 3],
        "id": [6, 9, 12],
        "values_mean": [5, 6, 7],
        "sum_plus_1": [7, 10, 13],
        "1_plus_sum": [9, 12, 15],
    }


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_agg_with_groupby_key_in_agg(make_df, repartition_nparts, with_morsel_size):
    daft_df = make_df(
        {
            "group": [1, 1, 1, 2, 2, 2, 3, 3, 3],
            "id": [1, 2, 3, 2, 3, 4, 3, 4, 5],
            "values": [4, 5, 6, 5, 6, 7, 6, 7, 8],
        },
        repartition=repartition_nparts,
    )

    daft_df = (
        daft_df.groupby("group")
        .agg(
            col("group").alias("group_alias"),
            (col("group") + 1).alias("group_plus_1"),
            (col("id").sum() + col("group")).alias("id_plus_group"),
        )
        .sort("group")
    )

    res = daft_df.to_pydict()
    assert res == {
        "group": [1, 2, 3],
        "group_alias": [1, 2, 3],
        "group_plus_1": [2, 3, 4],
        "id_plus_group": [7, 11, 15],
    }


@pytest.mark.parametrize("repartition_nparts", [2, 3])
def test_agg_set_duplicates_across_partitions(make_df, repartition_nparts, with_morsel_size):
    """Test that set aggregation correctly maintains uniqueness across partitions.

    This test verifies that when we have duplicates across different partitions,
    the set aggregation still maintains uniqueness in the final result. For example,
    if partition 1 has [1, 1, 1] and partition 2 has [1, 2], the final result should
    be [1, 2] and not [1, 1, 2].
    """
    # Create a DataFrame with duplicates that will be distributed across partitions
    daft_df = make_df(
        {
            "group": [1, 1, 1, 1, 1],
            "values": [1, 1, 1, 1, 2],  # Multiple 1s to ensure duplicates across partitions
        },
        repartition=repartition_nparts,
    )

    # Test both global and groupby aggregations
    # Global aggregation
    global_result = daft_df.agg([col("values").agg_set().alias("set")])
    global_result.collect()
    global_set = global_result.to_pydict()["set"][0]

    # The result should be [1, 2] or [2, 1], order doesn't matter
    _assert_all_hashable(global_set, "test_agg_set_duplicates_across_partitions (global)")
    assert len(global_set) == 2, f"Expected 2 unique values, got {len(global_set)} values: {global_set}"
    assert set(global_set) == {1, 2}, f"Expected set {{1, 2}}, got set {set(global_set)}"

    # Groupby aggregation
    group_result = daft_df.groupby("group").agg([col("values").agg_set().alias("set")])
    group_result.collect()
    group_set = group_result.to_pydict()["set"][0]

    # The result should be [1, 2] or [2, 1], order doesn't matter
    _assert_all_hashable(group_set, "test_agg_set_duplicates_across_partitions (group)")
    assert len(group_set) == 2, f"Expected 2 unique values, got {len(group_set)} values: {group_set}"
    assert set(group_set) == {1, 2}, f"Expected set {{1, 2}}, got set {set(group_set)}"


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
@pytest.mark.parametrize(
    "input_values,expected_and,expected_or",
    [
        ([True, True, None], True, True),
        ([True, False, None], False, True),
        ([False, False, None], False, False),
        ([None, None, None], None, None),
        ([True, True, True], True, True),
        ([False, False, False], False, False),
        ([], None, None),  # Empty case
    ],
    ids=[
        "true_true_none",
        "true_false_none",
        "false_false_none",
        "all_none",
        "all_true",
        "all_false",
        "empty",
    ],
)
def test_bool_agg_global(make_df, repartition_nparts, with_morsel_size, input_values, expected_and, expected_or):
    df = make_df({"bool_col": input_values}, repartition=repartition_nparts)

    res = df.agg(col("bool_col").bool_and())
    assert res.to_pydict() == {"bool_col": [expected_and]}, f"bool_and failed for input {input_values}"

    res = df.agg(col("bool_col").bool_or())
    assert res.to_pydict() == {"bool_col": [expected_or]}, f"bool_or failed for input {input_values}"


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_bool_agg_groupby(make_df, repartition_nparts, with_morsel_size):
    df = make_df(
        {
            "group": [1, 1, 1, 2, 2, 2, 3, 3, 3, 4],
            "bool_col": [
                True,
                True,
                None,  # Group 1: true for both and/or
                False,
                None,
                False,  # Group 2: false for both and/or
                None,
                None,
                None,  # Group 3: null for both and/or
                True,  # Group 4: true for both and/or
            ],
        },
        repartition=repartition_nparts,
    )

    # Test bool_and with groups
    res = df.groupby("group").agg(col("bool_col").bool_and()).sort("group")
    assert res.to_pydict() == {"group": [1, 2, 3, 4], "bool_col": [True, False, None, True]}

    # Test bool_or with groups
    res = df.groupby("group").agg(col("bool_col").bool_or()).sort("group")
    assert res.to_pydict() == {"group": [1, 2, 3, 4], "bool_col": [True, False, None, True]}


def test_bool_agg_type_error(make_df):
    df = make_df({"int_col": [1, 2, 3]})

    with pytest.raises(Exception) as exc_info:
        df.agg(col("int_col").bool_and()).collect()
    assert "bool_and is not implemented for type Int64" in str(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        df.agg(col("int_col").bool_or()).collect()
    assert "bool_or is not implemented for type Int64" in str(exc_info.value)
