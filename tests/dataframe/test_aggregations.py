from __future__ import annotations

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
            col("values").agg_set(ignore_nulls=True).alias("set_no_nulls"),
            col("values").agg_set(ignore_nulls=False).alias("set_with_nulls"),
        ]
    )
    expected = {
        "sum": [3],
        "mean": [1.5],
        "min": [1],
        "max": [2],
        "count": [2],
        "list": [[1, None, 2]],
        "set_no_nulls": [[1, 2]],
        "set_with_nulls": [[1, None, 2]],
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    res_list = daft_cols.pop("list")
    exp_list = expected.pop("list")
    res_set_no_nulls = daft_cols.pop("set_no_nulls")
    exp_set_no_nulls = expected.pop("set_no_nulls")
    res_set_with_nulls = daft_cols.pop("set_with_nulls")
    exp_set_with_nulls = expected.pop("set_with_nulls")

    assert pa.Table.from_pydict(daft_cols) == pa.Table.from_pydict(expected)

    # Check list agg
    assert len(res_list) == 1
    assert set(res_list[0]) == set(exp_list[0])

    # Check set agg without nulls
    assert len(res_set_no_nulls) == 1
    assert len(res_set_no_nulls[0]) == len(
        set(x for x in res_set_no_nulls[0] if x is not None)
    ), "Result should contain no duplicates"
    assert set(x for x in res_set_no_nulls[0] if x is not None) == set(
        x for x in exp_set_no_nulls[0] if x is not None
    ), "Sets should contain same non-null elements"

    # Check set agg with nulls
    assert len(res_set_with_nulls) == 1
    assert len(res_set_with_nulls[0]) == len(
        set(x for x in res_set_with_nulls[0])
    ), "Result should contain no duplicates"
    assert None in res_set_with_nulls[0], "Result should contain null when ignore_nulls=False"
    assert set(res_set_with_nulls[0]) == set(exp_set_with_nulls[0]), "Sets should contain same elements including nulls"


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
            col("values").agg_set(ignore_nulls=True).alias("set_no_nulls"),
            col("values").agg_set(ignore_nulls=False).alias("set_with_nulls"),
        ]
    )
    expected = {
        "sum": [None],
        "mean": [None],
        "min": [None],
        "max": [None],
        "count": [0],
        "list": [[None, None, None]],
        "set_no_nulls": [[]],  # Empty list since no non-null values
        "set_with_nulls": [[None]],  # List with single null since all values are null
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    # Handle list and set results separately
    res_list = daft_cols.pop("list")
    exp_list = expected.pop("list")
    res_set_no_nulls = daft_cols.pop("set_no_nulls")
    exp_set_no_nulls = expected.pop("set_no_nulls")
    res_set_with_nulls = daft_cols.pop("set_with_nulls")
    exp_set_with_nulls = expected.pop("set_with_nulls")

    # Check regular aggregations
    assert pa.Table.from_pydict(daft_cols) == pa.Table.from_pydict(expected)

    # Check list result
    assert len(res_list) == 1
    assert res_list[0] == exp_list[0]

    # Check set without nulls
    assert len(res_set_no_nulls) == 1
    assert res_set_no_nulls[0] == exp_set_no_nulls[0], "Should be empty list when no non-null values exist"

    # Check set with nulls
    assert len(res_set_with_nulls) == 1
    assert res_set_with_nulls[0] == exp_set_with_nulls[0], "Should contain single null when all values are null"


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
            col("values").agg_set(ignore_nulls=True).alias("set_no_nulls"),
            col("values").agg_set(ignore_nulls=False).alias("set_with_nulls"),
        ]
    )
    expected = {
        "sum": [None],
        "mean": [None],
        "min": [None],
        "max": [None],
        "count": [0],
        "list": [[]],
        "set_no_nulls": [[]],  # Empty list since DataFrame is empty
        "set_with_nulls": [[]],  # Empty list since DataFrame is empty
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    # Handle list and set results separately
    res_list = daft_cols.pop("list")
    exp_list = expected.pop("list")
    res_set_no_nulls = daft_cols.pop("set_no_nulls")
    exp_set_no_nulls = expected.pop("set_no_nulls")
    res_set_with_nulls = daft_cols.pop("set_with_nulls")
    exp_set_with_nulls = expected.pop("set_with_nulls")

    # Check regular aggregations
    assert pa.Table.from_pydict(daft_cols) == pa.Table.from_pydict(expected)

    # Check list result
    assert len(res_list) == 1
    assert res_list[0] == exp_list[0], "List should be empty for empty DataFrame"

    # Check set without nulls
    assert len(res_set_no_nulls) == 1
    assert res_set_no_nulls[0] == exp_set_no_nulls[0], "Set should be empty for empty DataFrame"

    # Check set with nulls
    assert len(res_set_with_nulls) == 1
    assert res_set_with_nulls[0] == exp_set_with_nulls[0], "Set should be empty for empty DataFrame"


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
            col("values").agg_set(ignore_nulls=True).alias("set_no_nulls"),
            col("values").agg_set(ignore_nulls=False).alias("set_with_nulls"),
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
        "set_no_nulls": [[1, 2], [2, 4]],  # Only non-null values, preserving order
        "set_with_nulls": [[1, None, 2], [2, None, 4]],  # All unique values including nulls
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    # Handle list and set results separately
    res_list = daft_cols.pop("list")
    exp_list = expected.pop("list")
    res_set_no_nulls = daft_cols.pop("set_no_nulls")
    exp_set_no_nulls = expected.pop("set_no_nulls")
    res_set_with_nulls = daft_cols.pop("set_with_nulls")
    exp_set_with_nulls = expected.pop("set_with_nulls")

    # Check regular aggregations
    assert sort_arrow_table(pa.Table.from_pydict(daft_cols), "group") == sort_arrow_table(
        pa.Table.from_pydict(expected), "group"
    )

    # Sort results by group for comparison
    arg_sort = np.argsort(daft_cols["group"])

    # Check list results
    assert freeze([list(map(set, res_list))[i] for i in arg_sort]) == freeze(list(map(set, exp_list)))

    # Check set without nulls
    sorted_res_no_nulls = [res_set_no_nulls[i] for i in arg_sort]
    sorted_exp_no_nulls = exp_set_no_nulls
    for res, exp in zip(sorted_res_no_nulls, sorted_exp_no_nulls):
        assert len(res) == len(set(x for x in res if x is not None)), "Result should contain no duplicates"
        assert set(x for x in res if x is not None) == set(
            x for x in exp if x is not None
        ), "Sets should contain same non-null elements"
        assert None not in res, "Result should not contain nulls when ignore_nulls=True"

    # Check set with nulls
    sorted_res_with_nulls = [res_set_with_nulls[i] for i in arg_sort]
    sorted_exp_with_nulls = exp_set_with_nulls
    for res, exp in zip(sorted_res_with_nulls, sorted_exp_with_nulls):
        assert len(res) == len(set(x for x in res)), "Result should contain no duplicates"
        assert set(res) == set(exp), "Sets should contain same elements including nulls"
        if None in exp:
            assert None in res, "Result should contain null when ignore_nulls=False and nulls exist"


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
            col("values").agg_set(ignore_nulls=True).alias("set_no_nulls"),
            col("values").agg_set(ignore_nulls=False).alias("set_with_nulls"),
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
        "set_no_nulls": [[], []],  # Empty lists since no non-null values in any group
        "set_with_nulls": [[None], [None]],  # Single null per group since all values are null
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    # Handle list and set results separately
    res_list = daft_cols.pop("list")
    exp_list = expected.pop("list")
    res_set_no_nulls = daft_cols.pop("set_no_nulls")
    exp_set_no_nulls = expected.pop("set_no_nulls")
    res_set_with_nulls = daft_cols.pop("set_with_nulls")
    exp_set_with_nulls = expected.pop("set_with_nulls")

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
    sorted_res_no_nulls = [res_set_no_nulls[i] for i in arg_sort]
    for res, exp in zip(sorted_res_no_nulls, exp_set_no_nulls):
        assert res == exp, "Should be empty list when no non-null values exist"
        assert len(res) == 0, "Should be empty list when no non-null values exist"

    # Check set with nulls
    sorted_res_with_nulls = [res_set_with_nulls[i] for i in arg_sort]
    for res, exp in zip(sorted_res_with_nulls, exp_set_with_nulls):
        assert res == exp, "Should contain single null when all values are null"
        assert len(res) == 1, "Should contain exactly one null when all values are null"
        assert None in res, "Should contain null when ignore_nulls=False"


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
            col("values").agg_set(ignore_nulls=True).alias("set_no_nulls"),
            col("values").agg_set(ignore_nulls=False).alias("set_with_nulls"),
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
    assert len(daft_cols["set_no_nulls"]) == 1
    assert len(daft_cols["set_no_nulls"][0]) == 3, "Should contain all unique non-null values"
    assert set(daft_cols["set_no_nulls"][0]) == {1, 2, 3}, "Should contain all unique values"
    assert None not in daft_cols["set_no_nulls"][0], "Should not contain nulls"

    # Check set with nulls (should be same as without nulls since no nulls in values)
    assert len(daft_cols["set_with_nulls"]) == 1
    assert len(daft_cols["set_with_nulls"][0]) == 3, "Should contain all unique values"
    assert set(daft_cols["set_with_nulls"][0]) == {1, 2, 3}, "Should contain all unique values"


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
            col("values").agg_set(ignore_nulls=True).alias("set_no_nulls"),
            col("values").agg_set(ignore_nulls=False).alias("set_with_nulls"),
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
        "set_no_nulls": [],  # Empty since DataFrame is empty
        "set_with_nulls": [],  # Empty since DataFrame is empty
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
            col("values").agg_set(ignore_nulls=True).alias("set_no_nulls"),
            col("values").agg_set(ignore_nulls=False).alias("set_with_nulls"),
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
        "set_no_nulls": [[1, 2], [2, 4]],  # Only non-null values, preserving order
        "set_with_nulls": [[1, None, 2], [2, None, 4]],  # All unique values including nulls
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    # Handle list and set results separately
    res_list = daft_cols.pop("list")
    exp_list = expected.pop("list")
    res_set_no_nulls = daft_cols.pop("set_no_nulls")
    exp_set_no_nulls = expected.pop("set_no_nulls")
    res_set_with_nulls = daft_cols.pop("set_with_nulls")
    exp_set_with_nulls = expected.pop("set_with_nulls")

    # Check regular aggregations
    assert sort_arrow_table(pa.Table.from_pydict(daft_cols), "group_alias") == sort_arrow_table(
        pa.Table.from_pydict(expected), "group_alias"
    )

    # Sort results by group for comparison
    arg_sort = np.argsort(daft_cols["group_alias"])

    # Check list results
    assert freeze([list(map(set, res_list))[i] for i in arg_sort]) == freeze(list(map(set, exp_list)))

    # Check set without nulls
    sorted_res_no_nulls = [res_set_no_nulls[i] for i in arg_sort]
    sorted_exp_no_nulls = exp_set_no_nulls
    for res, exp in zip(sorted_res_no_nulls, sorted_exp_no_nulls):
        assert len(res) == len(set(x for x in res if x is not None)), "Result should contain no duplicates"
        assert set(x for x in res if x is not None) == set(
            x for x in exp if x is not None
        ), "Sets should contain same non-null elements"
        assert None not in res, "Result should not contain nulls when ignore_nulls=True"

    # Check set with nulls
    sorted_res_with_nulls = [res_set_with_nulls[i] for i in arg_sort]
    sorted_exp_with_nulls = exp_set_with_nulls
    for res, exp in zip(sorted_res_with_nulls, sorted_exp_with_nulls):
        assert len(res) == len(set(x for x in res)), "Result should contain no duplicates"
        assert set(res) == set(exp), "Sets should contain same elements including nulls"
        if None in exp:
            assert None in res, "Result should contain null when ignore_nulls=False and nulls exist"


@dataclass
class CustomObject:
    val: int

    def __hash__(self):
        return hash(self.val)


def test_agg_pyobjects():
    objects = [CustomObject(val=0), None, CustomObject(val=1)]
    df = daft.from_pydict({"objs": objects})
    df = df.into_partitions(2)
    df = df.agg(
        [
            col("objs").count().alias("count"),
            col("objs").agg_list().alias("list"),
            col("objs").agg_set(ignore_nulls=True).alias("set_no_nulls"),
            col("objs").agg_set(ignore_nulls=False).alias("set_with_nulls"),
        ]
    )
    df.collect()
    res = df.to_pydict()

    assert res["count"] == [2]
    assert set(res["list"][0]) == set(objects)

    # Check set without nulls
    assert len(res["set_no_nulls"][0]) == 2, "Should only contain non-null objects"
    assert set(x.val for x in res["set_no_nulls"][0]) == {0, 1}, "Should contain correct non-null values"
    assert None not in res["set_no_nulls"][0], "Should not contain nulls"

    # Check set with nulls
    assert len(res["set_with_nulls"][0]) == 3, "Should contain all unique objects including null"
    assert set(x.val if x is not None else None for x in res["set_with_nulls"][0]) == {
        0,
        1,
        None,
    }, "Should contain all values including null"
    assert None in res["set_with_nulls"][0], "Should contain null"


def test_groupby_agg_pyobjects():
    objects = [CustomObject(val=0), CustomObject(val=1), None, None, CustomObject(val=2)]
    df = daft.from_pydict({"objects": objects, "groups": [1, 2, 1, 2, 1]})
    df = df.into_partitions(2)
    df = (
        df.groupby(col("groups"))
        .agg(
            [
                col("objects").count().alias("count"),
                col("objects").agg_list().alias("list"),
                col("objects").agg_set(ignore_nulls=True).alias("set_no_nulls"),
                col("objects").agg_set(ignore_nulls=False).alias("set_with_nulls"),
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

    # Check set without nulls for group 1
    assert len(res["set_no_nulls"][0]) == 2, "Group 1 should have two non-null objects"
    assert set(x.val for x in res["set_no_nulls"][0]) == {0, 2}, "Group 1 should have correct non-null values"
    assert None not in res["set_no_nulls"][0], "Group 1 should not contain nulls"

    # Check set without nulls for group 2
    assert len(res["set_no_nulls"][1]) == 1, "Group 2 should have one non-null object"
    assert set(x.val for x in res["set_no_nulls"][1]) == {1}, "Group 2 should have correct non-null value"
    assert None not in res["set_no_nulls"][1], "Group 2 should not contain nulls"

    # Check set with nulls for group 1
    assert len(res["set_with_nulls"][0]) == 3, "Group 1 should have all unique objects including null"
    assert set(x.val if x is not None else None for x in res["set_with_nulls"][0]) == {
        0,
        2,
        None,
    }, "Group 1 should have all values including null"
    assert None in res["set_with_nulls"][0], "Group 1 should contain null"

    # Check set with nulls for group 2
    assert len(res["set_with_nulls"][1]) == 2, "Group 2 should have all unique objects including null"
    assert set(x.val if x is not None else None for x in res["set_with_nulls"][1]) == {
        1,
        None,
    }, "Group 2 should have all values including null"
    assert None in res["set_with_nulls"][1], "Group 2 should contain null"


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
