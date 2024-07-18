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
def test_agg_global(make_df, repartition_nparts):
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
        ]
    )
    expected = {"sum": [3], "mean": [1.5], "min": [1], "max": [2], "count": [2], "list": [[1, None, 2]]}

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    res_list = daft_cols.pop("list")
    exp_list = expected.pop("list")

    assert pa.Table.from_pydict(daft_cols) == pa.Table.from_pydict(expected)
    assert len(res_list) == 1
    assert set(res_list[0]) == set(exp_list[0])


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_agg_global_all_null(make_df, repartition_nparts):
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
        ]
    )
    expected = {
        "sum": [None],
        "mean": [None],
        "min": [None],
        "max": [None],
        "count": [0],
        "list": [[None, None, None]],
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    assert pa.Table.from_pydict(daft_cols) == pa.Table.from_pydict(expected)


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
        ]
    )
    expected = {
        "sum": [None],
        "mean": [None],
        "min": [None],
        "max": [None],
        "count": [0],
        "list": [[]],
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    assert pa.Table.from_pydict(daft_cols) == pa.Table.from_pydict(expected)


@pytest.mark.parametrize("repartition_nparts", [1, 2, 7])
def test_agg_groupby(make_df, repartition_nparts):
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
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()
    res_list = daft_cols.pop("list")
    exp_list = expected.pop("list")

    assert sort_arrow_table(pa.Table.from_pydict(daft_cols), "group") == sort_arrow_table(
        pa.Table.from_pydict(expected), "group"
    )

    arg_sort = np.argsort(daft_cols["group"])
    assert freeze([list(map(set, res_list))[i] for i in arg_sort]) == freeze(list(map(set, exp_list)))


@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
def test_agg_groupby_all_null(make_df, repartition_nparts):
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
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    assert sort_arrow_table(pa.Table.from_pydict(daft_cols), "group") == sort_arrow_table(
        pa.Table.from_pydict(expected), "group"
    )


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
def test_null_groupby_keys(make_df, repartition_nparts):
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
def test_all_null_groupby_keys(make_df, repartition_nparts):
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
        )
    )

    daft_cols = daft_df.to_pydict()

    assert daft_cols["group"] == [None]
    assert daft_cols["mean"] == [2.0]
    assert len(daft_cols["list"]) == 1
    assert set(daft_cols["list"][0]) == {1, 2, 3}


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
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()
    assert sort_arrow_table(pa.Table.from_pydict(daft_cols), "group") == sort_arrow_table(
        pa.Table.from_pydict(expected), "group"
    )


@dataclass
class CustomObject:
    val: int


def test_agg_pyobjects():
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
    res = df.to_pydict()

    assert res["count"] == [2]
    assert res["list"] == [objects]


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
            ]
        )
        .sort(col("groups"))
    )

    df.collect()
    res = df.to_pydict()
    assert res["groups"] == [1, 2]
    assert res["count"] == [2, 1]
    assert res["list"] == [[objects[0], objects[2], objects[4]], [objects[1], objects[3]]]


@pytest.mark.parametrize("shuffle_aggregation_default_partitions", [None, 20])
def test_groupby_result_partitions_smaller_than_input(shuffle_aggregation_default_partitions):
    if shuffle_aggregation_default_partitions is None:
        min_partitions = get_context().daft_execution_config.shuffle_aggregation_default_partitions
    else:
        daft.set_execution_config(shuffle_aggregation_default_partitions=shuffle_aggregation_default_partitions)
        min_partitions = shuffle_aggregation_default_partitions

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


def test_agg_deprecation():
    with pytest.deprecated_call():
        df = daft.from_pydict({"a": [1, 2, 3], "b": [True, False, True]})
        df = df.agg([("a", "sum"), ("b", "count")])
        df.collect()

        assert df.to_pydict() == {"a": [6], "b": [3]}

    with pytest.deprecated_call():
        df = daft.from_pydict({"a": [1, 2, 3], "b": [True, False, True]})
        df = df.groupby("b").agg([("a", "sum")])
        df.collect()

        assert df.to_pydict() == {"b": [True, False], "a": [4, 2]}


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_agg_any_value(make_df, repartition_nparts):
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
def test_agg_any_value_ignore_nulls(make_df, repartition_nparts):
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
