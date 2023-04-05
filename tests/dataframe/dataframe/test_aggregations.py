from __future__ import annotations

import pyarrow as pa
import pytest

from daft import DataFrame, col
from daft.datatype import DataType
from daft.errors import ExpressionTypeError


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_agg_global(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "id": [1, 2, 3],
            "values": [1, None, 2],
        }
    )
    daft_df = daft_df.repartition(repartition_nparts).agg(
        [
            (col("values").alias("sum"), "sum"),
            (col("values").alias("mean"), "mean"),
            (col("values").alias("min"), "min"),
            (col("values").alias("max"), "max"),
            (col("values").alias("count"), "count"),
        ]
    )
    expected = {
        "sum": [3],
        "mean": [1.5],
        "min": [1],
        "max": [2],
        "count": [2],
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    assert pa.Table.from_pydict(daft_cols) == pa.Table.from_pydict(expected)


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_agg_global_all_null(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "id": [0, 1, 2, 3],
            "values": [1, None, None, None],
        }
    )
    daft_df = (
        daft_df.where(col("id") != 0)
        .repartition(repartition_nparts)
        .agg(
            [
                (col("values").alias("sum"), "sum"),
                (col("values").alias("mean"), "mean"),
                (col("values").alias("min"), "min"),
                (col("values").alias("max"), "max"),
                (col("values").alias("count"), "count"),
            ]
        )
    )
    expected = {
        "sum": [None],
        "mean": [None],
        "min": [None],
        "max": [None],
        "count": [0],
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    assert pa.Table.from_pydict(daft_cols) == pa.Table.from_pydict(expected)


def test_agg_global_empty():
    daft_df = DataFrame.from_pydict(
        {
            "id": [0],
            "values": [1],
        }
    )
    daft_df = (
        daft_df.where(col("id") != 0)
        .repartition(2)
        .agg(
            [
                (col("values").alias("sum"), "sum"),
                (col("values").alias("mean"), "mean"),
                (col("values").alias("min"), "min"),
                (col("values").alias("max"), "max"),
                (col("values").alias("count"), "count"),
            ]
        )
    )
    expected = {
        "sum": [None],
        "mean": [None],
        "min": [None],
        "max": [None],
        "count": [0],
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    assert pa.Table.from_pydict(daft_cols) == pa.Table.from_pydict(expected)


@pytest.mark.parametrize("repartition_nparts", [1, 2, 7])
def test_agg_groupby(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "group": [1, 1, 1, 2, 2, 2],
            "values": [1, None, 2, 2, None, 4],
        }
    )
    daft_df = (
        daft_df.repartition(repartition_nparts)
        .groupby("group")
        .agg(
            [
                (col("values").alias("sum"), "sum"),
                (col("values").alias("mean"), "mean"),
                (col("values").alias("min"), "min"),
                (col("values").alias("max"), "max"),
                (col("values").alias("count"), "count"),
            ]
        )
    )
    expected = {
        "group": [1, 2],
        "sum": [3, 6],
        "mean": [1.5, 3],
        "min": [1, 2],
        "max": [2, 4],
        "count": [2, 2],
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    assert pa.Table.from_pydict(daft_cols).sort_by("group") == pa.Table.from_pydict(expected).sort_by("group")


@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
def test_agg_groupby_all_null(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "id": [0, 1, 2, 3, 4],
            "group": [0, 1, 1, 2, 2],
            "values": [1, None, None, None, None],
        }
    )
    # Remove the first row so that all values are Null
    daft_df = daft_df.where(col("id") != 0).repartition(repartition_nparts)
    daft_df = daft_df.groupby(col("group")).agg(
        [
            (col("values").alias("sum"), "sum"),
            (col("values").alias("mean"), "mean"),
            (col("values").alias("min"), "min"),
            (col("values").alias("max"), "max"),
            (col("values").alias("count"), "count"),
        ]
    )

    expected = {
        "group": [1, 2],
        "sum": [None, None],
        "mean": [None, None],
        "min": [None, None],
        "max": [None, None],
        "count": [0, 0],
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    assert pa.Table.from_pydict(daft_cols).sort_by("group") == pa.Table.from_pydict(expected).sort_by("group")


def test_agg_groupby_null_type_column():
    daft_df = DataFrame.from_pydict(
        {
            "id": [1, 2, 3, 4],
            "group": [1, 1, 2, 2],
            "values": [None, None, None, None],
        }
    )
    daft_df = daft_df.groupby(col("group"))

    with pytest.raises(ValueError):
        daft_df.agg(
            [
                (col("values").alias("sum"), "sum"),
            ]
        )


@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
def test_null_groupby_keys(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "id": [0, 1, 2, 3, 4],
            "group": [0, 1, None, 2, None],
            "values": [0, 1, 3, 2, 3],
        }
    )

    daft_df = (
        daft_df.repartition(repartition_nparts)
        .groupby(col("group"))
        .agg(
            [
                (col("values").alias("mean"), "mean"),
            ]
        )
    )

    expected = {
        "group": [0, 1, 2, None],
        "mean": [0.0, 1.0, 2.0, 3.0],
    }
    daft_cols = daft_df.to_pydict()
    assert pa.Table.from_pydict(daft_cols).sort_by("group") == pa.Table.from_pydict(expected).sort_by("group")


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_all_null_groupby_keys(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "id": [0, 1, 2],
            "group": [None, None, None],
            "values": [1, 2, 3],
        }
    )

    daft_df = (
        daft_df.repartition(repartition_nparts)
        .with_column("group", daft_df["group"].cast(DataType.int64()))
        .groupby(col("group"))
        .agg(
            [
                (col("values").alias("mean"), "mean"),
            ]
        )
    )

    expected = {
        "group": [None],
        "mean": [2.0],
    }
    daft_cols = daft_df.to_pydict()
    assert pa.Table.from_pydict(daft_cols).sort_by("group") == pa.Table.from_pydict(expected).sort_by("group")


def test_null_type_column_groupby_keys():
    daft_df = DataFrame.from_pydict(
        {
            "id": [0, 1, 2],
            "group": [None, None, None],
            "values": [1, 2, 3],
        }
    )

    with pytest.raises(ExpressionTypeError):
        daft_df.groupby(col("group"))


def test_agg_groupby_empty():
    daft_df = DataFrame.from_pydict(
        {
            "id": [0],
            "group": [0],
            "values": [1],
        }
    )
    # Remove the first row so that dataframe is empty
    daft_df = daft_df.where(col("id") != 0).repartition(2)
    daft_df = daft_df.groupby(col("group")).agg(
        [
            (col("values").alias("sum"), "sum"),
            (col("values").alias("mean"), "mean"),
            (col("values").alias("min"), "min"),
            (col("values").alias("max"), "max"),
            (col("values").alias("count"), "count"),
        ]
    )

    expected = {
        "group": [],
        "sum": [],
        "mean": [],
        "min": [],
        "max": [],
        "count": [],
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()
    assert pa.Table.from_pydict(daft_cols).sort_by("group") == pa.Table.from_pydict(expected).sort_by("group")
