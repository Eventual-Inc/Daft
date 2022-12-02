from __future__ import annotations

import pyarrow as pa
import pytest

from daft import DataFrame, col
from daft.errors import ExpressionTypeError
from tests.conftest import assert_arrow_equals, assert_list_columns_equal


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
            (col("values").alias("list"), "list"),
        ]
    )
    expected_arrow_table = {
        "sum": pa.array([3]),
        "mean": pa.array([1.5], type=pa.float64()),
        "min": pa.array([1], type=pa.int64()),
        "max": pa.array([2], type=pa.int64()),
        "count": pa.array([2], type=pa.int64()),
    }
    expected_list_cols = {
        "list": [[1, None, 2]],
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    assert_arrow_equals(
        {key: col for key, col in daft_cols.items() if key in expected_arrow_table},
        expected_arrow_table,
        sort_key="sum",
    )

    assert_list_columns_equal(
        {key: col for key, col in daft_cols.items() if key in expected_list_cols},
        expected_list_cols,
    )


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
                (col("values").alias("list"), "list"),
            ]
        )
    )
    expected_arrow_table = {
        "sum": pa.array([0]),
        "mean": pa.array([float("nan")]),
        "min": pa.array([None], type=pa.int64()),
        "max": pa.array([None], type=pa.int64()),
        "count": pa.array([0]),
    }
    expected_list_cols = {
        "list": [[None, None, None]],
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    assert_arrow_equals(
        {key: col for key, col in daft_cols.items() if key in expected_arrow_table},
        expected_arrow_table,
        sort_key="sum",
    )

    assert_list_columns_equal(
        {key: col for key, col in daft_cols.items() if key in expected_list_cols},
        expected_list_cols,
    )


def test_agg_global_null_type_column():
    daft_df = DataFrame.from_pydict(
        {
            "id": [1, 2, 3],
            "values": pa.array([None, None, None], type=pa.null()),
        }
    )
    with pytest.raises(ExpressionTypeError):
        daft_df.agg(
            [
                (col("values").alias("sum"), "sum"),
            ]
        )


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
                (col("values").alias("list"), "list"),
            ]
        )
    )
    expected_arrow_table = {
        "sum": pa.array([], type=pa.int64()),
        "mean": pa.array([], type=pa.float64()),
        "min": pa.array([], type=pa.int64()),
        "max": pa.array([], type=pa.int64()),
        "count": pa.array([], type=pa.int64()),
    }
    expected_list_cols = {
        "list": [],
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    assert_arrow_equals(
        {key: col for key, col in daft_cols.items() if key in expected_arrow_table},
        expected_arrow_table,
        sort_key="sum",
    )

    assert_list_columns_equal(
        {key: col for key, col in daft_cols.items() if key in expected_list_cols},
        expected_list_cols,
    )


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
                (col("values").alias("list"), "list"),
            ]
        )
    )
    expected_arrow_table = {
        "group": pa.array([1, 2]),
        "sum": pa.array([3, 6]),
        "mean": pa.array([1.5, 3], type=pa.float64()),
        "min": pa.array([1, 2], type=pa.int64()),
        "max": pa.array([2, 4], type=pa.int64()),
        "count": pa.array([2, 2], type=pa.int64()),
    }
    expected_list_cols = {
        "list": [[1, None, 2], [2, None, 4]],
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    assert_arrow_equals(
        {key: col for key, col in daft_cols.items() if key in expected_arrow_table},
        expected_arrow_table,
        sort_key="group",
    )

    assert_list_columns_equal(
        {key: col for key, col in daft_cols.items() if key in expected_list_cols},
        expected_list_cols,
        daft_col_keys=daft_cols["group"].to_pylist(),
    )


# @pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
@pytest.mark.parametrize("repartition_nparts", [2])
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
            (col("values").alias("list"), "list"),
        ]
    )

    expected_arrow_table = {
        "group": pa.array([1, 2]),
        "sum": pa.array([0, 0]),
        "mean": pa.array([float("nan"), float("nan")]),
        "min": pa.array([None, None], type=pa.int64()),
        "max": pa.array([None, None], type=pa.int64()),
        "count": pa.array([0, 0]),
    }

    expected_list_cols = {
        "list": [[None, None], [None, None]],
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    assert_arrow_equals(
        {key: col for key, col in daft_cols.items() if key in expected_arrow_table},
        expected_arrow_table,
        sort_key="group",
    )

    assert_list_columns_equal(
        {key: col for key, col in daft_cols.items() if key in expected_list_cols},
        expected_list_cols,
        daft_col_keys=daft_cols["group"].to_pylist(),
    )


def test_agg_groupby_null_type_column():
    daft_df = DataFrame.from_pydict(
        {
            "id": [1, 2, 3, 4],
            "group": [1, 1, 2, 2],
            "values": pa.array([None, None, None, None], type=pa.null()),
        }
    )
    daft_df = daft_df.groupby(col("group"))

    with pytest.raises(ExpressionTypeError):
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

    expected_arrow_table = {
        "group": pa.array([0, 1, 2, None]),
        "mean": pa.array([0.0, 1.0, 2.0, 3.0]),
    }
    daft_df.collect()
    assert_arrow_equals(daft_df.to_pydict(), expected_arrow_table, sort_key="group")


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_all_null_groupby_keys(repartition_nparts):
    daft_df = DataFrame.from_pydict(
        {
            "id": [0, 1, 2],
            "group": pa.array([None, None, None], type=pa.int64()),
            "values": [1, 2, 3],
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

    expected_arrow_table = {
        "group": pa.array([None], type=pa.int64()),
        "mean": pa.array([2.0]),
    }
    daft_df.collect()
    assert_arrow_equals(daft_df.to_pydict(), expected_arrow_table, sort_key="group")


def test_null_type_column_groupby_keys():
    daft_df = DataFrame.from_pydict(
        {
            "id": [0, 1, 2],
            "group": pa.array([None, None, None], pa.null()),
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
            (col("values").alias("list"), "list"),
        ]
    )

    expected_arrow_table = {
        "group": pa.array([], type=pa.int64()),
        "sum": pa.array([], type=pa.int64()),
        "mean": pa.array([], type=pa.float64()),
        "min": pa.array([], type=pa.int64()),
        "max": pa.array([], type=pa.int64()),
        "count": pa.array([], type=pa.int64()),
    }
    expected_list_cols = {
        "list": [],
    }

    daft_df.collect()
    daft_cols = daft_df.to_pydict()

    assert_arrow_equals(
        {key: col for key, col in daft_cols.items() if key in expected_arrow_table},
        expected_arrow_table,
        sort_key="group",
    )

    assert_list_columns_equal(
        {key: col for key, col in daft_cols.items() if key in expected_list_cols},
        expected_list_cols,
        daft_col_keys=daft_cols["group"].to_pylist(),
    )
