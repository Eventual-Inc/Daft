import pandas as pd
import pyarrow as pa
import pytest

from daft import DataFrame
from daft.expressions import col
from tests.conftest import assert_arrow_equals, assert_df_equals
from tests.dataframe_cookbook.conftest import (
    parametrize_service_requests_csv_daft_df,
    parametrize_service_requests_csv_repartition,
    parametrize_sort_desc,
)


@parametrize_service_requests_csv_repartition
@parametrize_service_requests_csv_daft_df
def test_sum(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Sums across an entire column for the entire table"""
    daft_df = daft_df.repartition(repartition_nparts).sum(col("Unique Key").alias("unique_key_sum"))
    service_requests_csv_pd_df = pd.DataFrame.from_records(
        [{"unique_key_sum": service_requests_csv_pd_df["Unique Key"].sum().astype(int)}]
    )
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key="unique_key_sum")


@parametrize_service_requests_csv_repartition
@parametrize_service_requests_csv_daft_df
def test_mean(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Averages across a column for entire table"""
    daft_df = daft_df.repartition(repartition_nparts).mean(col("Unique Key").alias("unique_key_mean"))
    service_requests_csv_pd_df = pd.DataFrame.from_records(
        [{"unique_key_mean": service_requests_csv_pd_df["Unique Key"].mean()}]
    )
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key="unique_key_mean")


@parametrize_service_requests_csv_repartition
@parametrize_service_requests_csv_daft_df
def test_global_agg(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Averages across a column for entire table"""
    daft_df = daft_df.repartition(repartition_nparts).agg(
        [
            (col("Unique Key").alias("unique_key_mean"), "mean"),
            (col("Unique Key").alias("unique_key_sum"), "sum"),
            (col("Borough").alias("borough_min"), "min"),
            (col("Borough").alias("borough_max"), "max"),
        ]
    )
    service_requests_csv_pd_df = pd.DataFrame.from_records(
        [
            {
                "unique_key_mean": service_requests_csv_pd_df["Unique Key"].mean(),
                "unique_key_sum": service_requests_csv_pd_df["Unique Key"].sum(),
                "borough_min": service_requests_csv_pd_df["Borough"].min(),
                "borough_max": service_requests_csv_pd_df["Borough"].max(),
            }
        ]
    )
    daft_pd_df = daft_df.to_pandas()

    # Pandas handles Nulls as NaN, which casts int columns to float. We cast
    # here to ensure that all the types match when checking correctness
    for key, t in [("unique_key_sum", int)]:
        daft_pd_df[key] = daft_pd_df[key].astype(t)
        service_requests_csv_pd_df[key] = service_requests_csv_pd_df[key].astype(t)

    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key="unique_key_mean")


@parametrize_service_requests_csv_repartition
@parametrize_service_requests_csv_daft_df
def test_filtered_sum(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Sums across an entire column for the entire table filtered by a certain condition"""
    daft_df = (
        daft_df.repartition(repartition_nparts)
        .where(col("Borough") == "BROOKLYN")
        .sum(col("Unique Key").alias("unique_key_sum"))
    )
    service_requests_csv_pd_df = pd.DataFrame.from_records(
        [
            {
                "unique_key_sum": service_requests_csv_pd_df[service_requests_csv_pd_df["Borough"] == "BROOKLYN"][
                    "Unique Key"
                ].sum()
            }
        ]
    )
    service_requests_csv_pd_df["unique_key_sum"] = service_requests_csv_pd_df["unique_key_sum"].astype(int)
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key="unique_key_sum")


@parametrize_service_requests_csv_repartition
@parametrize_service_requests_csv_daft_df
@pytest.mark.parametrize(
    "keys",
    [
        pytest.param(["Borough"], id="NumGroupByKeys:1"),
        pytest.param(["Borough", "Complaint Type"], id="NumGroupByKeys:2"),
    ],
)
def test_agg_groupby(daft_df, service_requests_csv_pd_df, repartition_nparts, keys):
    daft_df = (
        daft_df.repartition(repartition_nparts)
        .groupby(*[col(k) for k in keys])
        .agg(
            [
                (col("Unique Key").alias("unique_key_sum"), "sum"),
                (col("Unique Key").alias("unique_key_mean"), "mean"),
                (col("Unique Key").alias("unique_key_min"), "min"),
                (col("Unique Key").alias("unique_key_max"), "max"),
                (col("Unique Key").alias("unique_key_count"), "count"),
            ]
        )
    )
    service_requests_csv_pd_df = (
        service_requests_csv_pd_df.groupby(keys)
        .agg(
            unique_key_sum=pd.NamedAgg(column="Unique Key", aggfunc="sum"),
            unique_key_mean=pd.NamedAgg(column="Unique Key", aggfunc="mean"),
            unique_key_min=pd.NamedAgg(column="Unique Key", aggfunc="min"),
            unique_key_max=pd.NamedAgg(column="Unique Key", aggfunc="max"),
            unique_key_count=pd.NamedAgg(column="Unique Key", aggfunc="count"),
        )
        .reset_index()
    )

    daft_pd_df = daft_df.to_pandas()

    # Pandas handles Nulls as NaN, which casts int columns to float. We cast
    # here to ensure that all the types match when checking correctness
    for key, t in [("unique_key_sum", int), ("unique_key_max", float), ("unique_key_min", float)]:
        daft_pd_df[key] = daft_pd_df[key].astype(t)
        service_requests_csv_pd_df[key] = service_requests_csv_pd_df[key].astype(t)

    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key=keys)


# We are skippping due to a bug in polars with groupby aggregations with min of strings
@pytest.mark.skip()
@parametrize_service_requests_csv_repartition
@parametrize_service_requests_csv_daft_df
@pytest.mark.parametrize(
    "keys",
    [
        pytest.param(["Borough"], id="NumGroupByKeys:1"),
        pytest.param(["Borough", "Complaint Type"], id="NumGroupByKeys:2"),
    ],
)
def test_min_groupby(daft_df, service_requests_csv_pd_df, repartition_nparts, keys):
    """Sums across groups"""
    daft_df = (
        daft_df.repartition(repartition_nparts)
        .groupby(*[col(k) for k in keys])
        .min(col("Unique Key"), col("Created Date"))
    )
    service_requests_csv_pd_df = (
        service_requests_csv_pd_df.groupby(keys)["Unique Key", "Created Date"].min().reset_index()
    )
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key=keys)


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
    daft_df = (
        daft_df.groupby(col("group"))
        .agg(
            [
                (col("values").alias("sum"), "sum"),
                (col("values").alias("mean"), "mean"),
                (col("values").alias("min"), "min"),
                (col("values").alias("max"), "max"),
                (col("values").alias("count"), "count"),
            ]
        )
        .sort(col("group"))
    )

    expected_arrow_table = {
        "group": pa.array([1, 2]),
        "sum": pa.array([0, 0]),
        "mean": pa.array([float("nan"), float("nan")]),
        "min": pa.array([None, None], type=pa.int64()),
        "max": pa.array([None, None], type=pa.int64()),
        "count": pa.array([0, 0]),
    }

    daft_df.collect()
    assert_arrow_equals(daft_df._result.to_pydict(), expected_arrow_table, sort_key="group")


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
    daft_df = (
        daft_df.groupby(col("group"))
        .agg(
            [
                (col("values").alias("sum"), "sum"),
                (col("values").alias("mean"), "mean"),
                (col("values").alias("min"), "min"),
                (col("values").alias("max"), "max"),
                (col("values").alias("count"), "count"),
            ]
        )
        .sort(col("group"))
    )

    expected_arrow_table = {
        "group": pa.array([], type=pa.int64()),
        "sum": pa.array([], type=pa.int64()),
        "mean": pa.array([], type=pa.float64()),
        "min": pa.array([], type=pa.int64()),
        "max": pa.array([], type=pa.int64()),
        "count": pa.array([], type=pa.int64()),
    }

    daft_df.collect()

    assert_arrow_equals(daft_df._result.to_pydict(), expected_arrow_table, sort_key="group")


@parametrize_service_requests_csv_repartition
@parametrize_service_requests_csv_daft_df
@parametrize_sort_desc("sort_desc")
@pytest.mark.parametrize(
    "keys",
    [
        pytest.param(["Borough"], id="NumGroupSortKeys:1"),
        pytest.param(["Borough", "Complaint Type"], id="NumGroupSortKeys:2"),
    ],
)
def test_sum_groupby_sorted(daft_df, sort_desc, service_requests_csv_pd_df, repartition_nparts, keys):
    """Test sorting after a groupby"""
    daft_df = (
        daft_df.repartition(repartition_nparts)
        .groupby(*[col(k) for k in keys])
        .sum(col("Unique Key"))
        .sort(by=[col(k) for k in keys], desc=sort_desc)
    )
    service_requests_csv_pd_df = (
        service_requests_csv_pd_df.groupby(keys).sum("Unique Key").sort_values(by=keys, ascending=not sort_desc)
    ).reset_index()[[*keys, "Unique Key"]]
    service_requests_csv_pd_df["Unique Key"] = service_requests_csv_pd_df["Unique Key"].astype(int)
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, assert_ordering=True, sort_key=keys)
