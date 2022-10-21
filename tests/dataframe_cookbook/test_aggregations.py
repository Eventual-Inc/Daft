from __future__ import annotations

import pandas as pd
import pytest

from daft.expressions import col
from tests.conftest import assert_df_equals
from tests.dataframe_cookbook.conftest import (
    parametrize_service_requests_csv_repartition,
    parametrize_sort_desc,
)


@parametrize_service_requests_csv_repartition
def test_sum(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Sums across an entire column for the entire table"""
    daft_df = daft_df.repartition(repartition_nparts).sum(col("Unique Key").alias("unique_key_sum"))
    service_requests_csv_pd_df = pd.DataFrame.from_records(
        [{"unique_key_sum": service_requests_csv_pd_df["Unique Key"].sum()}]
    )
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key="unique_key_sum")


@parametrize_service_requests_csv_repartition
def test_mean(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Averages across a column for entire table"""
    daft_df = daft_df.repartition(repartition_nparts).mean(col("Unique Key").alias("unique_key_mean"))
    service_requests_csv_pd_df = pd.DataFrame.from_records(
        [{"unique_key_mean": service_requests_csv_pd_df["Unique Key"].mean()}]
    )
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key="unique_key_mean")


@parametrize_service_requests_csv_repartition
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
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key="unique_key_mean")


@parametrize_service_requests_csv_repartition
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
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key="unique_key_sum")


@parametrize_service_requests_csv_repartition
@pytest.mark.parametrize(
    "keys",
    [
        pytest.param(["Borough"], id="NumGroupByKeys:1"),
        pytest.param(["Borough", "Complaint Type"], id="NumGroupByKeys:2"),
    ],
)
def test_sum_groupby(daft_df, service_requests_csv_pd_df, repartition_nparts, keys):
    """Sums across groups"""
    daft_df = daft_df.repartition(repartition_nparts).groupby(*[col(k) for k in keys]).sum(col("Unique Key"))
    service_requests_csv_pd_df = service_requests_csv_pd_df.groupby(keys).sum("Unique Key").reset_index()
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key=keys)


# We are skippping due to a bug in polars with groupby aggregations with min
@pytest.mark.skip()
@parametrize_service_requests_csv_repartition
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


@parametrize_service_requests_csv_repartition
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
    ).reset_index()
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, assert_ordering=True)
