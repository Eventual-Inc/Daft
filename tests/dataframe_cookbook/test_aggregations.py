import pandas as pd
import pytest

from daft.expressions import col
from tests.conftest import assert_df_equals
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
        [{"unique_key_sum": service_requests_csv_pd_df["Unique Key"].sum()}]
    )
    assert_df_equals(daft_df, service_requests_csv_pd_df, sort_key="unique_key_sum")


@parametrize_service_requests_csv_repartition
@parametrize_service_requests_csv_daft_df
def test_mean(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Averages across a column for entire table"""
    daft_df = daft_df.repartition(repartition_nparts).mean(col("Unique Key").alias("unique_key_mean"))
    service_requests_csv_pd_df = pd.DataFrame.from_records(
        [{"unique_key_mean": service_requests_csv_pd_df["Unique Key"].mean()}]
    )
    assert_df_equals(daft_df, service_requests_csv_pd_df, sort_key="unique_key_mean")


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
    assert_df_equals(daft_df, service_requests_csv_pd_df, sort_key="unique_key_sum")


@pytest.mark.tdd
@parametrize_service_requests_csv_repartition
@parametrize_service_requests_csv_daft_df
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
    service_requests_csv_pd_df = service_requests_csv_pd_df.groupby(keys).sum("Unique Key")
    assert_df_equals(daft_df, service_requests_csv_pd_df, sort_key=keys)


@pytest.mark.tdd
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
        .sort(*[col(k) for k in keys], desc=sort_desc)
    )
    service_requests_csv_pd_df = (
        service_requests_csv_pd_df.groupby(keys).sum("Unique Key").sort_values(by=keys, ascending=not sort_desc)
    )
    assert_df_equals(daft_df, service_requests_csv_pd_df, assert_ordering=True)
