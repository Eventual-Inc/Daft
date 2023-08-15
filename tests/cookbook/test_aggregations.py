from __future__ import annotations

import pandas as pd
import pytest

from daft.expressions import col
from tests.conftest import assert_df_equals


def test_sum(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Sums across an entire column for the entire table"""
    daft_df = daft_df.repartition(repartition_nparts).sum(col("Unique Key").alias("unique_key_sum"))
    service_requests_csv_pd_df = pd.DataFrame.from_records(
        [{"unique_key_sum": service_requests_csv_pd_df["Unique Key"].sum()}]
    )
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key="unique_key_sum")


def test_mean(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Averages across a column for entire table"""
    daft_df = daft_df.repartition(repartition_nparts).mean(col("Unique Key").alias("unique_key_mean"))
    service_requests_csv_pd_df = pd.DataFrame.from_records(
        [{"unique_key_mean": service_requests_csv_pd_df["Unique Key"].mean()}]
    )
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key="unique_key_mean")


def test_min(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """min across a column for entire table"""
    daft_df = daft_df.repartition(repartition_nparts).min(col("Unique Key").alias("unique_key_min"))
    service_requests_csv_pd_df = pd.DataFrame.from_records(
        [{"unique_key_min": service_requests_csv_pd_df["Unique Key"].min()}]
    )
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key="unique_key_min")


def test_max(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """max across a column for entire table"""
    daft_df = daft_df.repartition(repartition_nparts).max(col("Unique Key").alias("unique_key_max"))
    service_requests_csv_pd_df = pd.DataFrame.from_records(
        [{"unique_key_max": service_requests_csv_pd_df["Unique Key"].max()}]
    )
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key="unique_key_max")


def test_count(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """count a column for entire table"""
    daft_df = daft_df.repartition(repartition_nparts).count(col("Unique Key").alias("unique_key_count"))
    service_requests_csv_pd_df = pd.DataFrame.from_records(
        [{"unique_key_count": service_requests_csv_pd_df["Unique Key"].count()}]
    )
    service_requests_csv_pd_df["unique_key_count"] = service_requests_csv_pd_df["unique_key_count"].astype("uint64")
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key="unique_key_count")


def test_list(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """list agg a column for entire table"""
    daft_df = daft_df.repartition(repartition_nparts).agg_list(col("Unique Key").alias("unique_key_list")).collect()
    unique_key_list = service_requests_csv_pd_df["Unique Key"].to_list()

    result_list = daft_df.to_pydict()["unique_key_list"]
    assert len(result_list) == 1
    assert set(result_list[0]) == set(unique_key_list)


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


@pytest.mark.parametrize(
    "keys",
    [
        pytest.param(["Borough"], id="NumGroupByKeys:1"),
        pytest.param(["Borough", "Complaint Type"], id="NumGroupByKeys:2"),
    ],
)
def test_mean_groupby(daft_df, service_requests_csv_pd_df, repartition_nparts, keys):
    """Sums across groups"""
    daft_df = daft_df.repartition(repartition_nparts).groupby(*[col(k) for k in keys]).mean(col("Unique Key"))
    service_requests_csv_pd_df = service_requests_csv_pd_df.groupby(keys).mean("Unique Key").reset_index()
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key=keys)


@pytest.mark.parametrize(
    "keys",
    [
        pytest.param(["Borough"], id="NumGroupByKeys:1"),
        pytest.param(["Borough", "Complaint Type"], id="NumGroupByKeys:2"),
    ],
)
def test_count_groupby(daft_df, service_requests_csv_pd_df, repartition_nparts, keys):
    """count across groups"""
    daft_df = daft_df.repartition(repartition_nparts).groupby(*[col(k) for k in keys]).count()
    service_requests_csv_pd_df = service_requests_csv_pd_df.groupby(keys).count().reset_index()
    for cname in service_requests_csv_pd_df:
        if cname not in keys:
            service_requests_csv_pd_df[cname] = service_requests_csv_pd_df[cname].astype("uint64")
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key=keys)


@pytest.mark.parametrize(
    "keys",
    [
        pytest.param(["Borough"], id="NumGroupByKeys:1"),
        pytest.param(["Borough", "Complaint Type"], id="NumGroupByKeys:2"),
    ],
)
def test_min_groupby(daft_df, service_requests_csv_pd_df, repartition_nparts, keys):
    """min across groups"""
    daft_df = (
        daft_df.repartition(repartition_nparts)
        .groupby(*[col(k) for k in keys])
        .min(col("Unique Key"), col("Created Date"))
    )
    service_requests_csv_pd_df = (
        service_requests_csv_pd_df.groupby(keys)[["Unique Key", "Created Date"]].min().reset_index()
    )
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key=keys)


@pytest.mark.parametrize(
    "keys",
    [
        pytest.param(["Borough"], id="NumGroupByKeys:1"),
        pytest.param(["Borough", "Complaint Type"], id="NumGroupByKeys:2"),
    ],
)
def test_max_groupby(daft_df, service_requests_csv_pd_df, repartition_nparts, keys):
    """max across groups"""
    daft_df = (
        daft_df.repartition(repartition_nparts)
        .groupby(*[col(k) for k in keys])
        .max(col("Unique Key"), col("Created Date"))
    )
    service_requests_csv_pd_df = (
        service_requests_csv_pd_df.groupby(keys)[["Unique Key", "Created Date"]].max().reset_index()
    )
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key=keys)


@pytest.mark.parametrize(
    "keys",
    [
        pytest.param(["Borough"], id="NumGroupSortKeys:1"),
        pytest.param(["Borough", "Complaint Type"], id="NumGroupSortKeys:2"),
    ],
)
def test_sum_groupby_sorted(daft_df, service_requests_csv_pd_df, repartition_nparts, keys):
    """Test sorting after a groupby"""
    daft_df = (
        daft_df.repartition(repartition_nparts)
        .groupby(*[col(k) for k in keys])
        .sum(col("Unique Key"))
        .sort(by=[col(k) for k in keys], desc=True)
    )
    service_requests_csv_pd_df = (
        service_requests_csv_pd_df.groupby(keys).sum("Unique Key").sort_values(by=keys, ascending=False)
    ).reset_index()
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, assert_ordering=True)
