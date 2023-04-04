from __future__ import annotations

from daft.expressions import col
from tests.conftest import assert_df_equals


def test_add_one_to_column(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Creating a new column that is derived from (1 + other_column) and retrieving the top N results"""
    daft_df = daft_df.repartition(repartition_nparts).with_column("unique_key_mod", col("Unique Key") + 1)
    service_requests_csv_pd_df["unique_key_mod"] = service_requests_csv_pd_df["Unique Key"] + 1
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)


def test_add_one_to_column_name_override(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Creating a new column that is derived from (1 + other_column) and retrieving the top N results"""
    daft_df = (
        daft_df.repartition(repartition_nparts)
        .with_column("Unique Key", col("Unique Key") + 1)
        .with_column("Unique Key", col("Unique Key") + 1)
    )
    service_requests_csv_pd_df["Unique Key"] = service_requests_csv_pd_df["Unique Key"] + 2
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)


def test_add_one_to_column_limit(daft_df, service_requests_csv_pd_df):
    """Creating a new column that is derived from (1 + other_column) and retrieving the top N results"""
    daft_df = daft_df.with_column("unique_key_mod", col("Unique Key") + 1).limit(10)
    service_requests_csv_pd_df["unique_key_mod"] = service_requests_csv_pd_df["Unique Key"] + 1
    service_requests_csv_pd_df = service_requests_csv_pd_df.head(10)
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)


def test_add_one_twice_to_column(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Creating a new column that is derived from (1 + other_column) and retrieving the top N results"""
    daft_df = daft_df.repartition(repartition_nparts).with_column("unique_key_mod", col("Unique Key") + 1)
    daft_df = daft_df.with_column("unique_key_mod_second", col("unique_key_mod") + 1)
    service_requests_csv_pd_df["unique_key_mod"] = service_requests_csv_pd_df["Unique Key"] + 1
    service_requests_csv_pd_df["unique_key_mod_second"] = service_requests_csv_pd_df["unique_key_mod"] + 1
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)


def test_difference_cols(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Creating a new column that is derived from 2 other columns and retrieving the top N results"""
    daft_df = daft_df.repartition(repartition_nparts).with_column(
        "unique_key_mod", col("Unique Key") - col("Unique Key")
    )
    service_requests_csv_pd_df["unique_key_mod"] = (
        service_requests_csv_pd_df["Unique Key"] - service_requests_csv_pd_df["Unique Key"]
    )
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)
