from __future__ import annotations

import pandas as pd

from daft import DataType, lit
from tests.conftest import assert_df_equals


def test_literal_column(daft_df, service_requests_csv_pd_df):
    """Creating a new column that is derived from (1 + other_column) and retrieving the top N results"""
    daft_df = daft_df.with_column("literal_col", lit(1))
    daft_pd_df = daft_df.to_pandas()
    service_requests_csv_pd_df["literal_col"] = 1
    service_requests_csv_pd_df["literal_col"] = service_requests_csv_pd_df["literal_col"].astype("int32")
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)


def test_literal_column_computation(daft_df, service_requests_csv_pd_df):
    """Creating a new column that is derived from (1 + other_column) and retrieving the top N results"""
    daft_df = daft_df.with_column("literal_col", lit(1) + 1)
    daft_pd_df = daft_df.to_pandas()
    service_requests_csv_pd_df["literal_col"] = 1 + 1
    service_requests_csv_pd_df["literal_col"] = service_requests_csv_pd_df["literal_col"].astype("int32")
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)


def test_literal_column_aggregation(daft_df, service_requests_csv_pd_df):
    """Creating a new column that is derived from (1 + other_column) and retrieving the top N results"""
    daft_df = daft_df.repartition(2).groupby("Borough").agg([("Unique Key", "sum")])
    daft_df = daft_df.with_column("literal_col", lit(1) + 1)
    daft_pd_df = daft_df.to_pandas()
    service_requests_csv_pd_df = service_requests_csv_pd_df.groupby("Borough").agg({"Unique Key": "sum"})
    service_requests_csv_pd_df = service_requests_csv_pd_df.reset_index()[["Unique Key", "Borough"]]
    service_requests_csv_pd_df["literal_col"] = 1 + 1
    service_requests_csv_pd_df["literal_col"] = service_requests_csv_pd_df["literal_col"].astype("int32")
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df, sort_key="Borough")


def test_pyobj_literal_column(daft_df, service_requests_csv_pd_df):
    """Creating a new column that is derived from (1 + other_column) and retrieving the top N results"""
    daft_df = daft_df.with_column("literal_col", lit({"foo": "bar"}))
    daft_pd_df = daft_df.to_pandas()
    service_requests_csv_pd_df["literal_col"] = pd.Series(
        {"foo": "bar"} for _ in range(len(service_requests_csv_pd_df))
    )
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)


def test_literal_column_computation(daft_df, service_requests_csv_pd_df):
    """Creating a new column that is derived from (1 + other_column) and retrieving the top N results"""
    daft_df = daft_df.with_column(
        "literal_col", lit({"foo": "bar"}).apply(lambda d: d["foo"], return_dtype=DataType.string())
    )
    daft_pd_df = daft_df.to_pandas()
    service_requests_csv_pd_df["literal_col"] = "bar"
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)
