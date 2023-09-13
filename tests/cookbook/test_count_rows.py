from __future__ import annotations

import pytest

from daft.expressions import col


def test_count_rows(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Count rows for the entire table"""
    daft_df_row_count = daft_df.repartition(repartition_nparts).count_rows()
    assert daft_df_row_count == service_requests_csv_pd_df.shape[0]


def test_filtered_count_rows(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Count rows on a table filtered by a certain condition"""
    daft_df_row_count = daft_df.repartition(repartition_nparts).where(col("Borough") == "BROOKLYN").count_rows()

    pd_df_row_count = len(service_requests_csv_pd_df[service_requests_csv_pd_df["Borough"] == "BROOKLYN"])
    assert daft_df_row_count == pd_df_row_count


@pytest.mark.parametrize(
    "keys",
    [
        pytest.param(["Borough"], id="NumGroupByKeys:1"),
        pytest.param(["Borough", "Complaint Type"], id="NumGroupByKeys:2"),
    ],
)
def test_groupby_count_rows(daft_df, service_requests_csv_pd_df, repartition_nparts, keys):
    """Count rows after group by"""
    daft_df = daft_df.repartition(repartition_nparts).groupby(*[col(k) for k in keys]).sum(col("Unique Key"))
    service_requests_csv_pd_df = service_requests_csv_pd_df.groupby(keys).sum("Unique Key").reset_index()
    assert daft_df.count_rows() == len(service_requests_csv_pd_df)


def test_dataframe_length_after_collect(daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Count rows after group by"""
    daft_df = daft_df.repartition(repartition_nparts).collect()
    assert len(daft_df) == len(service_requests_csv_pd_df)


def test_dataframe_length_before_collect(daft_df):
    """Count rows for the entire table"""
    with pytest.raises(RuntimeError):
        len(daft_df)
