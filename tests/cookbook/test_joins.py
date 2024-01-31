from __future__ import annotations

import pytest

from daft.expressions import col
from tests.conftest import assert_df_equals


@pytest.mark.parametrize(
    "join_strategy", [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"], indirect=True
)
def test_simple_join(join_strategy, daft_df, service_requests_csv_pd_df, repartition_nparts):
    daft_df = daft_df.repartition(repartition_nparts)
    daft_df_left = daft_df.select(col("Unique Key"), col("Borough"))
    daft_df_right = daft_df.select(col("Unique Key"), col("Created Date"))
    daft_df = daft_df_left.join(daft_df_right, col("Unique Key"), strategy=join_strategy)

    service_requests_csv_pd_df_left = service_requests_csv_pd_df[["Unique Key", "Borough"]]
    service_requests_csv_pd_df_right = service_requests_csv_pd_df[["Unique Key", "Created Date"]]
    service_requests_csv_pd_df = (
        service_requests_csv_pd_df_left.set_index("Unique Key")
        .join(service_requests_csv_pd_df_right.set_index("Unique Key"), how="inner")
        .reset_index()
    )
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)


@pytest.mark.parametrize(
    "join_strategy", [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"], indirect=True
)
def test_simple_self_join(join_strategy, daft_df, service_requests_csv_pd_df, repartition_nparts):
    daft_df = daft_df.repartition(repartition_nparts)
    daft_df = daft_df.select(col("Unique Key"), col("Borough"))

    daft_df = daft_df.join(daft_df, col("Unique Key"), strategy=join_strategy)

    service_requests_csv_pd_df = service_requests_csv_pd_df[["Unique Key", "Borough"]]
    service_requests_csv_pd_df = (
        service_requests_csv_pd_df.set_index("Unique Key")
        .join(service_requests_csv_pd_df.set_index("Unique Key"), how="inner", rsuffix="_right")
        .reset_index()
    )
    service_requests_csv_pd_df = service_requests_csv_pd_df.rename({"Borough_right": "right.Borough"}, axis=1)
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)


@pytest.mark.parametrize(
    "join_strategy", [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"], indirect=True
)
def test_simple_join_missing_rvalues(join_strategy, daft_df, service_requests_csv_pd_df, repartition_nparts):
    daft_df_right = daft_df.sort("Unique Key").limit(25).repartition(repartition_nparts)
    daft_df_left = daft_df.repartition(repartition_nparts)
    daft_df_left = daft_df_left.select(col("Unique Key"), col("Borough"))
    daft_df_right = daft_df_right.select(col("Unique Key"), col("Created Date")).sort(col("Unique Key"))
    daft_df = daft_df_left.join(daft_df_right, col("Unique Key"), strategy=join_strategy)

    service_requests_csv_pd_df_left = service_requests_csv_pd_df[["Unique Key", "Borough"]]
    service_requests_csv_pd_df_right = (
        service_requests_csv_pd_df[["Unique Key", "Created Date"]].sort_values(by="Unique Key").head(25)
    )
    service_requests_csv_pd_df = (
        service_requests_csv_pd_df_left.set_index("Unique Key")
        .join(service_requests_csv_pd_df_right.set_index("Unique Key"), how="inner")
        .reset_index()
    )
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)


@pytest.mark.parametrize(
    "join_strategy", [None, "hash", "sort_merge", "sort_merge_aligned_boundaries", "broadcast"], indirect=True
)
def test_simple_join_missing_lvalues(join_strategy, daft_df, service_requests_csv_pd_df, repartition_nparts):
    daft_df_right = daft_df.repartition(repartition_nparts)
    daft_df_left = daft_df.sort(col("Unique Key")).limit(25).repartition(repartition_nparts)
    daft_df_left = daft_df_left.select(col("Unique Key"), col("Borough"))
    daft_df_right = daft_df_right.select(col("Unique Key"), col("Created Date"))
    daft_df = daft_df_left.join(daft_df_right, col("Unique Key"), strategy=join_strategy)

    service_requests_csv_pd_df_left = (
        service_requests_csv_pd_df[["Unique Key", "Borough"]].sort_values(by="Unique Key").head(25)
    )
    service_requests_csv_pd_df_right = service_requests_csv_pd_df[["Unique Key", "Created Date"]]
    service_requests_csv_pd_df = (
        service_requests_csv_pd_df_left.set_index("Unique Key")
        .join(service_requests_csv_pd_df_right.set_index("Unique Key"), how="inner")
        .reset_index()
    )
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)
