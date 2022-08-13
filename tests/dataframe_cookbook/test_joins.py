import pytest

from daft.expressions import col
from tests.conftest import assert_df_equals
from tests.dataframe_cookbook.conftest import (
    parametrize_service_requests_csv_daft_df,
    parametrize_service_requests_csv_repartition,
)


@pytest.mark.tdd
@parametrize_service_requests_csv_repartition
@parametrize_service_requests_csv_daft_df
def test_simple_join(daft_df, service_requests_csv_pd_df, repartition_nparts):
    daft_df = daft_df.repartition(repartition_nparts)
    daft_df_left = daft_df.select(col("Unique Key"), col("Borough"))
    daft_df_right = daft_df.select(col("Unique Key"), col("Created Date")).limit(25)
    daft_df = daft_df_left.join(daft_df_right, col("Unique Key"))

    service_requests_csv_pd_df_left = service_requests_csv_pd_df[["Unique Key", "Borough"]]
    service_requests_csv_pd_df_right = service_requests_csv_pd_df[["Unique Key", "Created Date"]]
    service_requests_csv_pd_df = (
        service_requests_csv_pd_df_left.set_index("Unique Key")
        .join(service_requests_csv_pd_df_right.set_index("Unique Key"), how="left")
        .reset_index()
    )
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)


@pytest.mark.tdd
@parametrize_service_requests_csv_repartition
@parametrize_service_requests_csv_daft_df
def test_simple_join_missing_rvalues(daft_df, service_requests_csv_pd_df, repartition_nparts):
    daft_df = daft_df.repartition(repartition_nparts)
    daft_df_left = daft_df.select(col("Unique Key"), col("Borough"))
    daft_df_right = daft_df.select(col("Unique Key"), col("Created Date")).sort(col("Unique Key")).limit(25)
    daft_df = daft_df_left.join(daft_df_right, col("Unique Key"))

    service_requests_csv_pd_df_left = service_requests_csv_pd_df[["Unique Key", "Borough"]]
    service_requests_csv_pd_df_right = (
        service_requests_csv_pd_df[["Unique Key", "Created Date"]].sort_values(by="Unique Key").head(25)
    )
    service_requests_csv_pd_df = (
        service_requests_csv_pd_df_left.set_index("Unique Key")
        .join(service_requests_csv_pd_df_right.set_index("Unique Key"), how="left")
        .reset_index()
    )
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)


@pytest.mark.tdd
@parametrize_service_requests_csv_repartition
@parametrize_service_requests_csv_daft_df
def test_simple_join_missing_lvalues(daft_df, service_requests_csv_pd_df, repartition_nparts):
    daft_df = daft_df.repartition(repartition_nparts)
    daft_df_left = daft_df.select(col("Unique Key"), col("Borough")).sort(col("Unique Key")).limit(25)
    daft_df_right = daft_df.select(col("Unique Key"), col("Created Date"))
    daft_df = daft_df_left.join(daft_df_right, col("Unique Key"))

    service_requests_csv_pd_df_left = (
        service_requests_csv_pd_df[["Unique Key", "Borough"]].sort_values(by="Unique Key").head(25)
    )
    service_requests_csv_pd_df_right = service_requests_csv_pd_df[["Unique Key", "Created Date"]]
    service_requests_csv_pd_df = (
        service_requests_csv_pd_df_left.set_index("Unique Key")
        .join(service_requests_csv_pd_df_right.set_index("Unique Key"), how="left")
        .reset_index()
    )
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, service_requests_csv_pd_df)
