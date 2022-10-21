from __future__ import annotations

import pytest

from daft.expressions import col
from tests.conftest import assert_df_equals
from tests.dataframe_cookbook.conftest import (
    parametrize_service_requests_csv_repartition,
)

COL_SUBSET = ["Unique Key", "Complaint Type", "Borough", "Descriptor"]


@parametrize_service_requests_csv_repartition
@pytest.mark.parametrize(
    "daft_df_ops",
    [
        pytest.param(
            lambda daft_df: (
                daft_df.where(col("Complaint Type") == "Noise - Street/Sidewalk").select(
                    col("Unique Key"), col("Complaint Type"), col("Borough"), col("Descriptor")
                )
            ),
            id="where..select",
        ),
        pytest.param(
            lambda daft_df: (
                daft_df.select(col("Unique Key"), col("Complaint Type"), col("Borough"), col("Descriptor")).where(
                    col("Complaint Type") == "Noise - Street/Sidewalk"
                )
            ),
            id="select..where",
        ),
    ],
)
def test_filter(daft_df_ops, daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Filter the dataframe, retrieve the top N results and select a subset of columns"""

    daft_noise_complaints = daft_df_ops(daft_df.repartition(repartition_nparts))

    pd_noise_complaints = service_requests_csv_pd_df[
        service_requests_csv_pd_df["Complaint Type"] == "Noise - Street/Sidewalk"
    ][COL_SUBSET]
    daft_pd_df = daft_noise_complaints.to_pandas()
    assert_df_equals(daft_pd_df, pd_noise_complaints)


@parametrize_service_requests_csv_repartition
@pytest.mark.parametrize(
    "daft_df_ops",
    [
        pytest.param(
            lambda daft_df: (
                daft_df.where(
                    (
                        (col("Complaint Type") == "Noise - Street/Sidewalk")
                        | (col("Complaint Type") == "Noise - Commercial")
                    )
                    & (col("Borough") == "BROOKLYN")
                ).select(col("Unique Key"), col("Complaint Type"), col("Borough"), col("Descriptor"))
            ),
            id="where..select",
        ),
        pytest.param(
            lambda daft_df: (
                daft_df.select(col("Unique Key"), col("Complaint Type"), col("Borough"), col("Descriptor")).where(
                    (
                        (col("Complaint Type") == "Noise - Street/Sidewalk")
                        | (col("Complaint Type") == "Noise - Commercial")
                    )
                    & (col("Borough") == "BROOKLYN")
                )
            ),
            id="select..where",
        ),
        pytest.param(
            lambda daft_df: (
                daft_df.select(col("Unique Key"), col("Complaint Type"), col("Borough"), col("Descriptor")).where(
                    (col("Borough") == "BROOKLYN")
                    & (
                        (col("Complaint Type") == "Noise - Street/Sidewalk")
                        | (col("Complaint Type") == "Noise - Commercial")
                    )
                )
            ),
            id="select..where(flipped&)",
        ),
    ],
)
def test_complex_filter(daft_df_ops, daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Filter the dataframe with a complex filter and select a subset of columns"""
    daft_noise_complaints_brooklyn = daft_df_ops(daft_df.repartition(repartition_nparts))

    pd_noise_complaints_brooklyn = service_requests_csv_pd_df[
        (
            (service_requests_csv_pd_df["Complaint Type"] == "Noise - Street/Sidewalk")
            | (service_requests_csv_pd_df["Complaint Type"] == "Noise - Commercial")
        )
        & (service_requests_csv_pd_df["Borough"] == "BROOKLYN")
    ][COL_SUBSET]
    daft_pd_df = daft_noise_complaints_brooklyn.to_pandas()
    assert_df_equals(daft_pd_df, pd_noise_complaints_brooklyn)


@parametrize_service_requests_csv_repartition
@pytest.mark.parametrize(
    "daft_df_ops",
    [
        pytest.param(
            lambda daft_df: (
                daft_df.where(col("Complaint Type") == "Noise - Street/Sidewalk")
                .where(col("Borough") == "BROOKLYN")
                .select(col("Unique Key"), col("Complaint Type"), col("Borough"), col("Descriptor"))
            ),
            id="where..where..select",
        ),
        pytest.param(
            lambda daft_df: (
                daft_df.where(col("Complaint Type") == "Noise - Street/Sidewalk")
                .select(col("Unique Key"), col("Complaint Type"), col("Borough"), col("Descriptor"))
                .where(col("Borough") == "BROOKLYN")
            ),
            id="where..select..where",
        ),
        pytest.param(
            lambda daft_df: (
                daft_df.select(col("Unique Key"), col("Complaint Type"), col("Borough"), col("Descriptor"))
                .where(col("Complaint Type") == "Noise - Street/Sidewalk")
                .where(col("Borough") == "BROOKLYN")
            ),
            id="select..where..where",
        ),
    ],
)
def test_chain_filter(daft_df_ops, daft_df, service_requests_csv_pd_df, repartition_nparts):
    """Filter the dataframe with a chain of filters and select a subset of columns"""
    daft_noise_complaints_brooklyn = daft_df_ops(daft_df.repartition(repartition_nparts))

    pd_noise_complaints_brooklyn = service_requests_csv_pd_df
    pd_noise_complaints_brooklyn = pd_noise_complaints_brooklyn[
        pd_noise_complaints_brooklyn["Complaint Type"] == "Noise - Street/Sidewalk"
    ]
    pd_noise_complaints_brooklyn = pd_noise_complaints_brooklyn[pd_noise_complaints_brooklyn["Borough"] == "BROOKLYN"][
        COL_SUBSET
    ]
    daft_pd_df = daft_noise_complaints_brooklyn.to_pandas()
    assert_df_equals(daft_pd_df, pd_noise_complaints_brooklyn)
