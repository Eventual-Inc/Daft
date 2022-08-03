import pytest

from daft.expressions import col
from tests.dataframe_cookbook.conftest import assert_df_equals, partitioned_daft_df

COL_SUBSET = ["Unique Key", "Complaint Type", "Borough", "Descriptor"]


@partitioned_daft_df("daft_df")
@pytest.mark.parametrize(
    "daft_df_ops",
    [
        # Select after the limit clause
        lambda daft_df: (
            daft_df.where(col("Complaint Type") == "Noise - Street/Sidewalk")
            .limit(10)
            .select(col("Unique Key"), col("Complaint Type"), col("Borough"), col("Descriptor"))
        ),
        # Select before the limit clause
        lambda daft_df: (
            daft_df.where(col("Complaint Type") == "Noise - Street/Sidewalk")
            .select(col("Unique Key"), col("Complaint Type"), col("Borough"), col("Descriptor"))
            .limit(10)
        ),
        # Select before the where clause
        lambda daft_df: (
            daft_df.select(col("Unique Key"), col("Complaint Type"), col("Borough"), col("Descriptor"))
            .where(col("Complaint Type") == "Noise - Street/Sidewalk")
            .limit(10)
        ),
    ],
)
def test_filter(daft_df_ops, daft_df, pd_df):
    """Filter the dataframe, retrieve the top N results and select a subset of columns"""

    daft_noise_complaints = daft_df_ops(daft_df)

    pd_noise_complaints = pd_df[pd_df["Complaint Type"] == "Noise - Street/Sidewalk"].head(10)[COL_SUBSET]
    assert_df_equals(daft_noise_complaints, pd_noise_complaints)


@partitioned_daft_df("daft_df")
@pytest.mark.parametrize(
    "daft_df_ops",
    [
        # Select after the Where clause
        lambda daft_df: (
            daft_df.where(
                ((col("Complaint Type") == "Noise - Street/Sidewalk") | (col("Complaint Type") == "Noise - Commercial"))
                & (col("Borough") == "BROOKLYN")
            ).select(col("Unique Key"), col("Complaint Type"), col("Borough"), col("Descriptor"))
        ),
        # Select before the Where clause
        lambda daft_df: (
            daft_df.select(col("Unique Key"), col("Complaint Type"), col("Borough"), col("Descriptor")).where(
                ((col("Complaint Type") == "Noise - Street/Sidewalk") | (col("Complaint Type") == "Noise - Commercial"))
                & (col("Borough") == "BROOKLYN")
            )
        ),
        # Flipped ordering of complex boolean expression
        lambda daft_df: (
            daft_df.select(col("Unique Key"), col("Complaint Type"), col("Borough"), col("Descriptor")).where(
                (col("Borough") == "BROOKLYN")
                & (
                    (col("Complaint Type") == "Noise - Street/Sidewalk")
                    | (col("Complaint Type") == "Noise - Commercial")
                )
            )
        ),
    ],
)
def test_composite_filter(daft_df_ops, daft_df, pd_df):
    """Filter the dataframe with a complex filter and select a subset of columns"""
    daft_noise_complaints_brooklyn = daft_df_ops(daft_df)

    pd_noise_complaints_brooklyn = pd_df[
        (((pd_df["Complaint Type"] == "Noise - Street/Sidewalk") | (pd_df["Complaint Type"] == "Noise - Commercial")))
        & (pd_df["Borough"] == "BROOKLYN")
    ][COL_SUBSET]
    assert_df_equals(daft_noise_complaints_brooklyn, pd_noise_complaints_brooklyn)


@partitioned_daft_df("daft_df")
@pytest.mark.parametrize(
    "daft_df_ops",
    [
        # Select after the where clauses
        lambda daft_df: (
            daft_df.where(col("Complaint Type") == "Noise - Street/Sidewalk")
            .where(col("Borough") == "BROOKLYN")
            .select(col("Unique Key"), col("Complaint Type"), col("Borough"), col("Descriptor"))
        ),
        # Select in between the where clauses
        lambda daft_df: (
            daft_df.where(col("Complaint Type") == "Noise - Street/Sidewalk")
            .select(col("Unique Key"), col("Complaint Type"), col("Borough"), col("Descriptor"))
            .where(col("Borough") == "BROOKLYN")
        ),
        # Select before the where clauses
        lambda daft_df: (
            daft_df.select(col("Unique Key"), col("Complaint Type"), col("Borough"), col("Descriptor"))
            .where(col("Complaint Type") == "Noise - Street/Sidewalk")
            .where(col("Borough") == "BROOKLYN")
        ),
    ],
)
def test_chained_filter(daft_df_ops, daft_df, pd_df):
    """Filter the dataframe with a chain of filters and select a subset of columns"""
    daft_noise_complaints_brooklyn = daft_df_ops(daft_df)

    pd_noise_complaints_brooklyn = pd_df
    pd_noise_complaints_brooklyn = pd_noise_complaints_brooklyn[
        pd_noise_complaints_brooklyn["Complaint Type"] == "Noise - Street/Sidewalk"
    ]
    pd_noise_complaints_brooklyn = pd_noise_complaints_brooklyn[pd_noise_complaints_brooklyn["Borough"] == "BROOKLYN"][
        COL_SUBSET
    ]
    assert_df_equals(daft_noise_complaints_brooklyn, pd_noise_complaints_brooklyn)
