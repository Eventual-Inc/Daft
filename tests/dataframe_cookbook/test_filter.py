import pytest

from daft.expressions import col
from tests.conftest import assert_df_equals
from tests.dataframe_cookbook.conftest import parametrize_partitioned_daft_df

COL_SUBSET = ["Unique Key", "Complaint Type", "Borough", "Descriptor"]


@parametrize_partitioned_daft_df()
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
def test_filter(daft_df_ops, daft_df, pd_df):
    """Filter the dataframe, retrieve the top N results and select a subset of columns"""

    daft_noise_complaints = daft_df_ops(daft_df)

    pd_noise_complaints = pd_df[pd_df["Complaint Type"] == "Noise - Street/Sidewalk"][COL_SUBSET]
    assert_df_equals(daft_noise_complaints, pd_noise_complaints)


@parametrize_partitioned_daft_df()
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
def test_composite_filter(daft_df_ops, daft_df, pd_df):
    """Filter the dataframe with a complex filter and select a subset of columns"""
    daft_noise_complaints_brooklyn = daft_df_ops(daft_df)

    pd_noise_complaints_brooklyn = pd_df[
        (((pd_df["Complaint Type"] == "Noise - Street/Sidewalk") | (pd_df["Complaint Type"] == "Noise - Commercial")))
        & (pd_df["Borough"] == "BROOKLYN")
    ][COL_SUBSET]
    assert_df_equals(daft_noise_complaints_brooklyn, pd_noise_complaints_brooklyn)


@parametrize_partitioned_daft_df()
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
