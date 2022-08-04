import pytest

from daft.expressions import col
from tests.dataframe_cookbook.conftest import assert_df_equals, partitioned_daft_df


@partitioned_daft_df("daft_df")
def test_get_sorted_top_n(daft_df, pd_df):
    """Sort by a column and retrieve the top N results"""
    daft_sorted_df = daft_df.sort(col("Unique Key")).limit(10)

    assert_df_equals(
        daft_sorted_df,
        pd_df.sort_values(by="Unique Key").head(10),
        assert_ordering=True,
    )


@partitioned_daft_df("daft_df")
def test_sort_on_small_sample(daft_df, pd_df):
    """Sample the dataframe for N number of items and then sort it"""
    daft_df = daft_df.limit(10).sort(col("Created Date"))
    expected = pd_df.head(10).sort_values(by="Created Date")
    assert_df_equals(
        daft_df,
        expected,
        assert_ordering=True,
    )


@partitioned_daft_df("daft_df")
@pytest.mark.parametrize(
    "daft_df_ops",
    [
        # Select after limit
        lambda daft_df: daft_df.sort(col("Created Date")).limit(10).select(col("Created Date"), col("Complaint Type")),
        # Select before limit
        lambda daft_df: daft_df.sort(col("Created Date")).select(col("Created Date"), col("Complaint Type")).limit(10),
        # Select before the sort
        lambda daft_df: daft_df.select(col("Created Date"), col("Complaint Type")).sort(col("Created Date")).limit(10),
    ],
)
def test_get_sorted_top_n_projected(daft_df_ops, daft_df, pd_df):
    """Sort by a column and retrieve specific columns from the top N results"""
    expected = pd_df.sort_values(by="Created Date").head(10)[["Created Date", "Complaint Type"]]
    assert_df_equals(
        daft_df_ops(daft_df),
        expected,
        assert_ordering=True,
    )
