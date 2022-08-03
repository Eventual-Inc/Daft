import pytest

from daft.expressions import col
from tests.dataframe_cookbook.conftest import assert_df_equals


@pytest.mark.parametrize(
    "daft_df_ops",
    [
        lambda daft_df: daft_df.with_column("unique_key_mod", col("Unique Key") + 1).limit(10),
        lambda daft_df: daft_df.limit(10).with_column("unique_key_mod", col("Unique Key") + 1),
    ],
)
def test_add_one_to_column(daft_df_ops, daft_df, pd_df):
    """Creating a new column that is derived from (1 + other_column) and retrieving the top N results"""
    pd_df["unique_key_mod"] = pd_df["Unique Key"] + 1
    assert_df_equals(daft_df_ops(daft_df), pd_df.head(10))


@pytest.mark.parametrize(
    "daft_df_ops",
    [
        lambda daft_df: daft_df.with_column("unique_key_mod", col("Unique Key") - col("Unique Key")).limit(10),
        lambda daft_df: daft_df.limit(10).with_column("unique_key_mod", col("Unique Key") - col("Unique Key")),
    ],
)
def test_difference_cols(daft_df_ops, daft_df, pd_df):
    """Creating a new column that is derived from 2 other columns and retrieving the top N results"""
    pd_df["unique_key_mod"] = pd_df["Unique Key"] - pd_df["Unique Key"]
    assert_df_equals(daft_df_ops(daft_df), pd_df.head(10))
