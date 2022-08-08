from daft.expressions import col
from tests.conftest import assert_df_equals
from tests.dataframe_cookbook.conftest import parametrize_partitioned_daft_df


@parametrize_partitioned_daft_df()
def test_add_one_to_column(daft_df, pd_df):
    """Creating a new column that is derived from (1 + other_column) and retrieving the top N results"""
    daft_df = daft_df.with_column("unique_key_mod", col("Unique Key") + 1)
    pd_df["unique_key_mod"] = pd_df["Unique Key"] + 1
    assert_df_equals(daft_df, pd_df)


@parametrize_partitioned_daft_df()
def test_difference_cols(daft_df, pd_df):
    """Creating a new column that is derived from 2 other columns and retrieving the top N results"""
    daft_df = daft_df.with_column("unique_key_mod", col("Unique Key") - col("Unique Key"))
    pd_df["unique_key_mod"] = pd_df["Unique Key"] - pd_df["Unique Key"]
    assert_df_equals(daft_df, pd_df)
