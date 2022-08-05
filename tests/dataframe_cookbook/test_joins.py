import pytest

from daft.expressions import col
from tests.dataframe_cookbook.conftest import (
    assert_df_equals,
    parametrize_partitioned_daft_df,
)


@pytest.mark.tdd
@parametrize_partitioned_daft_df()
def test_simple_join(daft_df, pd_df):
    daft_df_left = daft_df.select(col("Unique Key"), col("Borough"))
    daft_df_right = daft_df.select(col("Unique Key"), col("Created Date")).limit(25)
    daft_df = daft_df_left.join(daft_df_right, col("Unique Key"))

    pd_df_left = pd_df[["Unique Key", "Borough"]]
    pd_df_right = pd_df[["Unique Key", "Created Date"]]
    pd_df = pd_df_left.set_index("Unique Key").join(pd_df_right.set_index("Unique Key"), how="left").reset_index()
    assert_df_equals(daft_df, pd_df)


@pytest.mark.tdd
@parametrize_partitioned_daft_df()
def test_simple_join_missing_rvalues(daft_df, pd_df):
    daft_df_left = daft_df.select(col("Unique Key"), col("Borough"))
    daft_df_right = daft_df.select(col("Unique Key"), col("Created Date")).sort(col("Unique Key")).limit(25)
    daft_df = daft_df_left.join(daft_df_right, col("Unique Key"))

    pd_df_left = pd_df[["Unique Key", "Borough"]]
    pd_df_right = pd_df[["Unique Key", "Created Date"]].sort_values(by="Unique Key").head(25)
    pd_df = pd_df_left.set_index("Unique Key").join(pd_df_right.set_index("Unique Key"), how="left").reset_index()
    assert_df_equals(daft_df, pd_df)


@pytest.mark.tdd
@parametrize_partitioned_daft_df()
def test_simple_join_missing_lvalues(daft_df, pd_df):
    daft_df_left = daft_df.select(col("Unique Key"), col("Borough")).sort(col("Unique Key")).limit(25)
    daft_df_right = daft_df.select(col("Unique Key"), col("Created Date"))
    daft_df = daft_df_left.join(daft_df_right, col("Unique Key"))

    pd_df_left = pd_df[["Unique Key", "Borough"]].sort_values(by="Unique Key").head(25)
    pd_df_right = pd_df[["Unique Key", "Created Date"]]
    pd_df = pd_df_left.set_index("Unique Key").join(pd_df_right.set_index("Unique Key"), how="left").reset_index()
    assert_df_equals(daft_df, pd_df)
