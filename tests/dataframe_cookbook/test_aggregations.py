import pandas as pd
import pytest

from daft.expressions import col
from tests.dataframe_cookbook.conftest import (
    assert_df_equals,
    parametrize_partitioned_daft_df,
    parametrize_sort_desc,
)


@pytest.mark.tdd
@parametrize_partitioned_daft_df()
def test_sum(daft_df, pd_df):
    """Sums across an entire column for the entire table"""
    daft_df = daft_df.sum(col("Unique Key").alias("unique_key_sum"))
    pd_df = pd.DataFrame.from_records[{"unique_key_sum": [pd_df["Unique Key"].sum()]}]
    assert_df_equals(daft_df, pd_df)


@pytest.mark.tdd
@parametrize_partitioned_daft_df()
def test_mean(daft_df, pd_df):
    """Averages across a column in a sampling of the table"""
    daft_df = daft_df.mean(col("Unique Key").alias("unique_key_mean"))
    pd_df = pd.DataFrame.from_records[{"unique_key_mean": [pd_df["Unique Key"].mean()]}]
    assert_df_equals(daft_df, pd_df)


@pytest.mark.tdd
@parametrize_partitioned_daft_df()
def test_filtered_sum(daft_df, pd_df):
    """Sums across an entire column for the entire table filtered by a certain condition"""
    daft_df = daft_df.where(col("Borough") == "BROOKLYN").sum(col("Unique Key").alias("unique_key_sum"))
    pd_df = pd.DataFrame.from_records[{"unique_key_sum": [pd_df[pd_df["Borough"] == "BROOKLYN"]["Unique Key"].sum()]}]
    assert_df_equals(daft_df, pd_df)


@pytest.mark.tdd
@parametrize_partitioned_daft_df()
def test_sum_groupby(daft_df, pd_df):
    """Sums across groups"""
    daft_df = daft_df.groupby(col("Borough")).sum(col("Unique Key"))
    pd_df = pd_df.groupby("Borough").sum("Unique Key")
    assert_df_equals(daft_df, pd_df)


@pytest.mark.tdd
@parametrize_partitioned_daft_df()
@parametrize_sort_desc("sort_desc")
def test_sum_groupby_sorted(daft_df, sort_desc, pd_df):
    """Sums across groups"""
    daft_df = daft_df.groupby(col("Borough")).sum(col("Unique Key")).sort(col("Borough"), desc=sort_desc)
    pd_df = pd_df.groupby("Borough").sum("Unique Key").sort_values(by="Borough", ascending=not sort_desc)
    assert_df_equals(daft_df, pd_df, assert_ordering=True)
