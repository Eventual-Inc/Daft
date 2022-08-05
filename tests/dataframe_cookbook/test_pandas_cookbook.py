"""This module tests examples from https://pandas.pydata.org/docs/user_guide/cookbook.html"""

import numpy as np
import pandas as pd
import pytest

from daft.expressions import col, lit
from tests.dataframe_cookbook.conftest import (
    assert_df_equals,
    parametrize_partitioned_daft_df,
)

###
# Idioms: if-then
###

IF_THEN_DATA = {"AAA": [4, 5, 6, 7], "BBB": [10, 20, 30, 40], "CCC": [100, 50, -30, -50]}
IF_THEN_PARTITIONING = [1, 2, 3, 4, 5]


@pytest.mark.tdd_all("Requires In-Memory scans and Expression.where(...)")
@parametrize_partitioned_daft_df(source=IF_THEN_DATA, partitioning=IF_THEN_PARTITIONING)
def test_if_then(daft_df, pd_df):
    daft_df = daft_df.with_column("BBB", col("BBB").where(col("AAA") >= 5, -1))
    pd_df.loc[pd_df.AAA >= 5, "BBB"] = -1
    assert_df_equals(daft_df, pd_df, sort_key="AAA")


@pytest.mark.tdd_all("Requires In-Memory scans and Expression.where(...)")
@parametrize_partitioned_daft_df(source=IF_THEN_DATA)
def test_if_then_2_cols(daft_df, pd_df):
    daft_df = daft_df.with_column("BBB", col("BBB").where(col("AAA") >= 5, 2000)).with_column(
        "CCC",
        col("CCC").where(col("AAA") >= 5, 2000),
    )
    pd_df.loc[pd_df.AAA >= 5, ["BBB", "CCC"]] = 2000
    assert_df_equals(daft_df, pd_df, sort_key="AAA")


@pytest.mark.tdd_all("Requires In-Memory scans and Expression.where(...)")
@parametrize_partitioned_daft_df(source=IF_THEN_DATA)
def test_if_then_numpy_where(daft_df, pd_df):
    daft_df = daft_df.with_column("logic", lit("low").where(col("AAA") > 5, "high"))
    pd_df["logic"] = np.where(pd_df["AAA"] > 5, "high", "low")
    assert_df_equals(daft_df, pd_df, sort_key="AAA")


###
# Idioms: splitting
###

SPLITTING_DATA = {"AAA": [4, 5, 6, 7], "BBB": [10, 20, 30, 40], "CCC": [100, 50, -30, -50]}
SPLITTING_DATA_PARTITIONING = [1, 2, 3, 4, 5]


@pytest.mark.tdd_all("Requires In-Memory scans")
@parametrize_partitioned_daft_df(source=SPLITTING_DATA, partitioning=SPLITTING_DATA_PARTITIONING)
def test_split_frame_boolean_criterion(daft_df, pd_df):
    daft_df = daft_df.where(col("AAA") <= 5)
    pd_df = pd_df[pd_df.AAA <= 5]
    assert_df_equals(daft_df, pd_df, sort_key="AAA")


###
# Idioms: Building criteria
###

BUILDING_DATA = {"AAA": [4, 5, 6, 7], "BBB": [10, 20, 30, 40], "CCC": [100, 50, -30, -50]}
BUILDING_DATA_PARTITIONING = [1, 2, 3, 4, 5]


@pytest.mark.tdd_all("Requires In-Memory scans")
@parametrize_partitioned_daft_df(source=BUILDING_DATA, partitioning=BUILDING_DATA_PARTITIONING)
def test_multi_criteria_and(daft_df, pd_df):
    daft_df = daft_df.where((col("BBB") < 25) & (col("CCC") >= -40)).select(col("AAA"))
    pd_df = pd.DataFrame({"AAA": pd_df.loc[(pd_df["BBB"] < 25) & (pd_df["CCC"] >= -40), "AAA"]})
    assert_df_equals(daft_df, pd_df, sort_key="AAA")


@pytest.mark.tdd_all("Requires In-Memory scans")
@parametrize_partitioned_daft_df(source=BUILDING_DATA, partitioning=BUILDING_DATA_PARTITIONING)
def test_multi_criteria_or(daft_df, pd_df):
    daft_df = daft_df.where((col("BBB") > 25) | (col("CCC") >= -40)).select(col("AAA"))
    pd_df = pd.DataFrame({"AAA": pd_df.loc[(pd_df["BBB"] > 25) | (pd_df["CCC"] >= -40), "AAA"]})
    assert_df_equals(daft_df, pd_df, sort_key="AAA")


@pytest.mark.tdd_all("Requires In-Memory scans and Expression.where(...)")
@parametrize_partitioned_daft_df(source=BUILDING_DATA, partitioning=BUILDING_DATA_PARTITIONING)
def test_multi_criteria_or_assignment(daft_df, pd_df):
    daft_df = daft_df.with_column("AAA", col("AAA").where((col("BBB") > 25) | (col("CCC") >= 75), 0.1))
    pd_df.loc[(pd_df["BBB"] > 25) | (pd_df["CCC"] >= 75), "AAA"] = 0.1
    assert_df_equals(daft_df, pd_df, sort_key="AAA")


@pytest.mark.tdd_all("Requires In-Memory scans and Expression.abs()")
@parametrize_partitioned_daft_df(source=BUILDING_DATA, partitioning=BUILDING_DATA_PARTITIONING)
def test_select_rows_closest_to_certain_value_using_argsort(daft_df, pd_df):
    aValue = 43.0
    daft_df = daft_df.sort((col("CCC") - aValue).abs())
    pd_df = pd_df.loc[(pd_df.CCC - aValue).abs().argsort()]
    assert_df_equals(daft_df, pd_df, sort_key="AAA")


###
# Selection: Dataframes
###

SELECTION_DATA = {"AAA": [4, 5, 6, 7], "BBB": [10, 20, 30, 40], "CCC": [100, 50, -30, -50]}
SELECTION_DATA_PARTITIONING = [1, 2, 3, 4, 5]


@pytest.mark.tdd_all("Requires In-Memory scans, F.row_number() and Expression.is_in(...)")
@parametrize_partitioned_daft_df(source=SELECTION_DATA, partitioning=SELECTION_DATA_PARTITIONING)
def test_splitting_by_row_index(daft_df, pd_df):
    daft_df = daft_df.where((col("AAA") <= 6) & F.row_number().is_in([0, 2, 4]))
    pd_df = pd_df[(pd_df.AAA <= 6) & (pd_df.index.isin([0, 2, 4]))]
    assert_df_equals(daft_df, pd_df, sort_key="AAA")


@pytest.mark.tdd_all("Requires In-Memory scans and F.row_number()")
@parametrize_partitioned_daft_df(source=SELECTION_DATA, partitioning=SELECTION_DATA_PARTITIONING)
def test_splitting_by_row_range(daft_df, pd_df):
    daft_df = daft_df.where((F.row_number() >= 0) & (F.row_number() < 3))
    pd_df = pd_df[0:3]
    assert_df_equals(daft_df, pd_df, sort_key="AAA")


###
# Selection: New Columns
###

APPLYMAP_DATA = {"AAA": [1, 2, 1, 3], "BBB": [1, 1, 2, 2], "CCC": [2, 1, 3, 1]}
APPLYMAP_DATA_PARTITIONING = [1, 2, 3, 4, 5]


@pytest.mark.tdd_all("Requires In-Memory scans and Expression.applymap((val) => result)")
@parametrize_partitioned_daft_df(source=APPLYMAP_DATA, partitioning=APPLYMAP_DATA_PARTITIONING)
def test_efficiently_and_dynamically_creating_new_columns_using_applymap(daft_df, pd_df):
    source_cols = pd_df.columns
    categories = {1: "Alpha", 2: "Beta", 3: "Charlie"}
    new_cols = [str(x) + "_cat" for x in source_cols]

    for source_col, new_col in zip(source_cols, new_cols):
        daft_df = daft_df.with_column(new_col, col(source_col).applymap(lambda source_val: categories.get(source_val)))

    pd_df[new_cols] = pd_df[source_cols].applymap(categories.get)

    assert_df_equals(daft_df, pd_df, sort_key="AAA")


MIN_WITH_GROUPBY_DATA = {"AAA": [1, 1, 1, 2, 2, 2, 3, 3], "BBB": [2, 1, 3, 4, 5, 1, 2, 3]}
MIN_WITH_GROUPBY_DATA_PARTITIONING = [1, 2, 3, 8, 9]


@pytest.mark.tdd_all("Requires In-Memory scans, .groupby() and .min()")
@parametrize_partitioned_daft_df(source=MIN_WITH_GROUPBY_DATA, partitioning=MIN_WITH_GROUPBY_DATA_PARTITIONING)
def test_keep_other_columns_when_using_min_with_groupby(daft_df, pd_df):
    daft_df = daft_df.groupby(col("AAA")).min(col("BBB"))
    pd_df = pd_df.sort_values(by="BBB").groupby("AAA", as_index=False).first()
    assert_df_equals(daft_df, pd_df, sort_key="AAA")


###
# Missing Data: We do not support custom utilities for missing data at this time.
###

###
# Grouping: We do not support
#   1. .apply
#   2. .get_group
#   3. .transform
# Many of the tests in the Pandas cookbook are skipped here
###

GROUPBY_DATA = {
    "animal": "cat dog cat fish dog cat cat".split(),
    "size": list("SSMMMLL"),
    "weight": [8, 10, 11, 1, 20, 12, 12],
    "adult": [False] * 5 + [True] * 2,
}
GROUPBY_DATA_PARTITIONING = [1, 2, 7, 8]

# TODO: We can't do this yet, since it requires getting the first row for every Group, ordered by some column
# @pytest.mark.tdd
# @parametrize_partitioned_daft_df(source=GROUPBY_DATA, partitioning=GROUPBY_DATA_PARTITIONING)
# def test_basic_grouping_with_apply(daft_df, pd_df):
#     """Gets the sizes of the animals with the highest weight"""
#     pd_df = pd.DataFrame(pd_df.groupby("animal").apply(lambda subf: subf["size"][subf["weight"].idxmax()]), columns="size").reset_index()
#     assert_df_equals(daft_df, pd_df, sort_key="animal")


@pytest.mark.tdd_all("Requires In-Memory scans, Expression.where(), .groupby(), .agg() and Expression.agg.*")
@parametrize_partitioned_daft_df(source=GROUPBY_DATA, partitioning=GROUPBY_DATA_PARTITIONING)
def test_applying_to_different_items_in_group(daft_df, pd_df):
    daft_df = daft_df.with_column("weight", col("weight").where(col("size") == "S", col("weight") * 1.5))
    daft_df = daft_df.with_column("weight", col("weight").where(col("size") == "M", col("weight") * 1.25))
    daft_df = daft_df.with_column("weight", col("weight").where(col("size") == "L", col("weight")))
    daft_df = daft_df.groupby(col("animal")).agg(
        col("weight").agg.mean(), lit("L").alias("size"), lit(True).alias("adult")
    )

    def GrowUp(x):
        avg_weight = sum(x[x["size"] == "S"].weight * 1.5)
        avg_weight += sum(x[x["size"] == "M"].weight * 1.25)
        avg_weight += sum(x[x["size"] == "L"].weight)
        avg_weight /= len(x)
        return pd.Series(["L", avg_weight, True], index=["size", "weight", "adult"])

    pd_df = pd_df.groupby("animal").apply(GrowUp)
    assert_df_equals(daft_df, pd_df, sort_key="animal")


###
# Merge
###

JOIN_DATA = {
    "Area": ["A"] * 5 + ["C"] * 2,
    "Bins": [110] * 2 + [160] * 3 + [40] * 2,
    "Test_0": [0, 1, 0, 1, 2, 0, 1],
    "Data": np.random.randn(7),
}
JOIN_DATA_PARTITIONING = [1, 3, 7, 8]


@pytest.mark.tdd_all("Requires In-Memory scans and multi-column joins")
@parametrize_partitioned_daft_df(source=JOIN_DATA, partitioning=JOIN_DATA_PARTITIONING)
def test_self_join(daft_df, pd_df):
    daft_df = daft_df.with_column("Test_1", col("Test_0") - 1).with_column("key", col("Test_0"))
    daft_df_right = daft_df.with_column("key", col("Test_1"))
    daft_df = daft_df.join(daft_df_right, [col("Bins"), col("Area"), col("key")], left_suffix="_L", right_suffix="_R")

    pd_df["Test_1"] = pd_df["Test_0"] - 1
    pd_df = pd.merge(
        pd_df,
        pd_df,
        left_on=["Bins", "Area", "Test_0"],
        right_on=["Bins", "Area", "Test_1"],
        suffixes=("_L", "_R"),
    )
    #   Area  Bins  Test_0_L    Data_L  Test_1_L  Test_0_R    Data_R  Test_1_R
    # 0    A   110         0 -0.433937        -1         1 -0.160552         0
    # 1    A   160         0  0.744434        -1         1  1.754213         0
    # 2    A   160         1  1.754213         0         2  0.000850         1
    # 3    C    40         0  0.342243        -1         1  1.070599         0
    assert_df_equals(daft_df, pd_df, sort_key="Data_L")
