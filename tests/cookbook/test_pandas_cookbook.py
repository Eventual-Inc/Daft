"""This module tests examples from https://pandas.pydata.org/docs/user_guide/cookbook.html"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import daft
from daft.datatype import DataType
from daft.expressions import col, lit
from tests.conftest import assert_df_equals

###
# Idioms: if-then
###

IF_THEN_DATA = {"AAA": [4, 5, 6, 7], "BBB": [10, 20, 30, 40], "CCC": [100, 50, -30, -50]}


def test_if_then(repartition_nparts):
    daft_df = daft.from_pydict(IF_THEN_DATA).repartition(repartition_nparts)
    pd_df = pd.DataFrame.from_dict(IF_THEN_DATA)
    daft_df = daft_df.with_column("BBB", (col("AAA") >= 5).if_else(-1, col("BBB")))
    pd_df.loc[pd_df.AAA >= 5, "BBB"] = -1
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="AAA")


def test_if_then_2_cols(repartition_nparts):
    daft_df = daft.from_pydict(IF_THEN_DATA).repartition(repartition_nparts)
    pd_df = pd.DataFrame.from_dict(IF_THEN_DATA)
    daft_df = daft_df.with_column("BBB", (col("AAA") >= 5).if_else(2000, col("BBB"))).with_column(
        "CCC",
        (col("AAA") >= 5).if_else(2000, col("CCC")),
    )
    pd_df.loc[pd_df.AAA >= 5, ["BBB", "CCC"]] = 2000
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="AAA")


def test_if_then_numpy_where(repartition_nparts):
    daft_df = daft.from_pydict(IF_THEN_DATA).repartition(repartition_nparts)
    pd_df = pd.DataFrame.from_dict(IF_THEN_DATA)
    daft_df = daft_df.with_column("logic", (col("AAA") > 5).if_else("high", lit("low")))
    pd_df["logic"] = np.where(pd_df["AAA"] > 5, "high", "low")
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="AAA")


###
# Idioms: splitting
###

SPLITTING_DATA = {"AAA": [4, 5, 6, 7], "BBB": [10, 20, 30, 40], "CCC": [100, 50, -30, -50]}


def test_split_frame_boolean_criterion(repartition_nparts):
    daft_df = daft.from_pydict(SPLITTING_DATA).repartition(repartition_nparts)
    pd_df = pd.DataFrame.from_dict(SPLITTING_DATA)
    daft_df = daft_df.where(col("AAA") <= 5)
    pd_df = pd_df[pd_df.AAA <= 5]
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="AAA")


###
# Idioms: Building criteria
###

BUILDING_DATA = {"AAA": [4, 5, 6, 7], "BBB": [10, 20, 30, 40], "CCC": [100, 50, -30, -50]}


def test_multi_criteria_and(repartition_nparts):
    daft_df = daft.from_pydict(BUILDING_DATA).repartition(repartition_nparts)
    pd_df = pd.DataFrame.from_dict(BUILDING_DATA)
    daft_df = daft_df.where((col("BBB") < 25) & (col("CCC") >= -40)).select(col("AAA"))
    pd_df = pd.DataFrame({"AAA": pd_df.loc[(pd_df["BBB"] < 25) & (pd_df["CCC"] >= -40), "AAA"]})
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="AAA")


def test_multi_criteria_or(repartition_nparts):
    daft_df = daft.from_pydict(BUILDING_DATA).repartition(repartition_nparts)
    pd_df = pd.DataFrame.from_dict(BUILDING_DATA)
    daft_df = daft_df.where((col("BBB") > 25) | (col("CCC") >= -40)).select(col("AAA"))
    pd_df = pd.DataFrame({"AAA": pd_df.loc[(pd_df["BBB"] > 25) | (pd_df["CCC"] >= -40), "AAA"]})
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="AAA")


def test_multi_criteria_or_assignment(repartition_nparts):
    daft_df = daft.from_pydict(BUILDING_DATA).repartition(repartition_nparts)
    pd_df = pd.DataFrame.from_dict(BUILDING_DATA)
    daft_df = daft_df.with_column(
        "AAA", ((col("BBB") > 25) | (col("CCC") >= 75)).if_else(0.1, col("AAA").cast(DataType.float32()))
    )
    pd_df.loc[(pd_df["BBB"] > 25) | (pd_df["CCC"] >= 75), "AAA"] = 0.1
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="BBB")


def test_select_rows_closest_to_certain_value_using_argsort(repartition_nparts):
    daft_df = daft.from_pydict(BUILDING_DATA).repartition(repartition_nparts)
    pd_df = pd.DataFrame.from_dict(BUILDING_DATA)
    aValue = 43.0
    daft_df = daft_df.sort(abs(col("CCC") - aValue))
    pd_df = pd_df.loc[(pd_df.CCC - aValue).abs().argsort()]
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="AAA")


###
# Selection: Dataframes
###

SELECTION_DATA = {"AAA": [4, 5, 6, 7], "BBB": [10, 20, 30, 40], "CCC": [100, 50, -30, -50]}


@pytest.mark.skip(reason="Requires F.row_number() and Expression.is_in(...)")
def test_splitting_by_row_index(repartition_nparts):
    daft_df = daft.from_pydict(SELECTION_DATA).repartition(repartition_nparts)
    pd_df = pd.DataFrame.from_dict(SELECTION_DATA)
    daft_df = daft_df.where((col("AAA") <= 6) & F.row_number().is_in([0, 2, 4]))  # noqa: F821
    pd_df = pd_df[(pd_df.AAA <= 6) & (pd_df.index.isin([0, 2, 4]))]
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="AAA")


@pytest.mark.skip(reason="Requires F.row_number()")
def test_splitting_by_row_range(repartition_nparts):
    daft_df = daft.from_pydict(SELECTION_DATA).repartition(repartition_nparts)
    pd_df = pd.DataFrame.from_dict(SELECTION_DATA)
    daft_df = daft_df.where((F.row_number() >= 0) & (F.row_number() < 3))  # noqa: F821
    pd_df = pd_df[0:3]
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="AAA")


###
# Selection: New Columns
###

APPLYMAP_DATA = {"AAA": [1, 2, 1, 3], "BBB": [1, 1, 2, 2], "CCC": [2, 1, 3, 1]}


@pytest.mark.skip(reason="Requires Expression.applymap((val) => result)")
def test_efficiently_and_dynamically_creating_new_columns_using_applymap(repartition_nparts):
    daft_df = daft.from_pydict(APPLYMAP_DATA).repartition(repartition_nparts)
    pd_df = pd.DataFrame.from_dict(APPLYMAP_DATA)
    source_cols = pd_df.columns
    categories = {1: "Alpha", 2: "Beta", 3: "Charlie"}
    new_cols = [str(x) + "_cat" for x in source_cols]

    for source_col, new_col in zip(source_cols, new_cols):
        daft_df = daft_df.with_column(new_col, col(source_col).applymap(lambda source_val: categories.get(source_val)))

    pd_df[new_cols] = pd_df[source_cols].applymap(categories.get)

    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="AAA")


MIN_WITH_GROUPBY_DATA = {"AAA": [1, 1, 1, 2, 2, 2, 3, 3], "BBB": [2, 1, 3, 4, 5, 1, 2, 3]}


@pytest.mark.skip(reason="Requires .first() aggregations")
def test_keep_other_columns_when_using_min_with_groupby(repartition_nparts):
    daft_df = daft.from_pydict(APPLYMAP_DATA).repartition(repartition_nparts)
    pd_df = pd.DataFrame.from_dict(APPLYMAP_DATA)
    daft_df = daft_df.groupby(col("AAA")).min(col("BBB"))
    pd_df = pd_df.sort_values(by="BBB").groupby("AAA", as_index=False).first()
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="AAA")


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

# TODO: We can't do this yet, since it requires getting the first row for every Group, ordered by some column
# @pytest.mark.tdd
# @parametrize_partitioned_daft_df(source=GROUPBY_DATA, partitioning=GROUPBY_DATA_PARTITIONING)
# def test_basic_grouping_with_apply(repartition_nparts):
#     """Gets the sizes of the animals with the highest weight"""
#     pd_df = pd.DataFrame(pd_df.groupby("animal").apply(lambda subf: subf["size"][subf["weight"].idxmax()]), columns="size").reset_index()
#     assert_df_equals(daft_df, pd_df, sort_key="animal")


def test_applying_to_different_items_in_group(repartition_nparts):
    daft_df = daft.from_pydict(GROUPBY_DATA).repartition(repartition_nparts)
    pd_df = pd.DataFrame.from_dict(GROUPBY_DATA)
    daft_df = daft_df.with_column(
        "weight", (col("size") == "S").if_else(col("weight") * 1.5, col("weight").cast(DataType.float32()))
    )
    daft_df = daft_df.with_column("weight", (col("size") == "M").if_else(col("weight") * 1.25, col("weight")))
    daft_df = daft_df.with_column("weight", (col("size") == "L").if_else(col("weight"), col("weight")))
    daft_df = daft_df.groupby(col("animal")).agg(col("weight").mean())
    daft_df = daft_df.with_column("size", lit("L"))
    daft_df = daft_df.with_column("adult", lit(True))

    def GrowUp(x):
        avg_weight = sum(x[x["size"] == "S"].weight * 1.5)
        avg_weight += sum(x[x["size"] == "M"].weight * 1.25)
        avg_weight += sum(x[x["size"] == "L"].weight)
        avg_weight /= len(x)
        return pd.Series(["L", avg_weight, True], index=["size", "weight", "adult"])

    pd_df = pd_df.groupby("animal").apply(GrowUp).reset_index()
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="animal")


###
# Merge
###

JOIN_DATA = {
    "Area": ["A"] * 5 + ["C"] * 2,
    "Bins": [110] * 2 + [160] * 3 + [40] * 2,
    "Test_0": [0, 1, 0, 1, 2, 0, 1],
    "Data": np.random.randn(7),
}


def test_self_join(repartition_nparts):
    daft_df = daft.from_pydict(JOIN_DATA).repartition(repartition_nparts)
    daft_df = daft_df.with_column("Test_1", col("Test_0") - 1)
    daft_df = daft_df.join(
        daft_df, left_on=[col("Bins"), col("Area"), col("Test_0")], right_on=[col("Bins"), col("Area"), col("Test_1")]
    )
    daft_pd_df = daft_df.to_pandas()

    pd_df = pd.DataFrame.from_dict(JOIN_DATA)
    pd_df["Test_1"] = pd_df["Test_0"] - 1
    pd_df = pd.merge(
        pd_df,
        pd_df,
        left_on=["Bins", "Area", "Test_0"],
        right_on=["Bins", "Area", "Test_1"],
        suffixes=("", "_R"),
    )
    pd_df = pd_df.rename({"Test_0_R": "right.Test_0", "Data_R": "right.Data", "Test_1_R": "right.Test_1"}, axis=1)
    #   Area  Bins  Test_0_L    Data_L  Test_1_L  Test_0_R    Data_R  Test_1_R
    # 0    A   110         0 -0.433937        -1         1 -0.160552         0
    # 1    A   160         0  0.744434        -1         1  1.754213         0
    # 2    A   160         1  1.754213         0         2  0.000850         1
    # 3    C    40         0  0.342243        -1         1  1.070599         0
    assert_df_equals(daft_pd_df, pd_df, sort_key="Data")
