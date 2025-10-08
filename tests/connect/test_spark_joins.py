from __future__ import annotations

import pytest

import daft
from tests.utils import sort_pydict


def spark_to_daft(spark_df):
    return daft.from_pandas(spark_df.toPandas())


@pytest.mark.parametrize("join_type", ["inner", "left", "right", "outer"])
def test_multicol_joins(join_type, make_spark_df):
    df = make_spark_df(
        {
            "A": [1, 2, 3],
            "B": ["a", "b", "c"],
            "C": [True, False, True],
        }
    )

    joined = df.join(df, on=["A", "B"], how=join_type)
    joined_data = spark_to_daft(joined).to_pydict()
    assert sort_pydict(joined_data, "A", "B", ascending=True) == {
        "A": [1, 2, 3],
        "B": ["a", "b", "c"],
        "C": [True, False, True],
        "right.C": [True, False, True],
    }


def test_columns_after_join(make_spark_df):
    df1 = make_spark_df(
        {
            "A": [1, 2, 3],
        },
    )

    df2 = make_spark_df({"A": [1, 2, 3], "B": [1, 2, 3]})

    joined_df1 = df1.join(df2, df1["A"] == df2["B"])
    joined_df2 = df1.join(df2, df1["A"] == df2["A"])
    joined_df1 = spark_to_daft(joined_df1)
    joined_df2 = spark_to_daft(joined_df2)

    assert set(joined_df1.schema().column_names()) == set(["A", "B", "right.A"])

    assert set(joined_df2.schema().column_names()) == set(["A", "B", "right.A"])


def test_using_columns(make_spark_df):
    df1 = make_spark_df(
        {
            "A": [1, 2, 3],
        },
    )
    df2 = make_spark_df({"A": [1, 2, 3], "B": [1, 2, 3]})

    joined_df = df1.join(df2, "A")
    joined_df = spark_to_daft(joined_df)

    assert set(joined_df.schema().column_names()) == set(["A", "B"])


def test_rename_join_keys_in_dataframe(make_spark_df):
    df1 = make_spark_df({"A": [1, 2], "B": [2, 2]})

    df2 = make_spark_df({"A": [1, 2]})
    joined_df1 = df1.join(df2, (df1["A"] == df2["A"]) & (df1["B"] == df2["A"]))
    joined_df2 = df1.join(df2, (df1["B"] == df2["A"]) & (df1["A"] == df2["A"]))
    joined_df1 = spark_to_daft(joined_df1)
    joined_df2 = spark_to_daft(joined_df2)

    assert set(joined_df1.schema().column_names()) == set(["A", "B", "right.A"])
    assert set(joined_df2.schema().column_names()) == set(["A", "B", "right.A"])


@pytest.mark.parametrize("join_type", ["inner", "left", "right", "outer"])
def test_dupes_join_key(join_type, make_spark_df):
    df = make_spark_df(
        {
            "A": [1, 1, 2, 2, 3, 3],
            "B": ["a", "b", "c", "d", "e", "f"],
        },
    )

    joined = df.join(df, on="A", how=join_type)
    joined = joined.sort(["A", "B", "right.B"])
    joined = spark_to_daft(joined)
    joined_data = joined.to_pydict()

    assert joined_data == {
        "A": [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3],
        "B": ["a", "a", "b", "b", "c", "c", "d", "d", "e", "e", "f", "f"],
        "right.B": ["a", "b", "a", "b", "c", "d", "c", "d", "e", "f", "e", "f"],
    }


@pytest.mark.parametrize("join_type", ["inner", "left", "right", "outer"])
def test_multicol_dupes_join_key(join_type, make_spark_df):
    df = make_spark_df(
        {
            "A": [1, 1, 2, 2, 3, 3],
            "B": ["a", "a", "b", "b", "c", "d"],
            "C": [1, 0, 1, 0, 1, 0],
        },
    )

    joined = df.join(df, on=["A", "B"], how=join_type)
    joined = joined.sort(["A", "B", "C", "right.C"])
    joined = spark_to_daft(joined)
    joined_data = joined.to_pydict()

    assert joined_data == {
        "A": [1, 1, 1, 1, 2, 2, 2, 2, 3, 3],
        "B": ["a"] * 4 + ["b"] * 4 + ["c", "d"],
        "C": [0, 0, 1, 1, 0, 0, 1, 1, 1, 0],
        "right.C": [0, 1, 0, 1, 0, 1, 0, 1, 1, 0],
    }


@pytest.mark.parametrize("join_type", ["inner", "left", "right", "outer"])
def test_joins_all_same_key(join_type, make_spark_df):
    df = make_spark_df(
        {
            "A": [1] * 4,
            "B": ["a", "b", "c", "d"],
        },
    )

    joined = df.join(df, on="A", how=join_type)
    joined = joined.sort(["A", "B", "right.B"])
    joined = spark_to_daft(joined)
    joined_data = joined.to_pydict()

    assert joined_data == {
        "A": [1] * 16,
        "B": ["a"] * 4 + ["b"] * 4 + ["c"] * 4 + ["d"] * 4,
        "right.B": ["a", "b", "c", "d"] * 4,
    }
