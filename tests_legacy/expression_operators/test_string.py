from __future__ import annotations

import pandas as pd

from daft import DataFrame
from daft.expressions import col
from tests.conftest import assert_df_equals


def test_string_contains():
    data = {"id": [str(i) for i in range(20)]}
    df = DataFrame.from_pydict(data)
    df = df.with_column("contains_1", col("id").str.contains("1"))
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["contains_1"] = pd_df["id"].str.contains("1")
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")


def test_string_endswith():
    data = {"id": [str(i) for i in range(20)]}
    df = DataFrame.from_pydict(data)
    df = df.with_column("ends_with_1", col("id").str.endswith("1"))
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["ends_with_1"] = pd_df["id"].str.endswith("1")
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")


def test_string_endswith():
    data = {"id": [str(i) for i in range(20)]}
    df = DataFrame.from_pydict(data)
    df = df.with_column("starts_with_1", col("id").str.startswith("1"))
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["starts_with_1"] = pd_df["id"].str.startswith("1")
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")


def test_string_length():
    data = {"id": [str(i) for i in range(20)]}
    df = DataFrame.from_pydict(data)
    df = df.with_column("str_len", col("id").str.length())
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["str_len"] = pd_df["id"].str.len()

    # Arrow kernels return results as int32
    pd_df["str_len"] = pd_df["str_len"].astype("int32")

    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")


def test_string_concat():
    data = {"id": [str(i) for i in range(20)]}
    df = DataFrame.from_pydict(data)
    df = df.with_column("str_concat", df["id"].str.concat("_foo"))
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["str_concat"] = pd_df["id"] + "_foo"
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")
