from __future__ import annotations

import datetime

import pandas as pd

from daft import DataFrame
from daft.expressions import col
from tests.conftest import assert_df_equals


def test_datetime_year():
    data = {"dt": [datetime.date(1990 + i, i, i) for i in range(1, 13)]}
    df = DataFrame.from_pydict(data)
    df = df.with_column("year", col("dt").dt.year())
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["year"] = pd.to_datetime(pd_df["dt"]).dt.year
    assert_df_equals(df.to_pandas(), pd_df, sort_key="dt")


def test_datetime_month():
    data = {"dt": [datetime.date(1990 + i, i, i) for i in range(1, 13)]}
    df = DataFrame.from_pydict(data)
    df = df.with_column("month", col("dt").dt.month())
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["month"] = pd.to_datetime(pd_df["dt"]).dt.month
    assert_df_equals(df.to_pandas(), pd_df, sort_key="dt")


def test_datetime_day():
    data = {"dt": [datetime.date(1990 + i, i, i) for i in range(1, 13)]}
    df = DataFrame.from_pydict(data)
    df = df.with_column("day", col("dt").dt.day())
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["day"] = pd.to_datetime(pd_df["dt"]).dt.day
    assert_df_equals(df.to_pandas(), pd_df, sort_key="dt")


def test_datetime_day_of_week():
    data = {"dt": [datetime.date(1990 + i, i, i) for i in range(1, 13)]}
    df = DataFrame.from_pydict(data)
    df = df.with_column("day_of_week", col("dt").dt.day_of_week())
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["day_of_week"] = pd.to_datetime(pd_df["dt"]).dt.day_of_week
    assert_df_equals(df.to_pandas(), pd_df, sort_key="dt")
