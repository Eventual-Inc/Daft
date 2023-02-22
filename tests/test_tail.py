from __future__ import annotations

import pandas as pd
import pytest

from daft.dataframe import DataFrame
from daft.expressions import col
from tests.assets.assets import IRIS_CSV


@pytest.mark.parametrize("tail", [True, False])
@pytest.mark.parametrize("num", [10, 5, 2, 1, 0])
def test_tail_simple(tail, num):
    df = DataFrame.from_pydict({"A": [1, 2, 3, 4, 5]})
    daft_tail = df.limit(num, tail=tail).to_pandas()
    if tail:
        expected = df.to_pandas().tail(num)
    else:
        expected = df.to_pandas().head(num)
    assert daft_tail.reset_index(drop=True).equals(expected.reset_index(drop=True)), f"\n{daft_tail} \n!= \n{expected}"


@pytest.mark.parametrize("tail", [True, False])
@pytest.mark.parametrize("num", [100, 10, 1, 0])
def test_tail_complex_plan(tail, num):

    # Query under test
    df = DataFrame.read_csv(IRIS_CSV)
    df = df.with_column("area", col("sepal.width") * col("sepal.length"))
    df = df.repartition(3, "variety")
    df = df.where(col("area") < 20)
    df = df.where(col("variety") == "Virginica")
    df = df.sort([col("area"), col("sepal.length")])
    df = df.limit(num, tail=tail)
    df = df.collect()
    df = df.to_pandas().reset_index(drop=True)

    # Expected
    pd_df = pd.read_csv(IRIS_CSV)
    pd_df["area"] = pd_df["sepal.width"] * pd_df["sepal.length"]
    pd_df = pd_df[pd_df["area"] < 20]
    pd_df = pd_df[pd_df["variety"] == "Virginica"]
    pd_df = pd_df.sort_values(by=["area", "sepal.length"], ascending=True)
    if tail:
        pd_df = pd_df.tail(num)
    else:
        pd_df = pd_df.head(num)
    pd_df = pd_df.reset_index(drop=True)

    assert df.equals(pd_df), f"\n{df} \n!= \n{pd_df}"
