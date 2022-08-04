import pandas as pd
import pytest

from daft.dataframe import DataFrame
from daft.expressions import col


@pytest.mark.tdd_all
def test_pyrunner_with_pandas():

    # Pandas query
    pd_df = pd.read_csv("tests/assets/iris.csv")
    pd_df["area"] = pd_df["sepal.width"] * pd_df["sepal.length"]
    pd_df = pd_df[pd_df["area"] < 20]
    pd_df = pd_df[pd_df["variety"] == "Virginica"]
    pd_df = pd_df.sort_values(by=["area"], ascending=True)
    pd_df = pd_df.head(10)

    # Daft Query
    df = DataFrame.from_csv("tests/assets/iris.csv")
    df = df.with_column("area", col("sepal.width") * col("sepal.length"))
    df = df.repartition(3)
    df = df.where(col("area") < 20)
    df = df.where(col("variety") == "Virginica")
    df = df.sort(col("area"), desc=False)
    df = df.limit(10)
    df = df.collect()

    daft_pd_df = df.to_pandas()
    assert daft_pd_df.reset_index(drop=True).equals(pd_df.reset_index(drop=True))
