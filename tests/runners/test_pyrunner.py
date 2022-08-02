from typing import Dict, List

from daft.dataframe import DataFrame
from daft.expressions import col
from daft.runners.pyrunner import PyRunner


def test_pyrunner(valid_data: List[Dict[str, float]]):
    df = DataFrame.from_csv("tests/assets/iris.csv")
    df = df.with_column("area", col("sepal.width") * col("sepal.length"))
    df = df.where(col("area") < 20)
    df = df.where(col("variety") == "Virginica")
    df = df.sort(col("area"), col("sepal.width"), desc=True)
    df = df.limit(3)
    print(df.plan())
    PyRunner(df.plan()).run()
