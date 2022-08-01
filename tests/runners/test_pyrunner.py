from typing import Dict, List

from daft.dataframe import DataFrame
from daft.expressions import col
from daft.runners.pyrunner import PyRunner


def test_pyrunner(valid_data: List[Dict[str, float]]):
    df = DataFrame.from_csv("tests/assets/iris.csv")
    # df = df.select("sepal.length", "sepal.width").select((col("sepal.width") * col("sepal.length")).alias('area'))
    # df = df.with_column('area+1', col('area') == 20)
    df = df.select(col("sepal.length") == 10)
    print(df.plan())
    PyRunner(df.plan()).run()
