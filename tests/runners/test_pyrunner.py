from typing import Dict, List

from daft.dataframe import DataFrame
from daft.runners.pyrunner import PyRunner


def test_pyrunner(valid_data: List[Dict[str, float]]):
    df = DataFrame.from_csv("/Users/sammy/iris.csv")
    df = df.select("sepal.length", "sepal.width").select("sepal.width")
    PyRunner(df.plan()).run()
