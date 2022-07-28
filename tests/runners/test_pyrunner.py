from typing import Dict, List

from daft.dataframe import DataFrame
from daft.runners.pyrunner import PyRunner


def test_pyrunner(valid_data: List[Dict[str, float]]):
    df = DataFrame.from_pylist(valid_data)
    df = df.select("sepal_length", "sepal_width").select("sepal_width")
    PyRunner(df.plan()).run()
