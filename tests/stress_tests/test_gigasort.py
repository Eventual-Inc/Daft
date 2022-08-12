import numpy as np

from daft.dataframe import DataFrame


def test_gigasort() -> None:
    df = DataFrame.from_pydict({"x": np.random.randint(0, 10000, 100_000_000)})

    df = df.repartition(20)
    df = df.sort("x")
    df.collect()
