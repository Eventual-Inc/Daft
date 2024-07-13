import numpy as np
from numpy.testing import assert_almost_equal

import daft


def test_math_tensors() -> None:
    np.random.seed(1)
    x = np.random.random((10, 10, 1))
    y = np.random.random((10, 10, 1))
    expected = x * y

    df = daft.from_pydict({"x": x, "y": y})
    df = df.with_column("x", df["x"].cast(daft.DataType.tensor(daft.DataType.float32(), (10, 1))))
    df = df.with_column("y", df["y"].cast(daft.DataType.tensor(daft.DataType.float32(), (10, 1))))

    result = df.with_column("z", df["x"] * df["y"]).collect().to_pydict()
    assert_almost_equal(result["z"], expected)
