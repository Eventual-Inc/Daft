from __future__ import annotations

import pytest

import daft
from daft.datatype import DataType
from daft.expressions import col, lit


def test_repr_cosine():
    a = col("a")
    b = col("b")
    y = a.embedding.cosine_distance(b)
    repr_out = repr(y)
    assert repr_out == "cosine_distance(col(a), col(b))"


def test_cosine():
    data = {
        "a": [[1.0, 1.11, 1.01], [2, 2, 2], [3, 3, 3]],
    }
    df = daft.from_pydict(data)
    query = [1.0, 1.11, 1.01]
    dtype = DataType.fixed_size_list(dtype=DataType.float64(), size=3)

    res = df.select(col("a").cast(dtype).embedding.cosine_distance(lit(query).cast(dtype))).collect()
    res = res.to_pydict()

    def cosine_dist_brute_force(x, y):
        import math

        xy = sum(xi * yi for xi, yi in zip(x, y))
        x_sq = math.sqrt(sum(xi**2 for xi in x))
        y_sq = math.sqrt(sum(yi**2 for yi in y))
        return 1.0 - xy / (x_sq * y_sq)

    expected = [cosine_dist_brute_force(query, x) for x in data["a"]]

    # check if they are approximately equal
    for a, b in zip(res["a"], expected):
        print("a:", a, "b:", b)
        assert pytest.approx(b) == pytest.approx(a, abs=1e-5)
