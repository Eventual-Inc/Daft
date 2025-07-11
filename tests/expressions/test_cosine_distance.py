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


valid_cosine_distance_dtypes = [
    # float64
    DataType.fixed_size_list(DataType.float64(), 3),
    DataType.embedding(DataType.float64(), 3),
    # float32
    DataType.fixed_size_list(DataType.float32(), 3),
    DataType.embedding(DataType.float32(), 3),
    # int8
    DataType.fixed_size_list(DataType.int8(), 3),
    DataType.embedding(DataType.int8(), 3),
]


@pytest.mark.parametrize("dtype", valid_cosine_distance_dtypes)
def test_cosine_floats(dtype):
    data = {
        "source": [[1.0, 1.11, 1.01], [2, 2, 2], [3, 3, 3]],
    }
    query = [1.0, 1.11, 1.01]

    df = daft.from_pydict(data)
    df = df.with_column("source", col("source").cast(dtype))
    df = df.with_column("query", lit(query).cast(dtype))

    res = df.select(col("source").embedding.cosine_distance(col("query"))).to_pydict()

    def cosine_dist_brute_force(x, y):
        import math

        xy = sum(xi * yi for xi, yi in zip(x, y))
        x_sq = math.sqrt(sum(xi**2 for xi in x))
        y_sq = math.sqrt(sum(yi**2 for yi in y))
        return 1.0 - xy / (x_sq * y_sq)

    # If dtype is int8, cast data and query to int
    if dtype.dtype.is_int8():
        expected = [cosine_dist_brute_force(list(map(int, query)), list(map(int, x))) for x in data["source"]]
    else:
        expected = [cosine_dist_brute_force(query, x) for x in data["source"]]

    # check if they are approximately equal
    for a, b in zip(res["source"], expected):
        assert pytest.approx(b) == pytest.approx(a, abs=1e-5)


@pytest.mark.parametrize("dtype", valid_cosine_distance_dtypes)
def test_pairwise_cosine_distance(dtype):
    data = {
        "e1": [[1, 2, 3], [1, 2, 3]],
        "e2": [[1, 2, 3], [-1, -2, -3]],
    }
    df = daft.from_pydict(data)
    res = df.with_column(
        "distance",
        df["e1"].cast(dtype).embedding.cosine_distance(df["e2"].cast(dtype)),
    ).to_pydict()

    assert res["distance"] == [0.0, 2.0]


@pytest.mark.parametrize(
    "data",
    [
        # Non fixed size list
        [[1, 2, 3], [1, 2, 3]],
        # integers
        [1, 2, 3],
        # floats
        [1.0, 2.0, 3.0],
        # strings
        ["1", "2", "3"],
    ],
)
def test_cosine_distance_dtype_not_fixed_size(data):
    data = {
        "source": data,
    }
    df = daft.from_pydict(data)
    with pytest.raises(ValueError, match="Expected source and query to be fixed size"):
        df.with_column("distance", df["source"].embedding.cosine_distance(df["source"]))


def test_cosine_distance_dtype_size_mismatch():
    data = {
        "source": [[1, 2, 3], [1, 2, 3]],
        "query": [[1, 2, 3, 4], [1, 2, 3, 4]],
    }

    df = daft.from_pydict(data)
    df = df.with_column("source", df["source"].cast(DataType.embedding(DataType.float32(), 3)))
    df = df.with_column("query", df["query"].cast(DataType.embedding(DataType.float32(), 4)))

    with pytest.raises(ValueError, match="Expected source and query to have the same size"):
        df.with_column("distance", df["source"].embedding.cosine_distance(df["query"]))


def test_cosine_distance_dtype_precision_mismatch():
    data = {
        "source": [[1, 2, 3], [1, 2, 3]],
        "query": [[1, 2, 3], [1, 2, 3]],
    }

    df = daft.from_pydict(data)
    df = df.with_column("source", df["source"].cast(DataType.embedding(DataType.float64(), 3)))
    df = df.with_column("query", df["query"].cast(DataType.embedding(DataType.float32(), 3)))

    with pytest.raises(
        ValueError,
        match="Expected source and query to have the same inner dtype",
    ):
        df.with_column("distance", df["source"].embedding.cosine_distance(df["query"]))
