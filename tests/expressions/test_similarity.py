from __future__ import annotations

import itertools
import math

import pytest

import daft
from daft.datatype import DataType
from daft.expressions import col, lit


def test_repr_similarity():
    a = col("a")
    b = col("b")

    assert repr(a.cosine_similarity(b)) == "cosine_similarity(col(a), col(b))"
    assert repr(a.pearson_correlation(b)) == "pearson_correlation(col(a), col(b))"
    assert repr(a.jaccard_similarity(b)) == "jaccard_similarity(col(a), col(b))"


def _generate_valid_dtype_pairs_for_similarity():
    valid_inner_dtypes = [DataType.float64(), DataType.float32(), DataType.int8()]
    pairs = []

    for inner_dtype in valid_inner_dtypes:
        outer_dtypes = [
            DataType.fixed_size_list(inner_dtype, 3),
            DataType.embedding(inner_dtype, 3),
        ]
        pairs.extend(itertools.product(outer_dtypes, outer_dtypes))

    return pairs


valid_similarity_dtype_pairs = _generate_valid_dtype_pairs_for_similarity()


@pytest.mark.parametrize("dtype_pair", valid_similarity_dtype_pairs)
def test_cosine_similarity(dtype_pair):
    dtype1, dtype2 = dtype_pair
    data = {
        "source": [[1, 2, 3], [1, 0, 0]],
    }
    query = [1, 2, 3]

    df = daft.from_pydict(data)
    df = df.with_column("source", col("source").cast(dtype1))
    df = df.with_column("query", lit(query).cast(dtype2))

    res = df.select(col("source").cosine_similarity(col("query"))).to_pydict()["source"]

    def cosine_sim(x, y):
        dot = sum(a * b for a, b in zip(x, y))
        denom = math.sqrt(sum(a * a for a in x)) * math.sqrt(sum(b * b for b in y))
        return dot / denom

    expected = [cosine_sim(query, row) for row in data["source"]]

    for actual, exp in zip(res, expected):
        assert pytest.approx(exp) == pytest.approx(actual, abs=1e-5)


@pytest.mark.parametrize("dtype_pair", valid_similarity_dtype_pairs)
def test_pearson_correlation(dtype_pair):
    dtype1, dtype2 = dtype_pair
    data = {
        "source": [[1, 2, 3], [3, 2, 1]],
    }
    query = [1, 2, 3]

    df = daft.from_pydict(data)
    df = df.with_column("source", col("source").cast(dtype1))
    df = df.with_column("query", lit(query).cast(dtype2))

    res = df.select(col("source").pearson_correlation(col("query"))).to_pydict()["source"]

    def pearson(x, y):
        mean_x = sum(x) / len(x)
        mean_y = sum(y) / len(y)
        num = sum((a - mean_x) * (b - mean_y) for a, b in zip(x, y))
        denom = math.sqrt(sum((a - mean_x) ** 2 for a in x) * sum((b - mean_y) ** 2 for b in y))
        return num / denom

    expected = [pearson(query, row) for row in data["source"]]

    for actual, exp in zip(res, expected):
        assert pytest.approx(exp) == pytest.approx(actual, abs=1e-5)


@pytest.mark.parametrize("dtype_pair", valid_similarity_dtype_pairs)
def test_jaccard_similarity(dtype_pair):
    dtype1, dtype2 = dtype_pair
    data = {
        "source": [[1, 0, 1], [0, 0, 1]],
    }
    query = [1, 1, 0]

    df = daft.from_pydict(data)
    df = df.with_column("source", col("source").cast(dtype1))
    df = df.with_column("query", lit(query).cast(dtype2))

    res = df.select(col("source").jaccard_similarity(col("query"))).to_pydict()["source"]

    def jaccard(x, y):
        x_nonzero = [val != 0 for val in x]
        y_nonzero = [val != 0 for val in y]
        intersection = sum(a and b for a, b in zip(x_nonzero, y_nonzero))
        union = sum(a or b for a, b in zip(x_nonzero, y_nonzero))
        if union == 0:
            return 1.0
        return intersection / union

    expected = [jaccard(query, row) for row in data["source"]]

    for actual, exp in zip(res, expected):
        assert pytest.approx(exp) == pytest.approx(actual, abs=1e-5)
