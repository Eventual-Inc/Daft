from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

from daft.series import Series


@pytest.fixture(scope="module")
def dense_integer_series() -> Series:
    rng = np.random.default_rng(0)
    values = rng.integers(-8, 8, size=1_000_000, dtype=np.int64)
    return Series.from_arrow(pa.array(values))


@pytest.fixture(scope="module")
def sparse_integer_series() -> Series:
    rng = np.random.default_rng(1)
    values = rng.permutation(100_000).astype(np.int64) * 1_000_000
    return Series.from_arrow(pa.array(values))


@pytest.mark.benchmark(group="series_argsort")
def test_series_argsort_dense_integers(benchmark, dense_integer_series: Series) -> None:
    """Measure the low-cardinality integer argsort path used by Daft sort operators."""
    indices = benchmark(dense_integer_series.argsort)

    assert len(indices) == len(dense_integer_series)


@pytest.mark.benchmark(group="series_argsort")
def test_series_argsort_sparse_integer_fallback(benchmark, sparse_integer_series: Series) -> None:
    """Keep the generic comparison path under the same CodSpeed test contract."""
    indices = benchmark(sparse_integer_series.argsort)

    assert len(indices) == len(sparse_integer_series)
