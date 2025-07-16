from __future__ import annotations

from functools import partial
from pathlib import Path

import numpy as np
import pytest

import daft


def _make_embedding(rng, dtype, size: int) -> np.ndarray:
    if dtype in (np.float32, np.float64):
        return rng.random(size=(size,), dtype=dtype)
    else:
        v = rng.random(size=(size,), dtype=np.float32)
        c = np.rint(v * 100)
        return c.astype(dtype)


@pytest.mark.parametrize("fmt", ["parquet", "lance"])
@pytest.mark.parametrize(["dtype", "size"], [(np.float32, 10), (np.int8, 6), (np.int64, 12), (np.float64, 4)])
def test_roundtrip_embedding(tmp_path: Path, fmt: str, dtype: np.dtype, size: int) -> None:
    rng = np.random.default_rng()

    make_array = partial(_make_embedding, rng, dtype, size)

    test_df = (
        daft.from_pydict({"e": [make_array() for _ in range(10)]})
        .with_column("e", daft.col("e").cast(daft.DataType.embedding(daft.DataType.from_numpy_dtype(dtype), size)))
        .collect()
    )

    test_rows = list(x["e"] for x in test_df.iter_rows())
    for t in test_rows:
        assert isinstance(t, np.ndarray)
        assert t.dtype == dtype

    getattr(test_df, f"write_{fmt}")(str(tmp_path)).collect()

    loaded_df = getattr(daft, f"read_{fmt}")(str(tmp_path)).collect()

    l_rows = list(x["e"] for x in loaded_df.iter_rows())
    for i, (t, l) in enumerate(zip(test_rows, l_rows)):  # noqa: E741
        assert isinstance(l, np.ndarray), f"Expected a numpy array when loading, got a {type(l)}: {l}"
        assert (t == l).all(), f"Failed on row {i}: test_df={t} vs. loaded={l}"
        assert l.dtype == t.dtype
