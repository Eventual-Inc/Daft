from __future__ import annotations

from functools import partial
from pathlib import Path
from typing import Callable, Literal

import numpy as np
import pytest

import daft
from daft import DataType
from tests.utils import random_numerical_embedding

FMT = Literal["parquet", "lance"]


def _make_check_embeddings(
    test_df: daft.DataFrame, dtype, *, embeddings_col: str = "e"
) -> Callable[[daft.DataFrame], None]:
    """Verify validity of the test dataframe & produce a function that checks another dataframe for equality."""
    test_df = test_df.collect()
    if test_df.count_rows() == 0:
        raise ValueError("Test DataFrame cannot be empty!")

    if embeddings_col not in test_df:
        raise ValueError(f"Test DataFrame doesn't have an embeddings column={embeddings_col}")

    test_rows = test_df.to_pydict()[embeddings_col]
    for i, t in enumerate(test_rows):
        assert isinstance(t, np.ndarray), f"Row {i} is not a numpy array, it is: {type(t)}: {t}"
        assert t.dtype == dtype, f"Row {i} array doesn't have {dtype=}, instead={t.dtype}"

    def _check_embeddings(loaded_df: daft.DataFrame) -> None:
        loaded_df = loaded_df.collect()
        if loaded_df.count_rows() != test_df.count_rows():
            raise ValueError(
                f"Expecting {test_df.count_rows()} rows but got a " f"DataFrame with {loaded_df.count_rows()}"
            )

        l_rows = loaded_df.to_pydict()[embeddings_col]
        for i, (t, l) in enumerate(zip(test_rows, l_rows)):  # noqa: E741
            assert isinstance(l, np.ndarray), f"Row {i} expected a numpy array when loading, got a {type(l)}: {l}"
            assert l.dtype == t.dtype, f"Row {i} has wrong dtype. Expected={t.dtype} vs. found={l.dtype}"
            assert (t == l).all(), f"Row {i} failed equality check: test_df={t} vs. loaded={l}"

    return _check_embeddings


@pytest.mark.parametrize("fmt", ["parquet", "lance"])
@pytest.mark.parametrize(
    ["dtype", "size"],
    [
        # (np.float16, 64), -- Arrow doesn't support f16
        (np.float32, 1024),
        (np.float64, 512),
        (np.int8, 2048),
        (np.int16, 512),
        (np.int32, 256),
        (np.int64, 128),
        (np.uint8, 2048),
        (np.uint16, 512),
        (np.uint32, 256),
        (np.uint64, 128),
        # (np.bool_, 512), -- Arrow only accepts numeric types
        # (np.complex64, 32), (np.complex128, 16), - Arrow doesn't support complex numbers
    ],
)
def test_roundtrip_embedding(tmp_path: Path, fmt: FMT, dtype: np.dtype, size: int) -> None:
    # make some embeddings of the specified data type and dimensionality
    # with uniformly at random distributed values
    make_array = partial(random_numerical_embedding, np.random.default_rng(), dtype, size)
    test_df = daft.from_pydict({"e": [make_array() for _ in range(50)]}).with_column(
        "e", daft.col("e").cast(DataType.embedding(DataType.from_numpy_dtype(dtype), size))
    )

    # make a checking function for the loaded dataframe & verify our original dataframe
    check = _make_check_embeddings(test_df, dtype)

    # write the embeddings-containing dataframe to disk using the specified format
    getattr(test_df, f"write_{fmt}")(str(tmp_path))

    # read that same dataframe
    loaded_df = getattr(daft, f"read_{fmt}")(str(tmp_path))

    # check that the values in the embedding column exactly equal each other
    check(loaded_df)
