from __future__ import annotations

import datetime
from functools import partial
from pathlib import Path
from typing import Callable

import numpy as np
import pyarrow as pa
import pytest

import daft
from daft import DataType, TimeUnit


def _make_embedding(rng, dtype, size: int) -> np.ndarray:
    """Generate a uniformly distributed 1-D array of the specified datatype & dimensionality."""
    if dtype in (np.float32, np.float64):
        return rng.random(size=(size,), dtype=dtype)
    else:
        v = rng.random(size=(size,), dtype=np.float32)
        c = np.rint(v * 100)
        return c.astype(dtype)


def _make_check_embeddings(
    test_df: daft.DataFrame, dtype, *, embeddings_col: str = "e"
) -> Callable[[daft.DataFrame], None]:
    """Verify validity of the test dataframe & produce a function that checks another dataframe for equality."""
    test_df = test_df.collect()
    if test_df.count_rows() == 0:
        raise ValueError("Test DataFrame cannot be empty!")

    if embeddings_col not in test_df:
        raise ValueError(f"Test DataFrame doesn't have an embeddings column={embeddings_col}")

    test_rows = list(x[embeddings_col] for x in test_df.iter_rows())
    for i, t in enumerate(test_rows):
        assert isinstance(t, np.ndarray), f"Row {i} is not a numpy array, it is: {type(t)}: {t}"
        assert t.dtype == dtype, f"Row {i} array doesn't have {dtype=}, instead={t.dtype}"

    def _check_embeddings(loaded_df: daft.DataFrame) -> None:
        loaded_df = loaded_df.collect()
        if loaded_df.count_rows() != test_df.count_rows():
            raise ValueError(
                f"Expecting {test_df.count_rows()} rows but got a " f"DataFrame with {loaded_df.count_rows()}"
            )

        l_rows = list(x[embeddings_col] for x in loaded_df.iter_rows())
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
def test_roundtrip_embedding(tmp_path: Path, fmt: str, dtype: np.dtype, size: int) -> None:
    # make some embeddings of the specified data type and dimensionality
    # with uniformly at random distributed values
    make_array = partial(_make_embedding, np.random.default_rng(), dtype, size)
    test_df = (
        daft.from_pydict({"e": [make_array() for _ in range(50)]})
        .with_column("e", daft.col("e").cast(daft.DataType.embedding(daft.DataType.from_numpy_dtype(dtype), size)))
        .collect()
    )

    # make a checking function for the loaded dataframe & verify our original dataframe
    check = _make_check_embeddings(test_df, dtype)

    # write the embeddings-containing dataframe to disk using the specified format
    getattr(test_df, f"write_{fmt}")(str(tmp_path)).collect()

    # read that same dataframe
    loaded_df = getattr(daft, f"read_{fmt}")(str(tmp_path)).collect()

    # check that the values in the embedding column exactly equal each other
    check(loaded_df)


@pytest.mark.parametrize("fmt", ["parquet"])
@pytest.mark.parametrize(
    ["data", "pa_type", "expected_dtype"],
    [
        (
            [datetime.datetime(1994, 1, 1), datetime.datetime(1995, 1, 1), None],
            pa.timestamp("ms", None),
            DataType.timestamp(TimeUnit.ms(), None),
        ),
        (
            [datetime.datetime(1994, 1, 1), datetime.datetime(1995, 1, 1), None],
            pa.timestamp("ms", "+00:00"),
            DataType.timestamp(TimeUnit.ms(), "+00:00"),
        ),
        (
            [datetime.datetime(1994, 1, 1), datetime.datetime(1995, 1, 1), None],
            pa.timestamp("ms", "UTC"),
            DataType.timestamp(TimeUnit.ms(), "UTC"),
        ),
        (
            [datetime.datetime(1994, 1, 1), datetime.datetime(1995, 1, 1), None],
            pa.timestamp("ms", "+08:00"),
            DataType.timestamp(TimeUnit.ms(), "+08:00"),
        ),
    ],
)
def test_roundtrip_temporal_arrow_types(
    tmp_path: Path, fmt: str, data: list[datetime.datetime], pa_type, expected_dtype: daft.DataType
):
    before = daft.from_arrow(pa.table({"foo": pa.array(data, type=pa_type)}))
    before = before.concat(before)
    getattr(before, f"write_{fmt}")(str(tmp_path))
    after = getattr(daft, f"read_{fmt}")(str(tmp_path))
    assert before.schema()["foo"].dtype == expected_dtype
    assert after.schema()["foo"].dtype == expected_dtype
    assert before.to_arrow() == after.to_arrow()
