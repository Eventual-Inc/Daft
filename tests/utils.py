from __future__ import annotations

import re
from typing import Any

import numpy as np
import pyarrow as pa

from daft.recordbatch import RecordBatch

ANSI_ESCAPE = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")


def sort_arrow_table(tbl: pa.Table, *sort_by: str, ascending: bool = False) -> pa.Table:
    return tbl.sort_by([(name, "ascending" if ascending else "descending") for name in sort_by])


def assert_pyarrow_tables_equal(from_daft: pa.Table, expected: pa.Table) -> None:
    # Do a round-trip with Daft in order to cast pyarrow dtypes to Daft's supported Arrow dtypes (e.g. string -> large_string).
    expected = RecordBatch.from_arrow_table(expected).to_arrow_table()
    assert from_daft == expected, f"from_daft = {from_daft}\n\nexpected = {expected}"


def sort_pydict(pydict: dict[str, list[Any]], *sort_by: str, ascending: bool = False) -> pa.Table:
    return sort_arrow_table(RecordBatch.from_pydict(pydict).to_arrow_table(), *sort_by, ascending=ascending).to_pydict()


def random_numerical_embedding(
    rng: np.random.Generator, dtype: np.dtype, size: int, *, mult_for_int_like: int = 100
) -> np.ndarray:
    """Generate a uniformly distributed 1-D array of the specified datatype & dimensionality.

    The :param:`mult_for_int_like` parameter controls how much to multiply the randomly generated
    floating point values before rounding them to make integer vectors. Only applicable when
    :param:`dtype` is a signed or unsigned integer. Must be nonzero.

    :raises:`ValueError` if :param:`mult_for_int_like` is non-positive and :param:`dtype` is integer.
    """
    if dtype in (np.float32, np.float64):
        return rng.random(size=(size,), dtype=dtype)
    else:
        if mult_for_int_like <= 0:
            raise ValueError(
                "Multiplier for converting random float vectors into integer "
                f"vectors must be nonzero: {mult_for_int_like=}"
            )
        v = rng.random(size=(size,), dtype=np.float32)
        c = np.rint(v * mult_for_int_like)
        return c.astype(dtype)
