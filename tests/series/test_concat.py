from __future__ import annotations

import itertools

import pytest

from daft import DataType, Series
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES, ARROW_STRING_TYPES


@pytest.mark.parametrize(
    "dtype, chunks", itertools.product(ARROW_FLOAT_TYPES + ARROW_INT_TYPES + ARROW_STRING_TYPES, [1, 2, 3, 10])
)
def test_series_concat(dtype, chunks) -> None:
    series = []
    for i in range(chunks):
        series.append(Series.from_pylist([i * j for j in range(i)]).cast(dtype=DataType.from_arrow_type(dtype)))

    concated = Series.concat(series)

    assert concated.datatype() == DataType.from_arrow_type(dtype)
    concated_list = concated.to_pylist()

    counter = 0
    for i in range(chunks):
        for j in range(i):
            val = i * j
            assert float(concated_list[counter]) == val
            counter += 1


def test_series_concat_bad_input() -> None:
    mix_types_series = [Series.from_pylist([1, 2, 3]), []]
    with pytest.raises(TypeError, match="Expected a Series for concat"):
        Series.concat(mix_types_series)

    with pytest.raises(ValueError, match="Need at least 1 series"):
        Series.concat([])


def test_series_concat_dtype_mismatch() -> None:
    mix_types_series = [Series.from_pylist([1, 2, 3]), Series.from_pylist([1.0, 2.0, 3.0])]

    with pytest.raises(ValueError, match="concat requires all data types to match"):
        Series.concat(mix_types_series)
