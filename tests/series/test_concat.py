from __future__ import annotations

import itertools

import pyarrow as pa
import pytest

from daft import DataType, Series
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES, ARROW_STRING_TYPES


class MockObject:
    def __init__(self, test_val):
        self.test_val = test_val


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


@pytest.mark.parametrize("fixed", [False, True])
@pytest.mark.parametrize("chunks", [1, 2, 3, 10])
def test_series_concat_list_array(chunks, fixed) -> None:
    series = []
    arrow_type = pa.list_(pa.int64(), list_size=2 if fixed else -1)
    for i in range(chunks):
        series.append(Series.from_arrow(pa.array([[i + j, i * j] for j in range(i)], type=arrow_type)))

    concated = Series.concat(series)

    if fixed:
        assert concated.datatype() == DataType.fixed_size_list("item", DataType.int64(), 2)
    else:
        assert concated.datatype() == DataType.list("item", DataType.int64())
    concated_list = concated.to_pylist()

    counter = 0
    for i in range(chunks):
        for j in range(i):
            assert concated_list[counter][0] == i + j
            assert concated_list[counter][1] == i * j
            counter += 1


@pytest.mark.parametrize("chunks", [1, 2, 3, 10])
def test_series_concat_struct_array(chunks) -> None:
    series = []
    for i in range(chunks):
        series.append(
            Series.from_arrow(
                pa.array(
                    [{"a": i + j, "b": float(i * j)} for j in range(i)],
                    type=pa.struct({"a": pa.int64(), "b": pa.float64()}),
                )
            )
        )

    concated = Series.concat(series)

    assert concated.datatype() == DataType.struct({"a": DataType.int64(), "b": DataType.float64()})
    concated_list = concated.to_pylist()

    counter = 0
    for i in range(chunks):
        for j in range(i):
            assert concated_list[counter]["a"] == i + j
            assert concated_list[counter]["b"] == float(i * j)
            counter += 1


@pytest.mark.parametrize("chunks", [1, 2, 3, 10])
def test_series_concat_pyobj(chunks) -> None:
    series = []
    for i in range(chunks):
        series.append(Series.from_pylist([MockObject(i * j) for j in range(i)], pyobj="force"))

    concated = Series.concat(series)

    assert concated.datatype() == DataType.python()
    concated_list = concated.to_pylist()

    counter = 0
    for i in range(chunks):
        for j in range(i):
            val = i * j
            assert concated_list[counter].test_val == val
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
