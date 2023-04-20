from __future__ import annotations

import itertools
import math

import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.series import Series
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES

PYARROW_GE_7_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (7, 0, 0)


@pytest.mark.skipif(
    not PYARROW_GE_7_0_0,
    reason="Array.nbytes behavior changed in versions >= 7.0.0. Old behavior is incompatible with our tests and is renamed to Array.get_total_buffer_size()",
)
@pytest.mark.parametrize("dtype, size", itertools.product(ARROW_INT_TYPES + ARROW_FLOAT_TYPES, [0, 1, 2, 8, 9, 16]))
def test_series_numeric_size_bytes(dtype, size) -> None:
    pydata = list(range(size))
    data = pa.array(pydata, dtype)

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.from_arrow_type(dtype)
    assert s.size_bytes() == data.nbytes

    ## with nulls
    if size > 0:
        pydata = pydata[:-1] + [None]
    data = pa.array(pydata, dtype)

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.from_arrow_type(dtype)
    assert s.size_bytes() == data.nbytes


@pytest.mark.skipif(
    not PYARROW_GE_7_0_0,
    reason="Array.nbytes behavior changed in versions >= 7.0.0. Old behavior is incompatible with our tests and is renamed to Array.get_total_buffer_size()",
)
@pytest.mark.parametrize("size", [0, 1, 2, 8, 9, 16])
def test_series_string_size_bytes(size) -> None:

    pydata = list(str(i) for i in range(size))
    data = pa.array(pydata, pa.large_string())

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.string()
    # TODO(Clark): This is required due to an off-by-one error in pyarrow's calculation of of the offset array length.
    # We should fix this upstream and/or refactor these tests to not rely on pyarrow as the source-of-truth.
    assert s.size_bytes() == data.nbytes + 8

    ## with nulls
    if size > 0:
        pydata = pydata[:-1] + [None]
    data = pa.array(pydata, pa.large_string())

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.string()
    assert s.size_bytes() == data.nbytes + 8


@pytest.mark.skipif(
    not PYARROW_GE_7_0_0,
    reason="Array.nbytes behavior changed in versions >= 7.0.0. Old behavior is incompatible with our tests and is renamed to Array.get_total_buffer_size()",
)
@pytest.mark.parametrize("size", [0, 1, 2, 8, 9, 16])
def test_series_boolean_size_bytes(size) -> None:

    pydata = [True if i % 2 else False for i in range(size)]

    data = pa.array(pydata, pa.bool_())

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.bool()
    assert s.size_bytes() == data.nbytes

    ## with nulls
    if size > 0:
        pydata = pydata[:-1] + [None]
    data = pa.array(pydata, pa.bool_())

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.bool()
    assert s.size_bytes() == data.nbytes


@pytest.mark.skipif(
    not PYARROW_GE_7_0_0,
    reason="Array.nbytes behavior changed in versions >= 7.0.0. Old behavior is incompatible with our tests and is renamed to Array.get_total_buffer_size()",
)
@pytest.mark.parametrize("size", [0, 1, 2, 8, 9, 16])
def test_series_date_size_bytes(size) -> None:
    from datetime import date

    pydata = [date(2023, 1, i + 1) for i in range(size)]
    data = pa.array(pydata, pa.date32())
    s = Series.from_arrow(data)

    assert s.datatype() == DataType.date()
    assert s.size_bytes() == data.nbytes

    ## with nulls
    if size > 0:
        pydata = pydata[:-1] + [None]
    data = pa.array(pydata, pa.date32())
    s = Series.from_arrow(data)

    assert s.datatype() == DataType.date()
    assert s.size_bytes() == data.nbytes


@pytest.mark.skipif(
    not PYARROW_GE_7_0_0,
    reason="Array.nbytes behavior changed in versions >= 7.0.0. Old behavior is incompatible with our tests and is renamed to Array.get_total_buffer_size()",
)
@pytest.mark.parametrize("size", [0, 1, 2, 8, 9, 16])
def test_series_binary_size_bytes(size) -> None:
    pydata = [str(i).encode("utf-8") for i in range(size)]
    data = pa.array(pydata, pa.large_binary())
    s = Series.from_arrow(data)

    assert s.datatype() == DataType.binary()
    # TODO(Clark): This is required due to an off-by-one error in pyarrow's calculation of of the offset array length.
    # We should fix this upstream and/or refactor these tests to not rely on pyarrow as the source-of-truth.
    assert s.size_bytes() == data.nbytes + 8

    ## with nulls
    if size > 0:
        pydata = pydata[:-1] + [None]
    data = pa.array(pydata, pa.large_binary())
    s = Series.from_arrow(data)

    assert s.datatype() == DataType.binary()
    assert s.size_bytes() == data.nbytes + 8


@pytest.mark.skipif(
    not PYARROW_GE_7_0_0,
    reason="Array.nbytes behavior changed in versions >= 7.0.0. Old behavior is incompatible with our tests and is renamed to Array.get_total_buffer_size()",
)
@pytest.mark.parametrize("dtype, size", itertools.product(ARROW_INT_TYPES + ARROW_FLOAT_TYPES, [0, 1, 2, 8, 9, 16]))
def test_series_list_size_bytes(dtype, size) -> None:
    list_dtype = pa.list_(dtype)
    pydata = [[2 * i] if i % 2 == 0 else [2 * i, 2 * i + 1] for i in range(size)]
    data = pa.array(pydata, list_dtype)

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.from_arrow_type(list_dtype)
    # TODO(Clark): Investigate this discrepancy.
    # TODO(Clark): Investigate this discrepancy between Arrow2 and pyarrow.
    # We should fix this upstream and/or refactor these tests to not rely on pyarrow as the source-of-truth.
    assert s.size_bytes() == data.nbytes + (size * 8) // 2

    ## with nulls
    if size > 0:
        pydata = pydata[:-1] + [None]
    data = pa.array(pydata, list_dtype)

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.from_arrow_type(list_dtype)
    assert s.size_bytes() == data.nbytes + (size * 8) // 2


@pytest.mark.skipif(
    not PYARROW_GE_7_0_0,
    reason="Array.nbytes behavior changed in versions >= 7.0.0. Old behavior is incompatible with our tests and is renamed to Array.get_total_buffer_size()",
)
@pytest.mark.parametrize("dtype, size", itertools.product(ARROW_INT_TYPES + ARROW_FLOAT_TYPES, [0, 1, 2, 8, 9, 16]))
def test_series_fixed_size_list_size_bytes(dtype, size) -> None:
    list_dtype = pa.list_(dtype, 2)
    pydata = [[2 * i, 2 * i + 1] for i in range(size)]
    data = pa.array(pydata, list_dtype)

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.from_arrow_type(list_dtype)
    assert s.size_bytes() == data.nbytes

    ## with nulls
    if size > 0:
        pydata = pydata[:-1] + [None]
    data = pa.array(pydata, list_dtype)

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.from_arrow_type(list_dtype)
    assert s.size_bytes() == data.nbytes


@pytest.mark.skipif(
    not PYARROW_GE_7_0_0,
    reason="Array.nbytes behavior changed in versions >= 7.0.0. Old behavior is incompatible with our tests and is renamed to Array.get_total_buffer_size()",
)
@pytest.mark.parametrize("size", [0, 1, 2, 8, 9, 16])
def test_series_struct_size_bytes(size) -> None:
    dtype1, dtype2, dtype3 = pa.int64(), pa.float64(), pa.string()
    dtype = pa.struct({"a": dtype1, "b": dtype2, "c": dtype3})
    pydata = [
        {"a": 3 * i, "b": 3 * i + 1, "c": 3 * i + 2} if i % 2 == 0 else {"a": 3 * i, "c": 3 * i + 2}
        for i in range(size)
    ]
    data = pa.array(pydata).cast(dtype)

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.from_arrow_type(dtype)
    # TODO(Clark): Investigate this discrepancy.
    # TODO(Clark): Investigate this discrepancy between Arrow2 and pyarrow.
    # We should fix this upstream and/or refactor these tests to not rely on pyarrow as the source-of-truth.
    assert s.size_bytes() == data.nbytes + 8 + (size * 8) // 2

    ## with nulls
    if size > 0:
        pydata = pydata[:-1] + [None]
    data = pa.array(pydata).cast(dtype)

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.from_arrow_type(dtype)
    assert s.size_bytes() == data.nbytes + 8 + (size * 8) // 2 + math.ceil(size / 8)
