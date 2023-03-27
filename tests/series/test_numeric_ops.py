from __future__ import annotations

import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.series import Series
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES


@pytest.mark.parametrize("dtype", ARROW_INT_TYPES + ARROW_FLOAT_TYPES)
def test_series_numeric_abs(dtype) -> None:
    if pa.types.is_unsigned_integer(dtype):
        pydata = list(range(0, 10))
    else:
        pydata = list(range(-10, 10))

    data = pa.array(pydata, dtype)

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.from_arrow_type(dtype)

    abs_s = abs(s)

    assert abs_s.datatype() == DataType.from_arrow_type(dtype)

    assert abs_s.to_pylist() == list(map(abs, pydata))


def test_table_abs_bad_input() -> None:
    series = Series.from_pylist(["a", "b", "c"])

    with pytest.raises(ValueError, match="abs not implemented"):
        abs(series)
