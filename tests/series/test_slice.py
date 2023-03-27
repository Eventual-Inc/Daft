from __future__ import annotations

import pyarrow as pa
import pytest

from daft.series import Series
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES, ARROW_STRING_TYPES


@pytest.mark.parametrize("dtype", ARROW_INT_TYPES + ARROW_FLOAT_TYPES + ARROW_STRING_TYPES)
def test_series_slice(dtype) -> None:
    data = pa.array([10, 20, 33, None, 50, None])

    s = Series.from_arrow(data.cast(dtype))

    result = s.slice(2, 4)
    assert result.datatype() == s.datatype()
    assert len(result) == 2

    original_data = s.to_pylist()
    expected = original_data[2:4]
    assert result.to_pylist() == expected


def test_series_slice_bad_input() -> None:
    data = pa.array([10, 20, 33, None, 50, None])

    s = Series.from_arrow(data)

    with pytest.raises(ValueError, match="slice length can not be negative:"):
        s.slice(3, 2)

    with pytest.raises(ValueError, match="slice start can not be negative"):
        s.slice(-1, 2)

    with pytest.raises(ValueError, match="slice end can not be negative"):
        s.slice(0, -1)
