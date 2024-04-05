from __future__ import annotations

import pyarrow as pa
import pytest

from daft.series import Series


@pytest.mark.parametrize(
    "input,fill_value,expected",
    [
        # No broadcast
        [[1, 2, None], [3, 3, 3], [1, 2, 3]],
        # Broadcast input
        [[None], [3, 3, 3], [3, 3, 3]],
        # Broadcast fill_value
        [[1, 2, None], [3], [1, 2, 3]],
        # Empty
        [[], [], []],
    ],
)
def test_series_fill_null(input, fill_value, expected) -> None:
    s = Series.from_arrow(pa.array(input, pa.int64()))
    fill_value = Series.from_arrow(pa.array(fill_value, pa.int64()))
    filled = s.fill_null(fill_value)
    assert filled.to_pylist() == expected


def test_series_fill_null_bad_input() -> None:
    s = Series.from_arrow(pa.array([1, 2, 3], pa.int64()))
    with pytest.raises(ValueError, match="expected another Series but got"):
        s.fill_null([1, 2, 3])
