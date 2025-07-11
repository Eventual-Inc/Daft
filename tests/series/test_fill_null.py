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


# Strategy-based fill_null tests


@pytest.mark.parametrize(
    "input_data,dtype,strategy,expected",
    [
        # Basic functionality
        ([1, None, 3, None, 5], pa.int64(), "forward", [1, 1, 3, 3, 5]),
        ([1, None, 3, None, 5], pa.int64(), "backward", [1, 3, 3, 5, 5]),
        # Leading/trailing nulls
        ([None, None, 3, None, 5], pa.int64(), "forward", [None, None, 3, 3, 5]),
        ([1, None, 3, None, None], pa.int64(), "backward", [1, 3, 3, None, None]),
        # All nulls
        ([None, None, None], pa.int64(), "forward", [None, None, None]),
        ([None, None, None], pa.int64(), "backward", [None, None, None]),
        # No nulls
        ([1, 2, 3], pa.int64(), "forward", [1, 2, 3]),
        ([1, 2, 3], pa.int64(), "backward", [1, 2, 3]),
        # Single values
        ([None], pa.int64(), "forward", [None]),
        ([None], pa.int64(), "backward", [None]),
        ([42], pa.int64(), "forward", [42]),
        ([42], pa.int64(), "backward", [42]),
        # Float data
        ([1.5, None, 3.7, None, 5.2], pa.float64(), "forward", [1.5, 1.5, 3.7, 3.7, 5.2]),
        ([1.5, None, 3.7, None, 5.2], pa.float64(), "backward", [1.5, 3.7, 3.7, 5.2, 5.2]),
        # String data
        (["a", None, "c", None, "e"], pa.string(), "forward", ["a", "a", "c", "c", "e"]),
        (["a", None, "c", None, "e"], pa.string(), "backward", ["a", "c", "c", "e", "e"]),
        # Empty series
        ([], pa.int64(), "forward", []),
        ([], pa.int64(), "backward", []),
        # Alternating nulls
        ([1, None, 3, None, 5, None, 7], pa.int64(), "forward", [1, 1, 3, 3, 5, 5, 7]),
        ([1, None, 3, None, 5, None, 7], pa.int64(), "backward", [1, 3, 3, 5, 5, 7, 7]),
        # Consecutive nulls
        ([1, None, None, None, 5], pa.int64(), "forward", [1, 1, 1, 1, 5]),
        ([1, None, None, None, 5], pa.int64(), "backward", [1, 5, 5, 5, 5]),
        # Mixed patterns
        ([None, 1, None, 3, None], pa.int64(), "forward", [None, 1, 1, 3, 3]),
        ([None, 1, None, 3, None], pa.int64(), "backward", [1, 1, 3, 3, None]),
    ],
)
def test_series_fill_null_strategies(input_data, dtype, strategy, expected):
    """Test fill_null with various strategies and data patterns."""
    s = Series.from_arrow(pa.array(input_data, dtype))
    result = s.fill_null(strategy=strategy)
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    "input_data,fill_data,strategy,expected",
    [
        # Value strategy with old API
        ([1, None, 3], [2, 2, 2], None, [1, 2, 3]),
        # Value strategy with new API
        ([1, None, 3], [2, 2, 2], "value", [1, 2, 3]),
    ],
)
def test_series_fill_null_value_strategy_backward_compatibility(input_data, fill_data, strategy, expected):
    """Test that value strategy works with backward compatibility."""
    s = Series.from_arrow(pa.array(input_data, pa.int64()))
    fill_value = Series.from_arrow(pa.array(fill_data, pa.int64()))

    if strategy is None:
        # Test old API
        result = s.fill_null(fill_value)
    else:
        # Test new API with explicit value strategy
        result = s.fill_null(fill_value, strategy=strategy)

    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    "strategy,fill_value,error_match",
    [
        ("invalid", None, "strategy must be one of"),
        ("value", None, "fill_value must be provided"),
        ("forward", "dummy_fill", "fill_value should not be provided"),
        ("backward", "dummy_fill", "fill_value should not be provided"),
    ],
)
def test_series_fill_null_strategy_validation(strategy, fill_value, error_match):
    """Test strategy parameter validation."""
    s = Series.from_arrow(pa.array([1, None, 3], pa.int64()))

    # Create actual fill_value Series for cases that need it
    if fill_value == "dummy_fill":
        fill_value = Series.from_arrow(pa.array([2, 2, 2], pa.int64()))

    with pytest.raises(ValueError, match=error_match):
        if fill_value is None:
            s.fill_null(strategy=strategy)
        else:
            s.fill_null(fill_value, strategy=strategy)
