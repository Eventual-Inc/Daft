from __future__ import annotations

import pyarrow as pa
import pytest

from daft.series import Series


class TestFillNullStrategy:
    """Test forward and backward fill strategies for fill_null method."""

    def test_forward_fill_basic(self):
        """Test basic forward fill functionality."""
        s = Series.from_arrow(pa.array([1, None, 3, None, 5], pa.int64()))
        result = s.fill_null(strategy="forward")
        expected = [1, 1, 3, 3, 5]
        assert result.to_pylist() == expected

    def test_backward_fill_basic(self):
        """Test basic backward fill functionality."""
        s = Series.from_arrow(pa.array([1, None, 3, None, 5], pa.int64()))
        result = s.fill_null(strategy="backward")
        expected = [1, 3, 3, 5, 5]
        assert result.to_pylist() == expected

    def test_forward_fill_leading_nulls(self):
        """Test forward fill with leading nulls."""
        s = Series.from_arrow(pa.array([None, None, 3, None, 5], pa.int64()))
        result = s.fill_null(strategy="forward")
        expected = [None, None, 3, 3, 5]
        assert result.to_pylist() == expected

    def test_backward_fill_trailing_nulls(self):
        """Test backward fill with trailing nulls."""
        s = Series.from_arrow(pa.array([1, None, 3, None, None], pa.int64()))
        result = s.fill_null(strategy="backward")
        expected = [1, 3, 3, None, None]
        assert result.to_pylist() == expected

    def test_forward_fill_all_nulls(self):
        """Test forward fill with all nulls."""
        s = Series.from_arrow(pa.array([None, None, None], pa.int64()))
        result = s.fill_null(strategy="forward")
        expected = [None, None, None]
        assert result.to_pylist() == expected

    def test_backward_fill_all_nulls(self):
        """Test backward fill with all nulls."""
        s = Series.from_arrow(pa.array([None, None, None], pa.int64()))
        result = s.fill_null(strategy="backward")
        expected = [None, None, None]
        assert result.to_pylist() == expected

    def test_forward_fill_no_nulls(self):
        """Test forward fill with no nulls."""
        s = Series.from_arrow(pa.array([1, 2, 3], pa.int64()))
        result = s.fill_null(strategy="forward")
        expected = [1, 2, 3]
        assert result.to_pylist() == expected

    def test_backward_fill_no_nulls(self):
        """Test backward fill with no nulls."""
        s = Series.from_arrow(pa.array([1, 2, 3], pa.int64()))
        result = s.fill_null(strategy="backward")
        expected = [1, 2, 3]
        assert result.to_pylist() == expected

    def test_forward_fill_single_value(self):
        """Test forward fill with single value."""
        s = Series.from_arrow(pa.array([None], pa.int64()))
        result = s.fill_null(strategy="forward")
        expected = [None]
        assert result.to_pylist() == expected

        s = Series.from_arrow(pa.array([42], pa.int64()))
        result = s.fill_null(strategy="forward")
        expected = [42]
        assert result.to_pylist() == expected

    def test_backward_fill_single_value(self):
        """Test backward fill with single value."""
        s = Series.from_arrow(pa.array([None], pa.int64()))
        result = s.fill_null(strategy="backward")
        expected = [None]
        assert result.to_pylist() == expected

        s = Series.from_arrow(pa.array([42], pa.int64()))
        result = s.fill_null(strategy="backward")
        expected = [42]
        assert result.to_pylist() == expected

    def test_forward_fill_float_data(self):
        """Test forward fill with float data."""
        s = Series.from_arrow(pa.array([1.5, None, 3.7, None, 5.2], pa.float64()))
        result = s.fill_null(strategy="forward")
        expected = [1.5, 1.5, 3.7, 3.7, 5.2]
        assert result.to_pylist() == expected

    def test_backward_fill_float_data(self):
        """Test backward fill with float data."""
        s = Series.from_arrow(pa.array([1.5, None, 3.7, None, 5.2], pa.float64()))
        result = s.fill_null(strategy="backward")
        expected = [1.5, 3.7, 3.7, 5.2, 5.2]
        assert result.to_pylist() == expected

    def test_forward_fill_string_data(self):
        """Test forward fill with string data."""
        s = Series.from_arrow(pa.array(["a", None, "c", None, "e"], pa.string()))
        result = s.fill_null(strategy="forward")
        expected = ["a", "a", "c", "c", "e"]
        assert result.to_pylist() == expected

    def test_backward_fill_string_data(self):
        """Test backward fill with string data."""
        s = Series.from_arrow(pa.array(["a", None, "c", None, "e"], pa.string()))
        result = s.fill_null(strategy="backward")
        expected = ["a", "c", "c", "e", "e"]
        assert result.to_pylist() == expected

    def test_value_strategy_backward_compatibility(self):
        """Test that value strategy works with backward compatibility."""
        s = Series.from_arrow(pa.array([1, None, 3], pa.int64()))
        fill_value = Series.from_arrow(pa.array([2, 2, 2], pa.int64()))

        # Test old API
        result = s.fill_null(fill_value)
        expected = [1, 2, 3]
        assert result.to_pylist() == expected

        # Test new API with explicit value strategy
        result = s.fill_null(fill_value, strategy="value")
        expected = [1, 2, 3]
        assert result.to_pylist() == expected

    def test_strategy_validation(self):
        """Test strategy parameter validation."""
        s = Series.from_arrow(pa.array([1, None, 3], pa.int64()))

        # Test invalid strategy
        with pytest.raises(ValueError, match="strategy must be one of"):
            s.fill_null(strategy="invalid")

        # Test value strategy without fill_value
        with pytest.raises(ValueError, match="fill_value must be provided"):
            s.fill_null(strategy="value")

        # Test forward strategy with fill_value
        fill_value = Series.from_arrow(pa.array([2, 2, 2], pa.int64()))
        with pytest.raises(ValueError, match="fill_value should not be provided"):
            s.fill_null(fill_value, strategy="forward")

        # Test backward strategy with fill_value
        with pytest.raises(ValueError, match="fill_value should not be provided"):
            s.fill_null(fill_value, strategy="backward")

    def test_empty_series(self):
        """Test fill_null with empty series."""
        s = Series.from_arrow(pa.array([], pa.int64()))

        result = s.fill_null(strategy="forward")
        assert result.to_pylist() == []

        result = s.fill_null(strategy="backward")
        assert result.to_pylist() == []

    def test_alternating_nulls(self):
        """Test fill_null with alternating nulls and values."""
        s = Series.from_arrow(pa.array([1, None, 3, None, 5, None, 7], pa.int64()))

        result = s.fill_null(strategy="forward")
        expected = [1, 1, 3, 3, 5, 5, 7]
        assert result.to_pylist() == expected

        result = s.fill_null(strategy="backward")
        expected = [1, 3, 3, 5, 5, 7, 7]
        assert result.to_pylist() == expected

    def test_consecutive_nulls(self):
        """Test fill_null with consecutive nulls."""
        s = Series.from_arrow(pa.array([1, None, None, None, 5], pa.int64()))

        result = s.fill_null(strategy="forward")
        expected = [1, 1, 1, 1, 5]
        assert result.to_pylist() == expected

        result = s.fill_null(strategy="backward")
        expected = [1, 5, 5, 5, 5]
        assert result.to_pylist() == expected

    def test_mixed_nulls_and_values(self):
        """Test fill_null with mixed pattern matching pandas behavior."""
        s = Series.from_arrow(pa.array([None, 1, None, 3, None], pa.int64()))

        result = s.fill_null(strategy="forward")
        expected = [None, 1, 1, 3, 3]
        assert result.to_pylist() == expected

        result = s.fill_null(strategy="backward")
        expected = [1, 1, 3, 3, None]
        assert result.to_pylist() == expected
