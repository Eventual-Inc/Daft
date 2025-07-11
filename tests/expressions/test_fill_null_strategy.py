from __future__ import annotations

import pytest

import daft
from daft.expressions import col


class TestFillNullExpressionStrategy:
    """Test forward and backward fill strategies for fill_null expression."""

    def test_forward_fill_basic(self):
        """Test basic forward fill functionality at expression level."""
        df = daft.from_pydict({"a": [1, None, 3, None, 5]})
        result = df.select(col("a").fill_null(strategy="forward"))
        expected = {"a": [1, 1, 3, 3, 5]}
        assert result.collect().to_pydict() == expected

    def test_backward_fill_basic(self):
        """Test basic backward fill functionality at expression level."""
        df = daft.from_pydict({"a": [1, None, 3, None, 5]})
        result = df.select(col("a").fill_null(strategy="backward"))
        expected = {"a": [1, 3, 3, 5, 5]}
        assert result.collect().to_pydict() == expected

    def test_forward_fill_leading_nulls(self):
        """Test forward fill with leading nulls at expression level."""
        df = daft.from_pydict({"a": [None, None, 3, None, 5]})
        result = df.select(col("a").fill_null(strategy="forward"))
        expected = {"a": [None, None, 3, 3, 5]}
        assert result.collect().to_pydict() == expected

    def test_backward_fill_trailing_nulls(self):
        """Test backward fill with trailing nulls at expression level."""
        df = daft.from_pydict({"a": [1, None, 3, None, None]})
        result = df.select(col("a").fill_null(strategy="backward"))
        expected = {"a": [1, 3, 3, None, None]}
        assert result.collect().to_pydict() == expected

    def test_forward_fill_all_nulls(self):
        """Test forward fill with all nulls at expression level."""
        df = daft.from_pydict({"a": [None, None, None]})
        result = df.select(col("a").fill_null(strategy="forward"))
        expected = {"a": [None, None, None]}
        assert result.collect().to_pydict() == expected

    def test_backward_fill_all_nulls(self):
        """Test backward fill with all nulls at expression level."""
        df = daft.from_pydict({"a": [None, None, None]})
        result = df.select(col("a").fill_null(strategy="backward"))
        expected = {"a": [None, None, None]}
        assert result.collect().to_pydict() == expected

    def test_forward_fill_no_nulls(self):
        """Test forward fill with no nulls at expression level."""
        df = daft.from_pydict({"a": [1, 2, 3]})
        result = df.select(col("a").fill_null(strategy="forward"))
        expected = {"a": [1, 2, 3]}
        assert result.collect().to_pydict() == expected

    def test_backward_fill_no_nulls(self):
        """Test backward fill with no nulls at expression level."""
        df = daft.from_pydict({"a": [1, 2, 3]})
        result = df.select(col("a").fill_null(strategy="backward"))
        expected = {"a": [1, 2, 3]}
        assert result.collect().to_pydict() == expected

    def test_forward_fill_float_data(self):
        """Test forward fill with float data at expression level."""
        df = daft.from_pydict({"a": [1.5, None, 3.7, None, 5.2]})
        result = df.select(col("a").fill_null(strategy="forward"))
        expected = {"a": [1.5, 1.5, 3.7, 3.7, 5.2]}
        assert result.collect().to_pydict() == expected

    def test_backward_fill_float_data(self):
        """Test backward fill with float data at expression level."""
        df = daft.from_pydict({"a": [1.5, None, 3.7, None, 5.2]})
        result = df.select(col("a").fill_null(strategy="backward"))
        expected = {"a": [1.5, 3.7, 3.7, 5.2, 5.2]}
        assert result.collect().to_pydict() == expected

    def test_forward_fill_string_data(self):
        """Test forward fill with string data at expression level."""
        df = daft.from_pydict({"a": ["a", None, "c", None, "e"]})
        result = df.select(col("a").fill_null(strategy="forward"))
        expected = {"a": ["a", "a", "c", "c", "e"]}
        assert result.collect().to_pydict() == expected

    def test_backward_fill_string_data(self):
        """Test backward fill with string data at expression level."""
        df = daft.from_pydict({"a": ["a", None, "c", None, "e"]})
        result = df.select(col("a").fill_null(strategy="backward"))
        expected = {"a": ["a", "c", "c", "e", "e"]}
        assert result.collect().to_pydict() == expected

    def test_value_strategy_backward_compatibility(self):
        """Test that value strategy works with backward compatibility at expression level."""
        df = daft.from_pydict({"a": [1, None, 3]})

        # Test old API
        result = df.select(col("a").fill_null(2))
        expected = {"a": [1, 2, 3]}
        assert result.collect().to_pydict() == expected

        # Test new API with explicit value strategy
        result = df.select(col("a").fill_null(2, strategy="value"))
        expected = {"a": [1, 2, 3]}
        assert result.collect().to_pydict() == expected

    def test_strategy_validation(self):
        """Test strategy parameter validation at expression level."""
        df = daft.from_pydict({"a": [1, None, 3]})

        # Test invalid strategy
        with pytest.raises(ValueError, match="strategy must be one of"):
            df.select(col("a").fill_null(strategy="invalid")).collect()

        # Test value strategy without fill_value
        with pytest.raises(ValueError, match="fill_value must be provided"):
            df.select(col("a").fill_null(strategy="value")).collect()

        # Test forward strategy with fill_value
        with pytest.raises(ValueError, match="fill_value should not be provided"):
            df.select(col("a").fill_null(2, strategy="forward")).collect()

        # Test backward strategy with fill_value
        with pytest.raises(ValueError, match="fill_value should not be provided"):
            df.select(col("a").fill_null(2, strategy="backward")).collect()

    def test_empty_dataframe(self):
        """Test fill_null with empty dataframe."""
        df = daft.from_pydict({"a": []})

        result = df.select(col("a").fill_null(strategy="forward"))
        assert result.collect().to_pydict() == {"a": []}

        result = df.select(col("a").fill_null(strategy="backward"))
        assert result.collect().to_pydict() == {"a": []}

    def test_multiple_columns(self):
        """Test fill_null with multiple columns."""
        df = daft.from_pydict(
            {"a": [1, None, 3, None, 5], "b": [None, 2, None, 4, None], "c": ["x", None, "z", None, "w"]}
        )

        result = df.select(
            col("a").fill_null(strategy="forward").alias("a_forward"),
            col("b").fill_null(strategy="backward").alias("b_backward"),
            col("c").fill_null(strategy="forward").alias("c_forward"),
        )

        expected = {
            "a_forward": [1, 1, 3, 3, 5],
            "b_backward": [2, 2, 4, 4, None],
            "c_forward": ["x", "x", "z", "z", "w"],
        }

        assert result.collect().to_pydict() == expected

    def test_chained_operations(self):
        """Test fill_null in chained operations."""
        df = daft.from_pydict({"a": [1, None, 3, None, 5]})

        result = df.select(col("a").fill_null(strategy="forward").alias("filled")).where(col("filled") > 1)

        expected = {"filled": [3, 3, 5]}
        assert result.collect().to_pydict() == expected

    def test_mixed_null_patterns(self):
        """Test with various null patterns matching pandas behavior."""
        df = daft.from_pydict({"a": [None, 1, None, 3, None]})

        result = df.select(
            col("a").fill_null(strategy="forward").alias("forward"),
            col("a").fill_null(strategy="backward").alias("backward"),
        )

        expected = {"forward": [None, 1, 1, 3, 3], "backward": [1, 1, 3, 3, None]}

        assert result.collect().to_pydict() == expected

    def test_single_row_dataframe(self):
        """Test fill_null with single row dataframe."""
        df = daft.from_pydict({"a": [None]})

        result = df.select(col("a").fill_null(strategy="forward"))
        assert result.collect().to_pydict() == {"a": [None]}

        result = df.select(col("a").fill_null(strategy="backward"))
        assert result.collect().to_pydict() == {"a": [None]}

        df = daft.from_pydict({"a": [42]})

        result = df.select(col("a").fill_null(strategy="forward"))
        assert result.collect().to_pydict() == {"a": [42]}

        result = df.select(col("a").fill_null(strategy="backward"))
        assert result.collect().to_pydict() == {"a": [42]}
