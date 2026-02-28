"""Tests for daft.testing module."""

from __future__ import annotations

import pytest

import daft
from daft.testing import assert_frame_equal, assert_schema_equal, assert_series_equal


class TestAssertSchemaEqual:
    """Tests for assert_schema_equal function."""

    def test_equal_schemas(self):
        """Test that equal schemas pass."""
        schema1 = daft.from_pydict({"a": [1], "b": ["x"]}).schema()
        schema2 = daft.from_pydict({"a": [2], "b": ["y"]}).schema()
        assert_schema_equal(schema1, schema2)  # Should not raise

    def test_missing_column(self):
        """Test that missing columns are detected."""
        schema1 = daft.from_pydict({"a": [1]}).schema()
        schema2 = daft.from_pydict({"a": [1], "b": [2]}).schema()
        with pytest.raises(AssertionError, match="Missing columns.*'b'"):
            assert_schema_equal(schema1, schema2)

    def test_extra_column(self):
        """Test that extra columns are detected."""
        schema1 = daft.from_pydict({"a": [1], "b": [2]}).schema()
        schema2 = daft.from_pydict({"a": [1]}).schema()
        with pytest.raises(AssertionError, match="Extra columns.*'b'"):
            assert_schema_equal(schema1, schema2)

    def test_type_mismatch(self):
        """Test that type mismatches are detected."""
        schema1 = daft.from_pydict({"a": [1]}).schema()
        schema2 = daft.from_pydict({"a": ["x"]}).schema()
        with pytest.raises(AssertionError, match="Data type mismatch"):
            assert_schema_equal(schema1, schema2)

    def test_column_order_mismatch(self):
        """Test that column order mismatches are detected when check_column_order=True."""
        schema1 = daft.from_pydict({"a": [1], "b": [2]}).schema()
        schema2 = daft.from_pydict({"b": [2], "a": [1]}).schema()
        with pytest.raises(AssertionError, match="Column order mismatch"):
            assert_schema_equal(schema1, schema2, check_column_order=True)

    def test_ignore_column_order(self):
        """Test that column order is ignored when check_column_order=False."""
        schema1 = daft.from_pydict({"a": [1], "b": [2]}).schema()
        schema2 = daft.from_pydict({"b": [2], "a": [1]}).schema()
        assert_schema_equal(schema1, schema2, check_column_order=False)  # Should not raise

    def test_invalid_actual_type(self):
        """Test that TypeError is raised for invalid actual type."""
        schema = daft.from_pydict({"a": [1]}).schema()
        with pytest.raises(TypeError, match="actual must be a Schema"):
            assert_schema_equal("not a schema", schema)

    def test_invalid_expected_type(self):
        """Test that TypeError is raised for invalid expected type."""
        schema = daft.from_pydict({"a": [1]}).schema()
        with pytest.raises(TypeError, match="expected must be a Schema"):
            assert_schema_equal(schema, "not a schema")


class TestAssertFrameEqual:
    """Tests for assert_frame_equal function."""

    def test_equal_dataframes(self):
        """Test that equal DataFrames pass."""
        df1 = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        df2 = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        assert_frame_equal(df1, df2)  # Should not raise

    def test_compare_with_dict(self):
        """Test comparison with a dict."""
        df = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        assert_frame_equal(df, {"a": [1, 2, 3], "b": ["x", "y", "z"]})  # Should not raise

    def test_unequal_values(self):
        """Test that unequal values are detected."""
        df1 = daft.from_pydict({"a": [1, 2, 3]})
        df2 = daft.from_pydict({"a": [1, 2, 4]})
        with pytest.raises(AssertionError, match="Column 'a' mismatch"):
            assert_frame_equal(df1, df2)

    def test_row_count_mismatch(self):
        """Test that row count mismatches are detected."""
        df1 = daft.from_pydict({"a": [1, 2]})
        df2 = daft.from_pydict({"a": [1, 2, 3]})
        with pytest.raises(AssertionError, match="Row count mismatch"):
            assert_frame_equal(df1, df2)

    def test_schema_mismatch_missing_column(self):
        """Test that schema mismatches (missing column) are detected."""
        df1 = daft.from_pydict({"a": [1, 2, 3]})
        df2 = daft.from_pydict({"b": [1, 2, 3]})
        with pytest.raises(AssertionError, match="Schema mismatch"):
            assert_frame_equal(df1, df2)

    def test_schema_mismatch_type(self):
        """Test that schema mismatches (type) are detected."""
        df1 = daft.from_pydict({"a": [1, 2, 3]})
        df2 = daft.from_pydict({"a": ["1", "2", "3"]})
        with pytest.raises(AssertionError, match="Schema mismatch"):
            assert_frame_equal(df1, df2)

    def test_ignore_row_order(self):
        """Test that row order is ignored when check_row_order=False."""
        df1 = daft.from_pydict({"a": [1, 2, 3]})
        df2 = daft.from_pydict({"a": [3, 1, 2]})
        assert_frame_equal(df1, df2, check_row_order=False)  # Should not raise

    def test_check_row_order(self):
        """Test that row order is checked when check_row_order=True."""
        df1 = daft.from_pydict({"a": [1, 2, 3]})
        df2 = daft.from_pydict({"a": [3, 1, 2]})
        with pytest.raises(AssertionError, match="Column 'a' mismatch"):
            assert_frame_equal(df1, df2, check_row_order=True)

    def test_ignore_column_order(self):
        """Test that column order is ignored when check_column_order=False."""
        df1 = daft.from_pydict({"a": [1, 2], "b": [3, 4]})
        df2 = daft.from_pydict({"b": [3, 4], "a": [1, 2]})
        assert_frame_equal(df1, df2, check_column_order=False)  # Should not raise

    def test_check_column_order(self):
        """Test that column order is checked when check_column_order=True."""
        df1 = daft.from_pydict({"a": [1, 2], "b": [3, 4]})
        df2 = daft.from_pydict({"b": [3, 4], "a": [1, 2]})
        with pytest.raises(AssertionError, match="Column order mismatch"):
            assert_frame_equal(df1, df2, check_column_order=True)

    def test_empty_dataframes(self):
        """Test comparison of empty DataFrames."""
        df1 = daft.from_pydict({"a": []})
        df2 = daft.from_pydict({"a": []})
        assert_frame_equal(df1, df2)  # Should not raise

    def test_float_tolerance(self):
        """Test floating point comparison with tolerance."""
        df1 = daft.from_pydict({"a": [1.0, 2.0, 3.0]})
        df2 = daft.from_pydict({"a": [1.0000001, 2.0, 3.0]})
        assert_frame_equal(df1, df2, rtol=1e-5, atol=1e-6)  # Should not raise

    def test_float_tolerance_exceeded(self):
        """Test floating point comparison when tolerance is exceeded."""
        df1 = daft.from_pydict({"a": [1.0, 2.0, 3.0]})
        df2 = daft.from_pydict({"a": [1.1, 2.0, 3.0]})
        with pytest.raises(AssertionError, match="Column 'a' mismatch"):
            assert_frame_equal(df1, df2, rtol=1e-5, atol=1e-8)

    def test_ignore_dtype(self):
        """Test that dtype is ignored when check_dtype=False."""
        df1 = daft.from_pydict({"a": [1, 2, 3]})
        df2 = daft.from_pydict({"a": [1.0, 2.0, 3.0]})
        assert_frame_equal(df1, df2, check_dtype=False, check_schema=False)  # Should not raise

    def test_null_values(self):
        """Test comparison with null values."""
        df1 = daft.from_pydict({"a": [1, None, 3]})
        df2 = daft.from_pydict({"a": [1, None, 3]})
        assert_frame_equal(df1, df2)  # Should not raise

    def test_invalid_actual_type(self):
        """Test that TypeError is raised for invalid actual type."""
        df = daft.from_pydict({"a": [1]})
        with pytest.raises(TypeError, match="actual must be a DataFrame"):
            assert_frame_equal("not a dataframe", df)

    def test_invalid_expected_type(self):
        """Test that TypeError is raised for invalid expected type."""
        df = daft.from_pydict({"a": [1]})
        with pytest.raises(TypeError, match="expected must be a DataFrame or dict"):
            assert_frame_equal(df, 123)


class TestAssertSeriesEqual:
    """Tests for assert_series_equal function."""

    def test_equal_series(self):
        """Test that equal Series pass."""
        from daft.series import Series

        s1 = Series.from_pylist([1, 2, 3], name="a")
        s2 = Series.from_pylist([1, 2, 3], name="a")
        assert_series_equal(s1, s2)  # Should not raise

    def test_unequal_values(self):
        """Test that unequal values are detected."""
        from daft.series import Series

        s1 = Series.from_pylist([1, 2, 3], name="a")
        s2 = Series.from_pylist([1, 2, 4], name="a")
        with pytest.raises(AssertionError, match="Series values mismatch"):
            assert_series_equal(s1, s2)

    def test_length_mismatch(self):
        """Test that length mismatches are detected."""
        from daft.series import Series

        s1 = Series.from_pylist([1, 2], name="a")
        s2 = Series.from_pylist([1, 2, 3], name="a")
        with pytest.raises(AssertionError, match="Series length mismatch"):
            assert_series_equal(s1, s2)

    def test_name_mismatch(self):
        """Test that name mismatches are detected."""
        from daft.series import Series

        s1 = Series.from_pylist([1, 2, 3], name="a")
        s2 = Series.from_pylist([1, 2, 3], name="b")
        with pytest.raises(AssertionError, match="Series name mismatch"):
            assert_series_equal(s1, s2, check_names=True)

    def test_ignore_name(self):
        """Test that names are ignored when check_names=False."""
        from daft.series import Series

        s1 = Series.from_pylist([1, 2, 3], name="a")
        s2 = Series.from_pylist([1, 2, 3], name="b")
        assert_series_equal(s1, s2, check_names=False)  # Should not raise

    def test_dtype_mismatch(self):
        """Test that dtype mismatches are detected."""
        from daft.series import Series

        s1 = Series.from_pylist([1, 2, 3], name="a")
        s2 = Series.from_pylist([1.0, 2.0, 3.0], name="a")
        with pytest.raises(AssertionError, match="Series dtype mismatch"):
            assert_series_equal(s1, s2, check_dtype=True)

    def test_ignore_dtype(self):
        """Test that dtype is ignored when check_dtype=False."""
        from daft.series import Series

        s1 = Series.from_pylist([1, 2, 3], name="a")
        s2 = Series.from_pylist([1.0, 2.0, 3.0], name="a")
        assert_series_equal(s1, s2, check_dtype=False, check_names=True)  # Should not raise

    def test_invalid_actual_type(self):
        """Test that TypeError is raised for invalid actual type."""
        from daft.series import Series

        s = Series.from_pylist([1], name="a")
        with pytest.raises(TypeError, match="actual must be a Series"):
            assert_series_equal("not a series", s)

    def test_invalid_expected_type(self):
        """Test that TypeError is raised for invalid expected type."""
        from daft.series import Series

        s = Series.from_pylist([1], name="a")
        with pytest.raises(TypeError, match="expected must be a Series"):
            assert_series_equal(s, "not a series")
