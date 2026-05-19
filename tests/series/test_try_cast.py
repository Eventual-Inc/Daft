"""Tests for Series.try_cast() - safe type conversion that returns null on failure."""

import pytest

import daft
from daft import DataType, Series


def test_try_cast_string_to_int_success():
    """Test try_cast with all valid string-to-int conversions."""
    s = Series.from_pylist(["1", "2", "3"]).rename("a")
    result = s.try_cast(DataType.int64())
    assert result.datatype() == DataType.int64()
    assert result.to_pylist() == [1, 2, 3]


def test_try_cast_string_to_int_with_failures():
    """Test try_cast with some invalid string-to-int conversions returns null."""
    s = Series.from_pylist(["1", "abc", "3", "xyz"]).rename("a")
    result = s.try_cast(DataType.int64())
    assert result.datatype() == DataType.int64()
    assert result.to_pylist() == [1, None, 3, None]


def test_try_cast_string_to_float_with_failures():
    """Test try_cast with some invalid string-to-float conversions returns null."""
    s = Series.from_pylist(["1.5", "abc", "3.14", None]).rename("a")
    result = s.try_cast(DataType.float64())
    assert result.datatype() == DataType.float64()
    assert result.to_pylist() == [1.5, None, 3.14, None]


def test_try_cast_preserves_nulls():
    """Test that existing nulls are preserved in try_cast."""
    s = Series.from_pylist(["1", None, "3"]).rename("a")
    result = s.try_cast(DataType.int64())
    assert result.datatype() == DataType.int64()
    assert result.to_pylist() == [1, None, 3]


def test_try_cast_same_type():
    """Test try_cast to the same type returns the same series."""
    s = Series.from_pylist([1, 2, 3]).rename("a")
    result = s.try_cast(DataType.int64())
    assert result.datatype() == DataType.int64()
    assert result.to_pylist() == [1, 2, 3]


def test_try_cast_int_to_string():
    """Test try_cast int to string always succeeds."""
    s = Series.from_pylist([1, 2, 3]).rename("a")
    result = s.try_cast(DataType.string())
    assert result.datatype() == DataType.string()
    assert result.to_pylist() == ["1", "2", "3"]


def test_try_cast_all_failures():
    """Test try_cast where all values fail returns all nulls."""
    s = Series.from_pylist(["abc", "def", "ghi"]).rename("a")
    result = s.try_cast(DataType.int64())
    assert result.datatype() == DataType.int64()
    assert result.to_pylist() == [None, None, None]


def test_try_cast_empty_series():
    """Test try_cast on empty series."""
    s = Series.from_pylist([]).rename("a").cast(DataType.string())
    result = s.try_cast(DataType.int64())
    assert result.datatype() == DataType.int64()
    assert result.to_pylist() == []


def test_try_cast_expression():
    """Test try_cast via Expression API."""
    df = daft.from_pydict({"a": ["1", "abc", "3"]})
    result = df.select(df["a"].try_cast(DataType.int64())).collect()
    assert result.to_pydict() == {"a": [1, None, 3]}


def test_try_cast_sql():
    """Test TRY_CAST via SQL."""
    df = daft.from_pydict({"a": ["1", "abc", "3"]})
    result = daft.sql("SELECT TRY_CAST(a AS BIGINT) AS a FROM df", **{"df": df}).collect()
    assert result.to_pydict() == {"a": [1, None, 3]}
