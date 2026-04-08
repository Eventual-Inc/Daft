from __future__ import annotations

import pytest

import daft


def test_sample_with_percent():
    """Test SAMPLE with percentage syntax."""
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5], "b": ["x", "y", "z", "w", "v"]})

    # Sample 50% of rows
    result = daft.sql("SELECT * FROM df SAMPLE (50 PERCENT)", df=df)
    result.collect()

    # Should return approximately 50% of rows (2-3 rows for 5 rows)
    assert 1 <= len(result) <= 5
    assert result.column_names == ["a", "b"]


def test_sample_with_fraction():
    """Test SAMPLE with fraction syntax (0.0-1.0)."""
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5], "b": ["x", "y", "z", "w", "v"]})

    # Sample with fraction 0.4 (40%)
    result = daft.sql("SELECT * FROM df SAMPLE (0.4)", df=df)
    result.collect()

    # Should return approximately 40% of rows
    assert 0 <= len(result) <= 5
    assert result.column_names == ["a", "b"]


def test_sample_with_rows():
    """Test SAMPLE with ROWS unit."""
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5], "b": ["x", "y", "z", "w", "v"]})

    # Sample exactly 3 rows
    result = daft.sql("SELECT * FROM df SAMPLE (3 ROWS)", df=df)
    result.collect()

    # Should return exactly 3 rows
    assert len(result) == 3
    assert result.column_names == ["a", "b"]


def test_sample_with_seed():
    """Test SAMPLE with seed for reproducibility."""
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5], "b": ["x", "y", "z", "w", "v"]})

    # Sample with seed - should produce same results
    result1 = daft.sql("SELECT * FROM df SAMPLE (50 PERCENT) REPEATABLE (42)", df=df)
    result2 = daft.sql("SELECT * FROM df SAMPLE (50 PERCENT) REPEATABLE (42)", df=df)

    result1.collect()
    result2.collect()

    # Results should be identical with same seed
    assert result1.to_pydict() == result2.to_pydict()


def test_sample_with_alias():
    """Test SAMPLE works with table alias."""
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5], "b": ["x", "y", "z", "w", "v"]})

    # Sample with alias (alias before SAMPLE)
    result = daft.sql("SELECT t.* FROM df AS t SAMPLE (50 PERCENT)", df=df)
    result.collect()

    assert 1 <= len(result) <= 5
    assert result.column_names == ["a", "b"]


def test_sample_in_subquery():
    """Test SAMPLE works in subquery context."""
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5], "b": ["x", "y", "z", "w", "v"]})

    # Sample in subquery
    result = daft.sql("SELECT * FROM (SELECT * FROM df SAMPLE (50 PERCENT)) WHERE a > 2", df=df)
    result.collect()

    # All rows should have a > 2
    if len(result) > 0:
        assert all(val > 2 for val in result.to_pydict()["a"])


def test_sample_with_where():
    """Test SAMPLE combined with WHERE clause."""
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5], "b": ["x", "y", "z", "w", "v"]})

    # Sample first, then WHERE is applied after
    result = daft.sql("SELECT * FROM df SAMPLE (50 PERCENT) WHERE a > 2", df=df)
    result.collect()

    # All rows should have a > 2
    if len(result) > 0:
        assert all(val > 2 for val in result.to_pydict()["a"])


def test_sample_with_order_by():
    """Test SAMPLE combined with ORDER BY."""
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5], "b": ["x", "y", "z", "w", "v"]})

    # Sample then order
    result = daft.sql("SELECT * FROM df SAMPLE (50 PERCENT) ORDER BY a", df=df)
    result.collect()

    # Results should be ordered
    if len(result) > 1:
        values = result.to_pydict()["a"]
        assert values == sorted(values)


def test_sample_with_limit():
    """Test SAMPLE combined with LIMIT."""
    df = daft.from_pydict({"a": list(range(1, 101)), "b": ["x"] * 100})

    # Sample then limit
    result = daft.sql("SELECT * FROM df SAMPLE (50 PERCENT) LIMIT 5", df=df)
    result.collect()

    # Should return at most 5 rows
    assert len(result) <= 5


def test_sample_boundary_values():
    """Test SAMPLE with boundary values (0% and 100%)."""
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5], "b": ["x", "y", "z", "w", "v"]})

    # Sample 0% - should return 0 rows
    result0 = daft.sql("SELECT * FROM df SAMPLE (0 PERCENT)", df=df)
    result0.collect()
    assert len(result0) == 0

    # Sample 100% - should return all rows
    result100 = daft.sql("SELECT * FROM df SAMPLE (100 PERCENT)", df=df)
    result100.collect()
    assert len(result100) == 5


def test_sample_with_join():
    """Test SAMPLE with JOIN."""
    df1 = daft.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    df2 = daft.from_pydict({"a": [1, 2, 3], "c": ["p", "q", "r"]})

    # Sample one table in join
    result = daft.sql(
        "SELECT * FROM df1 SAMPLE (100 PERCENT) JOIN df2 ON df1.a = df2.a",
        df1=df1,
        df2=df2,
    )
    result.collect()

    assert len(result) == 3
    # JOIN adds prefixed column names
    assert "b" in result.column_names
    assert "c" in result.column_names
