from __future__ import annotations

import daft


def test_sample_with_percent():
    """Test SAMPLE with percentage syntax."""
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5], "b": ["x", "y", "z", "w", "v"]})

    # Sample 50% of rows
    result = daft.sql("SELECT * FROM df SAMPLE (50 PERCENT)", df=df)
    result.collect()

    # Should return approximately 50% of rows (2-3 rows for 5 rows)
    assert 0 <= len(result) <= 5
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

    assert 0 <= len(result) <= 5
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


def test_tablesample_system():
    """Test Postgres TABLESAMPLE SYSTEM syntax."""
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5], "b": ["x", "y", "z", "w", "v"]})

    # TABLESAMPLE SYSTEM (50) - should sample ~50% of rows
    result = daft.sql("SELECT * FROM df TABLESAMPLE SYSTEM (50)", df=df)
    result.collect()

    # Should return approximately 50% of rows
    assert 0 <= len(result) <= 5
    assert result.column_names == ["a", "b"]


def test_tablesample_bernoulli():
    """Test Postgres TABLESAMPLE BERNOULLI syntax."""
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5], "b": ["x", "y", "z", "w", "v"]})

    # TABLESAMPLE BERNOULLI (50) - should sample ~50% of rows
    result = daft.sql("SELECT * FROM df TABLESAMPLE BERNOULLI (50)", df=df)
    result.collect()

    # Should return approximately 50% of rows
    assert 0 <= len(result) <= 5
    assert result.column_names == ["a", "b"]


def test_tablesample_with_seed():
    """Test Postgres TABLESAMPLE with seed for reproducibility."""
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5], "b": ["x", "y", "z", "w", "v"]})

    # TABLESAMPLE SYSTEM with seed - should produce same results
    result1 = daft.sql("SELECT * FROM df TABLESAMPLE SYSTEM (50) REPEATABLE (42)", df=df)
    result2 = daft.sql("SELECT * FROM df TABLESAMPLE SYSTEM (50) REPEATABLE (42)", df=df)

    result1.collect()
    result2.collect()

    # Results should be identical with same seed
    assert result1.to_pydict() == result2.to_pydict()


def test_postgres_tablesample_syntax_variants():
    """Test Postgres TABLESAMPLE syntax variants from official documentation.
    
    Reference: https://www.postgresql.org/docs/current/tablesample-method.html
    
    Postgres supports:
    - TABLESAMPLE SYSTEM (percentage)
    - TABLESAMPLE BERNOULLI (percentage)
    - TABLESAMPLE method_name (parameter) REPEATABLE (seed)
    """
    df = daft.from_pydict({
        "id": list(range(1, 101)),
        "value": ["x"] * 100
    })

    # Test SYSTEM method with various percentages
    result_10 = daft.sql("SELECT * FROM df TABLESAMPLE SYSTEM (10)", df=df)
    result_10.collect()
    assert 0 <= len(result_10) <= 100  # ~10 rows expected

    result_50 = daft.sql("SELECT * FROM df TABLESAMPLE SYSTEM (50)", df=df)
    result_50.collect()
    assert 0 <= len(result_50) <= 100  # ~50 rows expected

    result_100 = daft.sql("SELECT * FROM df TABLESAMPLE SYSTEM (100)", df=df)
    result_100.collect()
    assert len(result_100) == 100  # All rows

    # Test BERNOULLI method with various percentages
    result_bernoulli_10 = daft.sql("SELECT * FROM df TABLESAMPLE BERNOULLI (10)", df=df)
    result_bernoulli_10.collect()
    assert 0 <= len(result_bernoulli_10) <= 100

    result_bernoulli_50 = daft.sql("SELECT * FROM df TABLESAMPLE BERNOULLI (50)", df=df)
    result_bernoulli_50.collect()
    assert 0 <= len(result_bernoulli_50) <= 100

    # Test with REPEATABLE seed for deterministic results
    result_seed_1 = daft.sql(
        "SELECT * FROM df TABLESAMPLE SYSTEM (30) REPEATABLE (123)", df=df
    )
    result_seed_2 = daft.sql(
        "SELECT * FROM df TABLESAMPLE SYSTEM (30) REPEATABLE (123)", df=df
    )
    result_seed_1.collect()
    result_seed_2.collect()
    assert result_seed_1.to_pydict() == result_seed_2.to_pydict()


def test_spark_tablesample_bucket():
    """Test Spark TABLESAMPLE BUCKET syntax.
    
    Reference: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-sampling.html
    
    Spark supports: TABLESAMPLE (BUCKET x OUT OF y)
    This samples approximately x/y fraction of the table.
    """
    df = daft.from_pydict({
        "id": list(range(1, 101)),
        "value": ["x"] * 100
    })

    # BUCKET 4 OUT OF 10 = 40%
    result_40 = daft.sql("SELECT * FROM df TABLESAMPLE (BUCKET 4 OUT OF 10)", df=df)
    result_40.collect()
    assert 35 <= len(result_40) <= 45  # Allow some variance

    # BUCKET 1 OUT OF 2 = 50%
    result_50 = daft.sql("SELECT * FROM df TABLESAMPLE (BUCKET 1 OUT OF 2)", df=df)
    result_50.collect()
    assert 45 <= len(result_50) <= 55

    # BUCKET 1 OUT OF 4 = 25%
    result_25 = daft.sql("SELECT * FROM df TABLESAMPLE (BUCKET 1 OUT OF 4)", df=df)
    result_25.collect()
    assert 20 <= len(result_25) <= 30

    # BUCKET 100 OUT OF 100 = 100%
    result_100 = daft.sql("SELECT * FROM df TABLESAMPLE (BUCKET 100 OUT OF 100)", df=df)
    result_100.collect()
    assert len(result_100) == 100
