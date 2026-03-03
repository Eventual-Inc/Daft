"""Tests for duplicate CTE name detection in SQL WITH clauses.

Validates that Daft's SQL engine correctly raises an error when duplicate
CTE names are used in a single WITH clause (issue #6325).
"""

from __future__ import annotations

import pytest

import daft


def test_duplicate_cte_names_raises_error():
    """Duplicate CTE names in a WITH clause should raise an error."""
    with pytest.raises(Exception, match="Duplicate CTE name"):
        daft.sql(
            """
            WITH cte1 AS (SELECT * FROM df),
                 cte1 AS (SELECT * FROM df)
            SELECT * FROM cte1
            """,
            df=daft.from_pydict({"a": [1, 2, 3]}),
        )


def test_duplicate_cte_names_error_message_contains_name():
    """Error message should include the duplicated CTE name."""
    with pytest.raises(Exception, match="Duplicate CTE name: 'my_cte'"):
        daft.sql(
            """
            WITH my_cte AS (SELECT * FROM df),
                 my_cte AS (SELECT * FROM df)
            SELECT * FROM my_cte
            """,
            df=daft.from_pydict({"a": [1, 2, 3]}),
        )


def test_single_cte_works():
    """A single CTE should continue to work without errors."""
    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
    actual = (
        daft.sql(
            """
            WITH cte1 AS (SELECT * FROM df)
            SELECT * FROM cte1
            """
        )
        .collect()
        .to_pydict()
    )
    expected = df.collect().to_pydict()
    assert actual == expected


def test_multiple_distinct_ctes_work():
    """Multiple CTEs with distinct names should work without errors."""
    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
    actual = (
        daft.sql(
            """
            WITH cte1 AS (SELECT a FROM df),
                 cte2 AS (SELECT b FROM df)
            SELECT cte1.a, cte2.b
            FROM cte1
            JOIN cte2 ON cte1.a = cte2.b - 3
            """,
            df=df,
        )
        .collect()
        .to_pydict()
    )
    assert actual == {"a": [1, 2, 3], "b": [4, 5, 6]}


def test_three_distinct_ctes_work():
    """Three distinct CTEs should all work without errors."""
    df = daft.from_pydict({"x": [10, 20, 30]})
    actual = (
        daft.sql(
            """
            WITH a AS (SELECT x FROM df),
                 b AS (SELECT x FROM df),
                 c AS (SELECT x FROM df)
            SELECT * FROM a
            """,
            df=df,
        )
        .collect()
        .to_pydict()
    )
    assert actual == {"x": [10, 20, 30]}


def test_duplicate_cte_names_case_sensitive():
    """CTE names are case-sensitive: 'cte' and 'CTE' should be treated as different names."""
    df = daft.from_pydict({"a": [1, 2, 3]})
    # Different cases should NOT raise an error since SQL identifiers
    # preserve case as parsed
    actual = (
        daft.sql(
            """
            WITH cte AS (SELECT * FROM df),
                 CTE AS (SELECT * FROM df)
            SELECT * FROM cte
            """
        )
        .collect()
        .to_pydict()
    )
    expected = df.collect().to_pydict()
    assert actual == expected


def test_duplicate_cte_names_exact_case_raises():
    """Same CTE name with exact same case should raise an error."""
    with pytest.raises(Exception, match="Duplicate CTE name: 'MyCte'"):
        daft.sql(
            """
            WITH MyCte AS (SELECT * FROM df),
                 MyCte AS (SELECT * FROM df)
            SELECT * FROM MyCte
            """,
            df=daft.from_pydict({"a": [1, 2, 3]}),
        )


def test_duplicate_cte_among_three():
    """Duplicate detection should work when the duplicate is not the first pair."""
    with pytest.raises(Exception, match="Duplicate CTE name: 'cte2'"):
        daft.sql(
            """
            WITH cte1 AS (SELECT * FROM df),
                 cte2 AS (SELECT * FROM df),
                 cte2 AS (SELECT * FROM df)
            SELECT * FROM cte1
            """,
            df=daft.from_pydict({"a": [1, 2, 3]}),
        )


def test_duplicate_cte_with_different_queries():
    """Duplicate CTE names should raise an error even if queries differ."""
    with pytest.raises(Exception, match="Duplicate CTE name: 'cte1'"):
        daft.sql(
            """
            WITH cte1 AS (SELECT a FROM df),
                 cte1 AS (SELECT b FROM df)
            SELECT * FROM cte1
            """,
            df=daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]}),
        )


def test_duplicate_cte_with_column_aliases():
    """Duplicate CTE names should raise an error even with column aliases."""
    with pytest.raises(Exception, match="Duplicate CTE name: 'cte1'"):
        daft.sql(
            """
            WITH cte1 (x) AS (SELECT a FROM df),
                 cte1 (y) AS (SELECT b FROM df)
            SELECT * FROM cte1
            """,
            df=daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]}),
        )


def test_cte_name_reuse_across_separate_queries():
    """Same CTE name in separate SQL calls should not conflict."""
    df = daft.from_pydict({"a": [1, 2, 3]})

    result1 = (
        daft.sql(
            """
            WITH shared_name AS (SELECT * FROM df)
            SELECT * FROM shared_name
            """
        )
        .collect()
        .to_pydict()
    )

    result2 = (
        daft.sql(
            """
            WITH shared_name AS (SELECT * FROM df)
            SELECT * FROM shared_name
            """
        )
        .collect()
        .to_pydict()
    )

    expected = df.collect().to_pydict()
    assert result1 == expected
    assert result2 == expected
