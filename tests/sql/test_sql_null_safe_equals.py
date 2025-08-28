from __future__ import annotations

import pytest

import daft
from daft.sql import SQLCatalog
from tests.utils import sort_pydict


@pytest.mark.parametrize(
    "query,expected",
    [
        ("SELECT * FROM df1 WHERE val <=> 20", {"id": [2], "val": [20]}),
        ("SELECT * FROM df1 WHERE val <=> NULL", {"id": [3], "val": [None]}),
        (
            "SELECT df1.id, df1.val, df2.score FROM df1 JOIN df2 ON df1.id <=> df2.id",
            {"id": [2, 1, None], "val": [20, 10, 40], "score": [0.2, 0.1, 0.3]},
        ),
        (
            "SELECT * FROM df1 WHERE val <=> 10 OR val <=> NULL",
            {"id": [3, 1], "val": [None, 10]},  # Matches both 10 and NULL values
        ),
    ],
)
def test_null_safe_equals_basic(query, expected):
    """Test basic null-safe equality operator (<=>)."""
    df1 = daft.from_pydict({"id": [1, 2, 3, None], "val": [10, 20, None, 40]})
    df2 = daft.from_pydict({"id": [1, 2, None, 4], "score": [0.1, 0.2, 0.3, 0.4]})

    catalog = SQLCatalog({"df1": df1, "df2": df2})
    result = daft.sql(query, catalog).to_pydict()
    assert sort_pydict(result, "id") == expected


@pytest.mark.parametrize(
    "query,expected",
    [
        ("SELECT * FROM df WHERE NOT (val <=> NULL)", {"id": [3, 1, None], "val": [30, 10, 40]}),
        ("SELECT * FROM df WHERE val <=> 10 OR id > 2", {"id": [3, 1], "val": [30, 10]}),
        (
            "SELECT *, CASE WHEN val <=> NULL THEN 'is_null' ELSE 'not_null' END as val_status FROM df",
            {
                "id": [3, 2, 1, None, None],
                "val": [30, None, 10, 40, None],
                "val_status": ["not_null", "is_null", "not_null", "not_null", "is_null"],
            },
        ),
    ],
)
def test_null_safe_equals_complex(query, expected):
    """Test complex expressions using null-safe equality."""
    df = daft.from_pydict({"id": [3, 2, 1, None, None], "val": [30, None, 10, 40, None]})

    catalog = SQLCatalog({"df": df})
    result = daft.sql(query, catalog).to_pydict()
    assert sort_pydict(result, "id") == expected


@pytest.mark.parametrize(
    "query,expected",
    [
        (
            "SELECT * FROM df WHERE int_val <=> 1",
            {"int_val": [1], "str_val": ["a"], "bool_val": [True], "float_val": [1.1]},
        ),
        (
            "SELECT * FROM df WHERE str_val <=> 'c'",
            {"int_val": [3], "str_val": ["c"], "bool_val": [False], "float_val": [3.3]},
        ),
        (
            "SELECT * FROM df WHERE bool_val <=> false",
            {"int_val": [3], "str_val": ["c"], "bool_val": [False], "float_val": [3.3]},
        ),
        (
            "SELECT * FROM df WHERE float_val <=> 1.1",
            {"int_val": [1], "str_val": ["a"], "bool_val": [True], "float_val": [1.1]},
        ),
        (
            "SELECT * FROM df WHERE int_val <=> NULL",
            {"int_val": [None], "str_val": [None], "bool_val": [None], "float_val": [None]},
        ),
        (
            "SELECT * FROM df WHERE str_val <=> NULL",
            {"int_val": [None], "str_val": [None], "bool_val": [None], "float_val": [None]},
        ),
        (
            "SELECT * FROM df WHERE bool_val <=> NULL",
            {"int_val": [None], "str_val": [None], "bool_val": [None], "float_val": [None]},
        ),
        (
            "SELECT * FROM df WHERE float_val <=> NULL",
            {"int_val": [None], "str_val": [None], "bool_val": [None], "float_val": [None]},
        ),
    ],
)
def test_null_safe_equals_types(query, expected):
    """Test null-safe equality with different data types."""
    df = daft.from_pydict(
        {
            "int_val": [1, None, 3],
            "str_val": ["a", None, "c"],
            "bool_val": [True, None, False],
            "float_val": [1.1, None, 3.3],
        }
    )

    catalog = SQLCatalog({"df": df})
    result = daft.sql(query, catalog).to_pydict()
    assert result == expected
