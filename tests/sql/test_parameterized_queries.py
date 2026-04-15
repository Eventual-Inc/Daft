from __future__ import annotations

import pytest

import daft
from daft import sql


def test_parameterized_query_with_question_mark():
    """Test auto-incremented parameters with ? placeholder."""
    df = daft.from_pydict(
        {"name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35], "gender": ["female", "male", "male"]}
    )
    result = sql("SELECT * FROM df WHERE age > ? AND gender = ?", df=df, params=[32, "male"])
    result_df = result.collect()

    assert len(result_df) == 1
    assert result_df.to_pydict()["name"][0] == "Charlie"


def test_parameterized_query_with_positional():
    """Test positional parameters with $1, $2 placeholders."""
    df = daft.from_pydict({"name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]})
    result = sql("SELECT * FROM df WHERE age > $1 AND name = $2", df=df, params=[20, "Alice"])
    result_df = result.collect()

    assert len(result_df) == 1
    assert result_df.to_pydict()["name"][0] == "Alice"


def test_parameterized_query_with_named():
    """Test named parameters with :name placeholder."""
    df = daft.from_pydict({"name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]})
    result = sql("SELECT * FROM df WHERE age > :age AND name = :name", df=df, params={"age": 20, "name": "Alice"})
    result_df = result.collect()

    assert len(result_df) == 1
    assert result_df.to_pydict()["name"][0] == "Alice"


def test_parameterized_query_multiple_placeholders():
    """Test query with multiple ? placeholders."""
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]})
    result = sql("SELECT * FROM df WHERE a > ? AND a < ? AND b > ?", df=df, params=[1, 5, 25])
    result_df = result.collect()

    assert len(result_df) == 2


def test_parameterized_query_positional_out_of_order():
    """Test $n placeholders used out of order."""
    df = daft.from_pydict({"a": [1, 2, 3], "b": [10, 20, 30]})
    result = sql("SELECT * FROM df WHERE a = $2 AND b = $1", df=df, params=[30, 3])
    result_df = result.collect()

    assert len(result_df) == 1
    assert result_df.to_pydict()["a"][0] == 3


def test_parameterized_query_positional_repeated():
    """Test same $n placeholder used multiple times."""
    df = daft.from_pydict({"a": [1, 2, 3], "b": [10, 20, 30]})
    result = sql("SELECT * FROM df WHERE a > $1 OR a < $1", df=df, params=[2])
    result_df = result.collect()

    assert len(result_df) == 2


def test_parameterized_query_no_params():
    """Test that queries work without parameters for backward compatibility."""
    df = daft.from_pydict({"name": ["Alice", "Bob"], "age": [25, 30]})
    result = sql("SELECT * FROM df WHERE age > 20", df=df)
    result_df = result.collect()

    assert len(result_df) == 2


def test_parameterized_query_mixed_styles_error():
    """Test that mixing parameter styles raises an error."""
    df = daft.from_pydict({"a": [1, 2, 3], "b": [10, 20, 30]})

    # Mixing $n and ?
    with pytest.raises(ValueError, match="Cannot mix"):
        sql("SELECT * FROM df WHERE a > $1 AND b > ?", df=df, params=[1, 15])

    # Mixing ? and :name (with list params)
    with pytest.raises(ValueError, match="Cannot mix"):
        sql("SELECT * FROM df WHERE a > ? AND b = :val", df=df, params=[1])
