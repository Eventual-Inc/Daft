import pytest
import daft

def test_sql_agg_default_naming():
    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
    bindings = {"df": df}
    
    # Test sum
    assert daft.sql("SELECT sum(a) FROM df", **bindings).column_names == ["sum(a)"]
    
    # Test avg
    assert daft.sql("SELECT avg(a) FROM df", **bindings).column_names == ["avg(a)"]
    
    # Test min
    assert daft.sql("SELECT min(a) FROM df", **bindings).column_names == ["min(a)"]
    
    # Test max
    assert daft.sql("SELECT max(a) FROM df", **bindings).column_names == ["max(a)"]
    
    # Test count
    assert daft.sql("SELECT count(a) FROM df", **bindings).column_names == ["count(a)"]
    assert daft.sql("SELECT count(*) FROM df", **bindings).column_names == ["count"] # SQL count(*) is aliased to count by default in daft-sql
    
    # Test count distinct
    assert daft.sql("SELECT count(distinct a) FROM df", **bindings).column_names == ["count(distinct a)"]
    
    # Test stddev
    assert daft.sql("SELECT stddev(a) FROM df", **bindings).column_names == ["stddev(a)"]
    
    # Test product (if supported in SQL)
    # daft.sql supports product
    assert daft.sql("SELECT product(a) FROM df", **bindings).column_names == ["product(a)"]
    
    # Test bool_and / bool_or
    df_bool = daft.from_pydict({"c": [True, False, True]})
    bindings_bool = {"df": df_bool}
    assert daft.sql("SELECT bool_and(c) FROM df", **bindings_bool).column_names == ["bool_and(c)"]
    assert daft.sql("SELECT bool_or(c) FROM df", **bindings_bool).column_names == ["bool_or(c)"]

def test_sql_agg_explicit_alias():
    df = daft.from_pydict({"a": [1, 2, 3]})
    bindings = {"df": df}
    
    # Explicit alias
    assert daft.sql("SELECT sum(a) AS my_sum FROM df", **bindings).column_names == ["my_sum"]

def test_sql_agg_complex_expressions():
    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
    bindings = {"df": df}
    
    # BinaryOp
    assert daft.sql("SELECT sum(a + b) FROM df", **bindings).column_names == ["sum(a + b)"]
    assert daft.sql("SELECT avg(a * 2) FROM df", **bindings).column_names == ["avg(a * 2)"]
    
    # Literal
    assert daft.sql("SELECT sum(1) FROM df", **bindings).column_names == ["sum(1)"]

def test_sql_agg_supported_expressions_naming():
    """Test that all expressions supported by to_sql() produce correct names when aggregated in SQL."""
    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6], "c": [True, False, True]})
    bindings = {"df": df}

    # BinaryOp - Arithmetic
    assert daft.sql("SELECT sum(a + b) FROM df", **bindings).column_names == ["sum(a + b)"]
    assert daft.sql("SELECT sum(a - b) FROM df", **bindings).column_names == ["sum(a - b)"]
    assert daft.sql("SELECT sum(a * b) FROM df", **bindings).column_names == ["sum(a * b)"]
    assert daft.sql("SELECT sum(a / b) FROM df", **bindings).column_names == ["sum(a / b)"]
    assert daft.sql("SELECT sum(a % b) FROM df", **bindings).column_names == ["sum(a % b)"]

    # BinaryOp - Comparison (inside bool_and)
    assert daft.sql("SELECT bool_and(a > b) FROM df", **bindings).column_names == ["bool_and(a > b)"]
    assert daft.sql("SELECT bool_and(a >= b) FROM df", **bindings).column_names == ["bool_and(a >= b)"]
    assert daft.sql("SELECT bool_and(a < b) FROM df", **bindings).column_names == ["bool_and(a < b)"]
    assert daft.sql("SELECT bool_and(a <= b) FROM df", **bindings).column_names == ["bool_and(a <= b)"]
    assert daft.sql("SELECT bool_and(a = b) FROM df", **bindings).column_names == ["bool_and(a = b)"]
    assert daft.sql("SELECT bool_and(a != b) FROM df", **bindings).column_names == ["bool_and(a != b)"]

    # BinaryOp - Logical
    assert daft.sql("SELECT bool_and(c AND c) FROM df", **bindings).column_names == ["bool_and(c AND c)"]
    assert daft.sql("SELECT bool_and(c OR c) FROM df", **bindings).column_names == ["bool_and(c OR c)"]

    # BinaryOp - Bitwise
    # Note: SQL parser support for bitwise operators might vary.
    # Postgres uses <<, >>, #.
    # If daft.sql parser supports them:
    # assert daft.sql("SELECT sum(a << 1) FROM df", **bindings).column_names == ["sum(a << 1)"]
    # assert daft.sql("SELECT sum(a >> 1) FROM df", **bindings).column_names == ["sum(a >> 1)"]
    # assert daft.sql("SELECT sum(a # b) FROM df", **bindings).column_names == ["sum(a # b)"]

    # Not
    assert daft.sql("SELECT bool_and(NOT c) FROM df", **bindings).column_names == ["bool_and(NOT (c))"]

    # IsNull / NotNull
    assert daft.sql("SELECT bool_and(a IS NULL) FROM df", **bindings).column_names == ["bool_and((a) IS NULL)"]
    # IS NOT NULL is parsed as NOT (IS NULL)
    assert daft.sql("SELECT bool_and(a IS NOT NULL) FROM df", **bindings).column_names == ["bool_and(NOT ((a) IS NULL))"]
