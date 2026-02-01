import pytest
import daft
from daft import col, lit

def test_agg_default_naming():
    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})

    # Test sum
    assert df.select(col("a").sum()).column_names == ["sum(a)"]

    # Test mean (avg)
    assert df.select(col("a").mean()).column_names == ["avg(a)"]

    # Test min
    assert df.select(col("a").min()).column_names == ["min(a)"]

    # Test max
    assert df.select(col("a").max()).column_names == ["max(a)"]

    # Test count
    assert df.select(col("a").count()).column_names == ["count(a)"]
    assert df.select(col("a").count(mode="all")).column_names == ["count(1)"]

    # Test count distinct
    assert df.select(col("a").count_distinct()).column_names == ["count(distinct a)"]

    # Test stddev
    assert df.select(col("a").stddev()).column_names == ["stddev(a)"]

    # Test product
    assert df.select(col("a").product()).column_names == ["product(a)"]

    # Test bool_and / bool_or (requires boolean column)
    df_bool = daft.from_pydict({"c": [True, False, True]})
    assert df_bool.select(col("c").bool_and()).column_names == ["bool_and(c)"]
    assert df_bool.select(col("c").bool_or()).column_names == ["bool_or(c)"]

def test_agg_default_naming_groupby():
    df = daft.from_pydict({"g": ["x", "x", "y"], "a": [1, 2, 3]})

    agg_df = df.groupby("g").agg(
        col("a").sum(),
        col("a").mean(),
        col("a").min(),
        col("a").max(),
        col("a").count(),
    )

    expected_columns = {"g", "sum(a)", "avg(a)", "min(a)", "max(a)", "count(a)"}
    assert set(agg_df.column_names) == expected_columns

def test_agg_explicit_alias():
    df = daft.from_pydict({"a": [1, 2, 3]})

    # Explicit alias on aggregation
    assert df.select(col("a").sum().alias("my_sum")).column_names == ["my_sum"]

    # Explicit alias on input (should be preserved)
    assert df.select(col("a").alias("my_col").sum()).column_names == ["sum(my_col)"]

    # Explicit alias on input, then alias on aggregation
    assert df.select(col("a").alias("my_col").sum().alias("final_sum")).column_names == ["final_sum"]

def test_agg_complex_expressions():
    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})

    # BinaryOp
    assert df.select((col("a") + col("b")).sum()).column_names == ["sum(a + b)"]
    assert df.select((col("a") * 2).mean()).column_names == ["avg(a * 2)"]

    # Literal
    assert df.select(lit(1).sum()).column_names == ["sum(1)"]

    # Not / IsNull / NotNull (on boolean/nullable)
    df_bool = daft.from_pydict({"c": [True, False, None]})
    # Note: sum of boolean is not supported in all contexts, but count is
    assert df_bool.select((~col("c")).count()).column_names == ["count(NOT (c))"]
    assert df_bool.select(col("c").is_null().count()).column_names == ["count((c) IS NULL)"]
    assert df_bool.select(col("c").not_null().count()).column_names == ["count((c) IS NOT NULL)"]

def test_to_sql_supported_expressions_naming():
    """Test that all expressions supported by to_sql() produce correct names when aggregated."""
    df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6], "c": [True, False, True]})

    # Column (Resolved/Unresolved) - already covered by basic tests (sum(a))

    # Literal
    assert df.select(lit(1).sum()).column_names == ["sum(1)"]
    assert df.select(lit("s").count()).column_names == ["count('s')"]

    # Alias (recursive)
    # col("a").alias("x") -> alias is "x". sum(alias("x")) -> sum(x)
    assert df.select(col("a").alias("x").sum()).column_names == ["sum(x)"]

    # But if we have Alias inside an expression?
    # (col("a").alias("x") + 1).sum()
    # BinaryOp(Alias(a, x), 1). to_sql -> "x + 1".
    # So sum(x + 1).
    assert df.select((col("a").alias("x") + 1).sum()).column_names == ["sum(x + 1)"]

    # BinaryOp - Arithmetic
    assert df.select((col("a") + col("b")).sum()).column_names == ["sum(a + b)"]
    assert df.select((col("a") - col("b")).sum()).column_names == ["sum(a - b)"]
    assert df.select((col("a") * col("b")).sum()).column_names == ["sum(a * b)"]
    assert df.select((col("a") / col("b")).sum()).column_names == ["sum(a / b)"]
    assert df.select((col("a") % col("b")).sum()).column_names == ["sum(a % b)"]

    # BinaryOp - Comparison (inside count or bool_and)
    # (a > b) is boolean.
    assert df.select((col("a") > col("b")).bool_and()).column_names == ["bool_and(a > b)"]
    assert df.select((col("a") >= col("b")).bool_and()).column_names == ["bool_and(a >= b)"]
    assert df.select((col("a") < col("b")).bool_and()).column_names == ["bool_and(a < b)"]
    assert df.select((col("a") <= col("b")).bool_and()).column_names == ["bool_and(a <= b)"]
    assert df.select((col("a") == col("b")).bool_and()).column_names == ["bool_and(a = b)"]
    assert df.select((col("a") != col("b")).bool_and()).column_names == ["bool_and(a != b)"]

    # BinaryOp - Logical
    assert df.select((col("c") & col("c")).bool_and()).column_names == ["bool_and(c AND c)"]
    assert df.select((col("c") | col("c")).bool_and()).column_names == ["bool_and(c OR c)"]

    # BinaryOp - Bitwise (ShiftLeft, ShiftRight, Xor)
    # Note: Bitwise operators might not be supported on all types or in all contexts, but we check naming.
    # Assuming integer columns for bitwise ops.
    assert df.select((col("a") << 1).sum()).column_names == ["sum(a << 1)"]
    assert df.select((col("a") >> 1).sum()).column_names == ["sum(a >> 1)"]
    # Python ^ is Xor.
    assert df.select((col("a") ^ col("b")).sum()).column_names == ["sum(a # b)"] # Postgres uses # for XOR

    # Not
    assert df.select((~col("c")).bool_and()).column_names == ["bool_and(NOT (c))"]

    # IsNull / NotNull
    assert df.select(col("a").is_null().bool_and()).column_names == ["bool_and((a) IS NULL)"]
    assert df.select(col("a").not_null().bool_and()).column_names == ["bool_and((a) IS NOT NULL)"]

