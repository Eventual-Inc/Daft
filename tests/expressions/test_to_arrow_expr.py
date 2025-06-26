from __future__ import annotations

from daft import col, lit
from daft.dependencies import pa, pc


def assert_eq(lhs: pc.Expression, rhs: str):
    """I don't know of a better way, but this can be switched a later time."""
    assert str(lhs) == rhs


def test_col():
    expr = col("a").to_arrow_expr()

    assert_eq(expr, "a")


def test_and():
    expr = lit(True) & lit(False)
    expr = expr.to_arrow_expr()

    assert_eq(expr, "(true and false)")


def test_or():
    expr = lit(True) | lit(False)
    expr = expr.to_arrow_expr()

    assert_eq(expr, "(true or false)")


def test_not():
    expr = ~lit(True)
    expr = expr.to_arrow_expr()

    assert_eq(expr, "invert(true)")


def test_equal():
    expr = col("a") == col("b")
    expr = expr.to_arrow_expr()

    assert_eq(expr, "(a == b)")


def test_not_equal():
    expr = col("a") != col("b")
    expr = expr.to_arrow_expr()

    assert_eq(expr, "(a != b)")


def test_less_than():
    expr = col("a") < col("b")
    expr = expr.to_arrow_expr()

    assert_eq(expr, "(a < b)")


def test_less_than_or_equal():
    expr = col("a") <= col("b")
    expr = expr.to_arrow_expr()

    assert_eq(expr, "(a <= b)")


def test_greater_than():
    expr = col("a") > col("b")
    expr = expr.to_arrow_expr()

    assert_eq(expr, "(a > b)")


def test_greater_than_or_equal():
    expr = col("a") >= col("b")
    expr = expr.to_arrow_expr()

    assert_eq(expr, "(a >= b)")


def test_between():
    expr = col("a").between(col("b"), col("c"))
    expr = expr.to_arrow_expr()

    assert_eq(expr, "((b <= a) and (a <= c))")


def test_is_in():
    expr = col("a").is_in([1, 2, 3])
    expr = expr.to_arrow_expr()

    expected = pc.field("a").isin([1, 2, 3])
    assert_eq(expr, str(expected))


def test_is_null():
    expr = col("a").is_null()
    expr = expr.to_arrow_expr()

    expected = pc.field("a").is_null()
    assert_eq(expr, str(expected))


def test_not_null():
    expr = col("a").not_null()
    expr = expr.to_arrow_expr()

    expected = ~pc.field("a").is_null()
    assert_eq(expr, str(expected))


def test_alias():
    expr = col("a").alias("b")
    expr = expr.to_arrow_expr()

    assert_eq(expr, "a")


def test_cast():
    expr = col("a").cast("int64")
    expr = expr.to_arrow_expr()

    expected = pc.field("a").cast(pa.int64())
    assert_eq(expr, str(expected))


def test_lit():
    expr = lit(42)
    expr = expr.to_arrow_expr()

    assert_eq(expr, "42")
