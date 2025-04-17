from __future__ import annotations

import pytest

from typing import TYPE_CHECKING

from daft.io.pushdowns import Expr, Literal, Reference, Term
from daft.daft import Pushdowns as PyPushdowns
from daft.expressions import col, lit

if TYPE_CHECKING:
    from daft.expressions import Expression


def test_print_literal():
    # str
    assert str(Literal("hello")) == '"hello"'
    assert str(Literal("🤠🤠")) == '"🤠🤠"'

    # int
    assert str(Literal(1)) == "1"
    assert str(Literal(0)) == "0"
    assert str(Literal(-1)) == "-1"

    # float
    assert str(Literal(2.0)) == "2.00"
    assert str(Literal(3.14159)) == "3.14"

    # bool
    assert str(Literal(True)) == "true"
    assert str(Literal(False)) == "false"

    # nil/null/none
    assert str(Literal(None)) == "null"


def test_print_reference():
    assert str(Reference("a")) == "a"
    assert str(Reference("a")) == "a"


def test_print_expr():
    # zero arguments
    assert str(Expr("f")) == "(f)"

    # positional arguments
    assert str(Expr("f", 42)) == "(f 42)"
    assert str(Expr("f", "hello")) == '(f "hello")'
    assert str(Expr("f", 1, 2.5, "test")) == '(f 1 2.50 "test")'

    # named arguments
    assert str(Expr("f", value=True)) == "(f value::true)"
    assert str(Expr("f", x=10, y=20.5, label="data")) == '(f x::10 y::20.50 label::"data")'

    # mixed arguments
    assert str(Expr("f", 100, False, name="example", flag=True)) == '(f 100 false name::"example" flag::true)'

    # nested expressions
    assert str(Expr("f", Expr("g", 5))) == "(f (g 5))"

    # literal objects
    assert str(Expr("f", Literal(42), keyword=Literal("value"))) == '(f 42 keyword::"value")'


def test_expr_getitem():
    # positional arguments
    expr = Expr("f", 1, 2, 3)
    assert expr[0] == Literal(1)
    assert expr[1] == Literal(2)
    assert expr[2] == Literal(3)
    
    # named arguments
    expr = Expr("g", x=10, y=20)
    assert expr["x"] == Literal(10)
    assert expr["y"] == Literal(20)

    # mixed arguments
    expr = Expr("h", 100, 200, name="test")
    assert expr[0] == Literal(100)
    assert expr[1] == Literal(200)
    assert expr["name"] == Literal("test")
    
    # nested expressions
    expr = Expr("nested", Expr("inner", 5))
    assert expr[0] == Expr("inner", 5)
    assert expr[0][0] == Literal(5)


def _term(expr: Expression) -> Term:
    return PyPushdowns._to_term(expr._expr)


def test_pyexpr_lit():
    # null/nil/none
    assert _term(lit(None)) == Literal(None)

    # bool
    assert _term(lit(True)) == Literal(True)
    assert _term(lit(False)) == Literal(False)

    # int
    assert _term(lit(0)) == Literal(0)
    assert _term(lit(1)) == Literal(1)
    assert _term(lit(-1)) == Literal(-1)

    # float
    assert _term(lit(0.0)) == Literal(0)
    assert _term(lit(1.0)) == Literal(1.0)
    assert _term(lit(-1.0)) == Literal(-1.0)

    # string
    assert _term(lit(("hello"))) == Literal("hello")
    assert _term(lit(("🤠🤠"))) == Literal("🤠🤠")


def test_pyexpr_col():
    assert _term(col("a")) == Reference("a")


def test_pyexpr_alias():
    assert _term(col("a").alias("xyz")) == Expr("alias", "xyz", Reference("a"))
    assert _term(lit(42).alias("ans")) == Expr("alias", "ans", 42)


def test_pyexpr_not():
    assert _term(~lit(42)) == Expr("not", 42)


def test_pyexpr_binary_ops():
    # comparison operators
    assert _term(lit(1) == lit(2)) == Expr("=", 1, 2)
    assert _term(lit(1) != lit(2)) == Expr("!=", 1, 2)
    assert _term(lit(1) < lit(2)) == Expr("<", 1, 2)
    assert _term(lit(1) <= lit(2)) == Expr("<=", 1, 2)
    assert _term(lit(1) > lit(2)) == Expr(">", 1, 2)
    assert _term(lit(1) >= lit(2)) == Expr(">=", 1, 2)
    assert _term(lit(1).eq_null_safe(lit(2))) == Expr("eq_null_safe", 1, 2)
    
    # arithmetic operators
    assert _term(lit(1) + lit(2)) == Expr("+", 1, 2)
    assert _term(lit(1) - lit(2)) == Expr("-", 1, 2)
    assert _term(lit(1) * lit(2)) == Expr("*", 1, 2)
    assert _term(lit(1) / lit(2)) == Expr("/", 1, 2)
    assert _term(lit(1) // lit(2)) == Expr("quotient", 1, 2)
    assert _term(lit(1) % lit(2)) == Expr("mod", 1, 2)
    
    # logical operators
    assert _term(lit(True) & lit(False)) == Expr("and", True, False)
    assert _term(lit(True) | lit(False)) == Expr("or", True, False)
    assert _term(lit(True) ^ lit(False)) == Expr("xor", True, False)

    # bitwise operators are conflated with the logical operators, so skip.
    # assert _term(lit(1).bitwise_and(lit(2))) == Expr("&", 1, 2)
    # assert _term(lit(1).bitwise_or(lit(2))) == Expr("|", 1, 2)
    # assert _term(lit(1).bitwise_xor(lit(2))) == Expr("^", 1, 2)

    # bitwise shifts are ok
    assert _term(lit(1).shift_left(lit(2))) == Expr("lshift", 1, 2)
    assert _term(lit(1).shift_right(lit(2))) == Expr("rshift", 1, 2)


def test_pyexpr_null_ops():
    assert _term(col("a").is_null()) == Expr("is_null", Reference("a"))
    assert _term(col("a").not_null()) == Expr("not_null", Reference("a"))


def test_pyexpr_between():
    assert _term(col("a").between(lit(1), lit(10))) == Expr("between", Reference("a"), 1, 10)
    assert _term(lit(5).between(lit(1), lit(10))) == Expr("between", 5, 1, 10)


def test_pyexpr_builtin_functions():
    assert _term(lit(1).abs()) == Expr("abs", 1)
    assert _term(lit(1).floor()) == Expr("floor", 1)


def test_pyexpr_partition_functions():
    assert _term(col("a").partitioning.years()) == Expr("years", Reference("a"))
    assert _term(col("a").partitioning.months()) == Expr("months", Reference("a"))
    assert _term(col("a").partitioning.days()) == Expr("days", Reference("a"))
    assert _term(col("a").partitioning.hours()) == Expr("hours", Reference("a"))
    assert _term(col("a").partitioning.iceberg_bucket(10)) == Expr("iceberg_bucket", Reference("a"))
    assert _term(col("a").partitioning.iceberg_truncate(5)) == Expr("iceberg_truncate", Reference("a"))
