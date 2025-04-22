from __future__ import annotations

from typing import TYPE_CHECKING

from daft.daft import Pushdowns as PyPushdowns
from daft.expressions import col, lit
from daft.io.pushdowns import Expr, Literal, Reference, Term
from daft.io.scan import ScanPushdowns
from daft.logical.schema import DataType as dt
from daft.logical.schema import Schema

if TYPE_CHECKING:
    from daft.expressions import Expression


def _term(expr: Expression, schema: Schema | None = None) -> Term:
    return PyPushdowns._to_term(expr._expr, schema._schema if schema else None)


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


###
# Sanity Translation Tests
###


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
    assert _term(lit("hello")) == Literal("hello")
    assert _term(lit("🤠🤠")) == Literal("🤠🤠")


def test_pyexpr_col():
    assert _term(col("a")) == Reference("a")


def test_pyexpr_alias():
    assert _term(col("a").alias("xyz")) == Expr("alias", "xyz", Reference("a"))
    assert _term(lit(42).alias("answer")) == Expr("alias", "answer", 42)


def test_pyexpr_not():
    assert _term(~lit(42)) == Expr("not", 42)


def test_pyexpr_predicates():
    # logical operators
    assert _term(lit(True) & lit(False)) == Expr("and", True, False)
    assert _term(lit(True) | lit(False)) == Expr("or", True, False)

    # comparisons
    assert _term(lit(1) == lit(2)) == Expr("=", 1, 2)
    assert _term(lit(1) != lit(2)) == Expr("!=", 1, 2)
    assert _term(lit(1) < lit(2)) == Expr("<", 1, 2)
    assert _term(lit(1) <= lit(2)) == Expr("<=", 1, 2)
    assert _term(lit(1) > lit(2)) == Expr(">", 1, 2)
    assert _term(lit(1) >= lit(2)) == Expr(">=", 1, 2)


###
# Pushdowns Translations Tests
###


def test_column_pushdown_binding():
    schema = Schema._from_pydict(
        {
            "a": dt.bool(),  # 0
            "b": dt.bool(),  # 1
            "c": dt.bool(),  # 2
        }
    )
    pypushdowns = PyPushdowns(columns=["c", "b", "a"])  # !! reverse order on purpose !!
    pushdowns = ScanPushdowns._from_pypushdowns(pypushdowns, schema)

    assert pushdowns.projections
    assert pushdowns.projections[0] == Reference("c", 2)
    assert pushdowns.projections[1] == Reference("b", 1)
    assert pushdowns.projections[2] == Reference("a", 0)
    assert pushdowns.limit is None
    assert pushdowns.predicate is None


def test_simple_predicate_pushdown():
    schema = Schema._from_pydict(
        {
            "a": dt.bool(),  # 0
        }
    )
    predicate = col("a") == lit(1)  # (= a 1)
    pypushdowns = PyPushdowns(filters=predicate._expr)
    pushdowns = ScanPushdowns._from_pypushdowns(pypushdowns, schema)

    assert pushdowns.projections is None
    assert pushdowns.predicate == Expr("=", Reference("a", 0), 1)
    assert pushdowns.limit is None


def test_complex_predicate_pushdown():
    schema = Schema._from_pydict(
        {
            "a": dt.bool(),  # 0
            "b": dt.bool(),  # 1
            "c": dt.bool(),  # 2
        }
    )
    predicate = col("a") == (col("b") + col("c"))  # (= a (+ b c))
    pypushdowns = PyPushdowns(filters=predicate._expr)
    pushdowns = ScanPushdowns._from_pypushdowns(pypushdowns, schema)

    assert pushdowns.projections is None
    assert pushdowns.predicate == Expr("=", Reference("a", 0), Expr("+", Reference("b", 1), Reference("c", 2)))
    assert pushdowns.limit is None


def test_limit_pushdown():
    schema = Schema._from_pydict({"a": dt.bool()})
    pypushdowns = PyPushdowns(limit=1738)
    pushdowns = ScanPushdowns._from_pypushdowns(pypushdowns, schema)

    assert pushdowns.projections is None
    assert pushdowns.predicate is None
    assert pushdowns.limit == 1738


def test_simple_partition_pushdown():
    schema = Schema._from_pydict(
        {
            "a": dt.bool(),  # 0
        }
    )
    predicate = col("a") == lit(1)  # (= a 1)
    pypushdowns = PyPushdowns(partition_filters=predicate._expr)
    pushdowns = ScanPushdowns._from_pypushdowns(pypushdowns, schema)

    assert pushdowns.projections is None
    assert pushdowns.predicate == Expr("=", Reference("a", 0), 1)
    assert pushdowns.limit is None


def test_composite_partition_pushdown():
    schema = Schema._from_pydict(
        {
            "a": dt.bool(),  # 0
            "b": dt.bool(),  # 1
        }
    )

    # rust side has these split
    filters = col("a") == lit(1)  # (= a 1)
    partition_filters = col("b") > lit(2)  # (> b 2)
    pypushdowns = PyPushdowns(filters=filters._expr, partition_filters=partition_filters._expr)
    pushdowns = ScanPushdowns._from_pypushdowns(pypushdowns, schema)

    # translation should combine them.
    p1 = Expr("=", Reference("a", 0), 1)
    p2 = Expr(">", Reference("b", 1), 2)

    assert pushdowns.projections is None
    assert pushdowns.predicate == Expr("and", p1, p2)
    assert pushdowns.limit is None


###
# LispyVisitor Tests
###


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
