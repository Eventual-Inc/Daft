from __future__ import annotations

from typing import Any

import pytest

from daft.expressions import Expression
from daft.expressions.visitor import PredicateVisitor
from daft.logical.schema import DataType

pyiceberg = pytest.importorskip("pyiceberg")

from pyiceberg.expressions import (
    And,
    BooleanExpression,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEqualTo,
    NotNull,
    Or,
    Reference,
    UnboundTerm,
)
from pyiceberg.expressions.literals import Literal, literal

R = BooleanExpression | UnboundTerm | Literal


class IcebergTranslator(PredicateVisitor[R]):
    def visit_col(self, name: str) -> R:
        return Reference(name)

    def visit_lit(self, value: Any) -> R:
        return literal(value)

    def visit_alias(self, expr: Expression, alias: str) -> R:
        raise NotImplementedError

    def visit_cast(self, expr: Expression, dtype: DataType) -> R:
        raise ValueError("Cannot")

    def visit_and(self, left: Expression, right: Expression) -> R:
        l = self.visit(left)
        r = self.visit(right)
        return And(l, r)

    def visit_or(self, left: Expression, right: Expression) -> R:
        l = self.visit(left)
        r = self.visit(right)
        return Or(l, r)

    def visit_not(self, expr: Expression) -> R:
        c = self.visit_expr(expr)
        return Not(c)

    def visit_equal(self, left: Expression, right: Expression) -> R:
        l = self.visit(left)
        r = self.visit(right)
        return EqualTo(l, r)

    def visit_not_equal(self, left: Expression, right: Expression) -> R:
        l = self.visit(left)
        r = self.visit(right)
        return NotEqualTo(l, r)

    def visit_less_than(self, left: Expression, right: Expression) -> R:
        l = self.visit(left)
        r = self.visit(right)
        return LessThan(l, r)

    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> R:
        l = self.visit(left)
        r = self.visit(right)
        return LessThanOrEqual(l, r)

    def visit_greater_than(self, left: Expression, right: Expression) -> R:
        l = self.visit(left)
        r = self.visit(right)
        return GreaterThan(l, r)

    def visit_greater_than_or_equal(self, left: Expression, right: Expression) -> R:
        l = self.visit(left)
        r = self.visit(right)
        return GreaterThanOrEqual(l, r)

    def visit_between(self, expr: Expression, lower: Expression, upper: Expression) -> R:
        raise ValueError("pyiceberg does not have a between expression.")

    def visit_is_in(self, expr: Expression, items: list[Expression]) -> R:
        term = self.visit(expr)
        literals = [self.visit(item) for item in items]
        return In(term, literals)

    def visit_is_null(self, expr: Expression) -> R:
        term = self.visit(expr)
        return IsNull(term)

    def visit_not_null(self, expr: Expression) -> R:
        term = self.visit(expr)
        return NotNull(term)

    def visit_function(self, name: str, args: list[Expression]) -> R:
        raise ValueError


@pytest.mark.skip
def test_iceberg_predicates():
    """Test we can translate each one of the Iceberg predicates.

    References:
        - https://iceberg.apache.org/javadoc/1.0.0/index.html?org/apache/iceberg/expressions/Expressions.html
        - https://py.iceberg.apache.org/reference/pyiceberg/expressions/
    """
    assert _term(~col("a")) == Expr("not", Reference("a"))

    assert _term(lit(True) & lit(False)) == Expr("and", True, False)
    assert _term(lit(True) | lit(False)) == Expr("or", True, False)

    assert _term(lit(1) == lit(2)) == Expr("=", 1, 2)
    assert _term(lit(1) != lit(2)) == Expr("!=", 1, 2)
    assert _term(lit(1) < lit(2)) == Expr("<", 1, 2)
    assert _term(lit(1) <= lit(2)) == Expr("<=", 1, 2)
    assert _term(lit(1) > lit(2)) == Expr(">", 1, 2)
    assert _term(lit(1) >= lit(2)) == Expr(">=", 1, 2)

    assert _term(col("a").is_null()) == Expr("is_null", Reference("a"))
    assert _term(col("a").not_null()) == Expr("not_null", Reference("a"))

    assert _term(col("a").float.is_nan()) == Expr("is_nan", Reference("a"))
    assert _term(col("a").float.not_nan()) == Expr("not_nan", Reference("a"))

    items = [1, 2, 3]
    assert _term(col("a").is_in(items)) == Expr("is_in", Reference("a"), Expr("list", 1, 2, 3))
    assert _term(~(col("a").is_in(items))) == Expr("not", Expr("is_in", Reference("a"), Expr("list", 1, 2, 3)))

    assert _term(col("a").str.startswith("xyz")) == Expr("startswith", Reference("a"), "xyz")
    assert _term(~(col("a").str.startswith("xyz"))) == Expr("not", Expr("startswith", Reference("a"), "xyz"))


@pytest.mark.skip
def test_iceberg_transforms():
    """Test we can translate each one of the Iceberg partition expressions.

    References:
        - https://iceberg.apache.org/javadoc/1.0.0/index.html?org/apache/iceberg/transforms/Transforms.html
        - https://py.iceberg.apache.org/reference/pyiceberg/transforms/
    """
    assert _term(col("a").partitioning.years()) == Expr("years", Reference("a"))
    assert _term(col("a").partitioning.months()) == Expr("months", Reference("a"))
    assert _term(col("a").partitioning.days()) == Expr("days", Reference("a"))
    assert _term(col("a").partitioning.hours()) == Expr("hours", Reference("a"))

    # need args from functions
    assert _term(col("a").partitioning.iceberg_bucket(16)) == Expr("iceberg_bucket", Reference("a"), 16)
    assert _term(col("a").partitioning.iceberg_truncate(10)) == Expr("iceberg_truncate", Reference("a"), 10)
