from __future__ import annotations

from typing import TYPE_CHECKING, Any

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
)
from pyiceberg.expressions.literals import Literal, literal

from daft.expressions.visitor import PredicateVisitor

if TYPE_CHECKING:
    from daft.datatype import DataType
    from daft.expressions import Expression


class IcebergPredicateVisitor(PredicateVisitor[BooleanExpression]):
    def visit_col(self, name: str) -> BooleanExpression:
        return Reference(name)  # type: ignore[return-value]

    def visit_lit(self, value: Any) -> BooleanExpression:
        return literal(value)  # type: ignore[return-value]

    def visit_alias(self, expr: Expression, alias: str) -> BooleanExpression:
        return self.visit(expr)

    def visit_cast(self, expr: Expression, dtype: DataType) -> BooleanExpression:
        return self.visit(expr)

    def visit_function(self, name: str, args: list[Expression]) -> BooleanExpression:
        raise ValueError(f"Iceberg does not support function '{name}' in filter expressions")

    def visit_coalesce(self, args: list[Expression]) -> BooleanExpression:
        raise ValueError("Iceberg does not support coalesce in filter expressions")

    # --- logical combinators ---

    def visit_and(self, left: Expression, right: Expression) -> BooleanExpression:
        return And(self.visit(left), self.visit(right))

    def visit_or(self, left: Expression, right: Expression) -> BooleanExpression:
        return Or(self.visit(left), self.visit(right))

    def visit_not(self, expr: Expression) -> BooleanExpression:
        return Not(self.visit(expr))

    # --- comparison predicates ---

    def visit_equal(self, left: Expression, right: Expression) -> BooleanExpression:
        ref, lit, _ = self._extract_ref_and_lit(left, right)
        return EqualTo(ref, lit)

    def visit_not_equal(self, left: Expression, right: Expression) -> BooleanExpression:
        ref, lit, _ = self._extract_ref_and_lit(left, right)
        return NotEqualTo(ref, lit)

    def visit_less_than(self, left: Expression, right: Expression) -> BooleanExpression:
        ref, lit, swapped = self._extract_ref_and_lit(left, right)
        return GreaterThan(ref, lit) if swapped else LessThan(ref, lit)

    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> BooleanExpression:
        ref, lit, swapped = self._extract_ref_and_lit(left, right)
        return GreaterThanOrEqual(ref, lit) if swapped else LessThanOrEqual(ref, lit)

    def visit_greater_than(self, left: Expression, right: Expression) -> BooleanExpression:
        ref, lit, swapped = self._extract_ref_and_lit(left, right)
        return LessThan(ref, lit) if swapped else GreaterThan(ref, lit)

    def visit_greater_than_or_equal(self, left: Expression, right: Expression) -> BooleanExpression:
        ref, lit, swapped = self._extract_ref_and_lit(left, right)
        return LessThanOrEqual(ref, lit) if swapped else GreaterThanOrEqual(ref, lit)

    # --- range and set predicates ---

    def visit_between(self, expr: Expression, lower: Expression, upper: Expression) -> BooleanExpression:
        ref = self._visit_as_reference(expr)
        lo = self._visit_as_literal(lower)
        hi = self._visit_as_literal(upper)
        return And(GreaterThanOrEqual(ref, lo), LessThanOrEqual(ref, hi))

    def visit_is_in(self, expr: Expression, items: list[Expression]) -> BooleanExpression:
        ref = self._visit_as_reference(expr)
        lits = [self._visit_as_literal(item).value for item in items]
        return In(ref, lits)

    # --- null predicates ---

    def visit_is_null(self, expr: Expression) -> BooleanExpression:
        return IsNull(self._visit_as_reference(expr))

    def visit_not_null(self, expr: Expression) -> BooleanExpression:
        return NotNull(self._visit_as_reference(expr))

    # --- helpers ---

    def _extract_ref_and_lit(
        self,
        left: Expression,
        right: Expression,
    ) -> tuple[Reference, Literal, bool]:
        l, r = self.visit(left), self.visit(right)
        if isinstance(l, Reference) and isinstance(r, Literal):
            return l, r, False
        if isinstance(r, Reference) and isinstance(l, Literal):
            return r, l, True
        raise ValueError(
            f"Expected one column reference and one literal, got {type(l).__name__} and {type(r).__name__}"
        )

    def _visit_as_reference(self, expr: Expression) -> Reference:
        result = self.visit(expr)
        if not isinstance(result, Reference):
            raise ValueError(f"Expected a column reference, got {type(result).__name__}")
        return result

    def _visit_as_literal(self, expr: Expression) -> Literal:
        result = self.visit(expr)
        if not isinstance(result, Literal):
            raise ValueError(f"Expected a literal value, got {type(result).__name__}")
        return result
