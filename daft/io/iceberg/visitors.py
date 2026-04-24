from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pyiceberg.expressions import (
    And,
    Bound,
    BoundEqualTo,
    BoundGreaterThan,
    BoundGreaterThanOrEqual,
    BoundIn,
    BoundIsNull,
    BoundLessThan,
    BoundLessThanOrEqual,
    BoundNotEqualTo,
    BoundNotIn,
    BoundNotNull,
    BoundReference,
    Not,
    Or,
    Reference,
)
from pyiceberg.expressions.literals import Literal, literal

from daft.expressions.visitor import PredicateVisitor

if TYPE_CHECKING:
    from pyiceberg.schema import Schema as IcebergSchema

    from daft.datatype import DataType
    from daft.expressions import Expression


class IcebergPredicateVisitor(PredicateVisitor[Bound]):

    def __init__(self, schema: IcebergSchema) -> None:
        self._schema = schema

    def visit_col(self, name: str) -> Bound:
        return Reference(name).bind(self._schema)

    def visit_lit(self, value: Any) -> Bound:
        return literal(value)  # type: ignore[return-value]

    def visit_alias(self, expr: Expression, alias: str) -> Bound:
        return self.visit(expr)

    def visit_cast(self, expr: Expression, dtype: DataType) -> Bound:
        return self.visit(expr)

    def visit_function(self, name: str, args: list[Expression]) -> Bound:
        raise ValueError(f"Iceberg does not support function '{name}' in filter expressions")

    def visit_coalesce(self, args: list[Expression]) -> Bound:
        raise ValueError("Iceberg does not support coalesce in filter expressions")

    # --- logical combinators ---

    def visit_and(self, left: Expression, right: Expression) -> Bound:
        return And(self.visit(left), self.visit(right))  # type: ignore[return-value]

    def visit_or(self, left: Expression, right: Expression) -> Bound:
        return Or(self.visit(left), self.visit(right))  # type: ignore[return-value]

    def visit_not(self, expr: Expression) -> Bound:
        return Not(self.visit(expr))  # type: ignore[return-value]

    # --- comparison predicates ---

    def visit_equal(self, left: Expression, right: Expression) -> Bound:
        ref, lit, _ = self._extract_ref_and_lit(left, right)
        return BoundEqualTo(ref, lit)

    def visit_not_equal(self, left: Expression, right: Expression) -> Bound:
        ref, lit, _ = self._extract_ref_and_lit(left, right)
        return BoundNotEqualTo(ref, lit)

    def visit_less_than(self, left: Expression, right: Expression) -> Bound:
        ref, lit, swapped = self._extract_ref_and_lit(left, right)
        return BoundGreaterThan(ref, lit) if swapped else BoundLessThan(ref, lit)

    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> Bound:
        ref, lit, swapped = self._extract_ref_and_lit(left, right)
        return BoundGreaterThanOrEqual(ref, lit) if swapped else BoundLessThanOrEqual(ref, lit)

    def visit_greater_than(self, left: Expression, right: Expression) -> Bound:
        ref, lit, swapped = self._extract_ref_and_lit(left, right)
        return BoundLessThan(ref, lit) if swapped else BoundGreaterThan(ref, lit)

    def visit_greater_than_or_equal(self, left: Expression, right: Expression) -> Bound:
        ref, lit, swapped = self._extract_ref_and_lit(left, right)
        return BoundLessThanOrEqual(ref, lit) if swapped else BoundGreaterThanOrEqual(ref, lit)

    # --- range and set predicates ---

    def visit_between(self, expr: Expression, lower: Expression, upper: Expression) -> Bound:
        ref = self.visit(expr)
        assert isinstance(ref, BoundReference)
        field_type = ref.ref().field.field_type
        lo = self._visit_as_literal(lower).to(field_type)
        hi = self._visit_as_literal(upper).to(field_type)
        return And(BoundGreaterThanOrEqual(ref, lo), BoundLessThanOrEqual(ref, hi))  # type: ignore[return-value]

    def visit_is_in(self, expr: Expression, items: list[Expression]) -> Bound:
        ref = self.visit(expr)
        assert isinstance(ref, BoundReference)
        field_type = ref.ref().field.field_type
        lits = {self._visit_as_literal(item).to(field_type) for item in items}
        return BoundIn(ref, lits)

    # --- null predicates ---

    def visit_is_null(self, expr: Expression) -> Bound:
        return BoundIsNull(self.visit(expr))

    def visit_not_null(self, expr: Expression) -> Bound:
        return BoundNotNull(self.visit(expr))

    # --- helpers ---

    def _extract_ref_and_lit(
        self,
        left: Expression,
        right: Expression,
    ) -> tuple[BoundReference, Literal, bool]:
        l, r = self.visit(left), self.visit(right)
        if isinstance(l, BoundReference) and isinstance(r, Literal):
            return l, r.to(l.ref().field.field_type), False
        if isinstance(r, BoundReference) and isinstance(l, Literal):
            return r, l.to(r.ref().field.field_type), True
        raise ValueError(f"Expected one column reference and one literal, got {type(l).__name__} and {type(r).__name__}")

    def _visit_as_literal(self, expr: Expression) -> Literal:
        result = self.visit(expr)
        if not isinstance(result, Literal):
            raise ValueError(f"Expected a literal value, got {type(result).__name__}")
        return result
