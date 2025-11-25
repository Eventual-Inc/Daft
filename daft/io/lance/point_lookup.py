"""Utilities for detecting point-look-up style filter predicates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from daft.expressions import Expression
    from daft.logical.schema import DataType
from daft.expressions.visitor import PredicateVisitor


@dataclass(frozen=True)
class _PointPredicate:
    column: str


class _PointLookupVisitor(PredicateVisitor[list[_PointPredicate] | None]):
    """Visitor that returns a list of point predicates or ``None`` if unsupported.

    Methods are typed against Expression only for type-checking; at runtime we rely on
    Expression's duck-typed helpers (is_column, is_literal, column_name).
    """

    def visit_and(self, left: Expression, right: Expression) -> list[_PointPredicate] | None:
        left_res = self.visit(left)
        if left_res is None:
            return None
        right_res = self.visit(right)
        if right_res is None:
            return None
        return left_res + right_res

    def visit_or(self, left: Expression, right: Expression) -> list[_PointPredicate] | None:
        return None

    def visit_not(self, expr: Expression) -> list[_PointPredicate] | None:
        return None

    def visit_equal(self, left: Expression, right: Expression) -> list[_PointPredicate] | None:
        column = self._extract_column(left, right)
        if column is None:
            return None
        return [_PointPredicate(column)]

    def visit_not_equal(self, left: Expression, right: Expression) -> list[_PointPredicate] | None:
        return None

    def visit_less_than(self, left: Expression, right: Expression) -> list[_PointPredicate] | None:
        return None

    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> list[_PointPredicate] | None:
        return None

    def visit_greater_than(self, left: Expression, right: Expression) -> list[_PointPredicate] | None:
        return None

    def visit_greater_than_or_equal(self, left: Expression, right: Expression) -> list[_PointPredicate] | None:
        return None

    def visit_between(self, expr: Expression, lower: Expression, upper: Expression) -> list[_PointPredicate] | None:
        return None

    def visit_is_in(self, expr: Expression, items: Sequence[Expression]) -> list[_PointPredicate] | None:
        if not expr.is_column():
            return None
        # Must be a non-empty list of literal values
        if not items or not all(item.is_literal() for item in items):
            return None
        column = expr.column_name()
        if column is None:
            return None
        return [_PointPredicate(column)]

    def visit_is_null(self, expr: Expression) -> list[_PointPredicate] | None:
        # Treat IS NULL as a point-lookup on the column; supported by BTREE.
        if not expr.is_column():
            return None
        column = expr.column_name()
        if column is None:
            return None
        return [_PointPredicate(column)]

    def visit_not_null(self, expr: Expression) -> list[_PointPredicate] | None:
        return None

    def visit_alias(self, expr: Expression, alias: str) -> list[_PointPredicate] | None:
        return self.visit(expr)

    def visit_cast(self, expr: Expression, dtype: DataType) -> list[_PointPredicate] | None:
        # Cast changes semantics/type; conservative: do not treat as point-lookup
        return None

    def visit_col(self, name: str) -> list[_PointPredicate] | None:
        return []

    def visit_function(self, func: str, args: list[Expression]) -> list[_PointPredicate] | None:
        return None

    def visit_lit(self, value: Expression) -> list[_PointPredicate] | None:
        return []

    def visit_default(self, expr: Expression) -> list[_PointPredicate] | None:
        return None

    def _extract_column(self, left: Expression, right: Expression) -> str | None:
        if left.is_column() and right.is_literal():
            return left.column_name()
        if right.is_column() and left.is_literal():
            return right.column_name()
        return None


def detect_point_lookup_columns(filters: Iterable[Expression]) -> list[str]:
    """Return the list of columns participating in point lookups.

    If any filter is not a supported point predicate, an empty list is returned.
    """
    visitor = _PointLookupVisitor()
    columns: list[str] = []
    for flt in filters:
        res = visitor.visit(flt)
        if res is None:
            return []
        columns.extend(predicate.column for predicate in res)
    return columns
