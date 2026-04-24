from __future__ import annotations

from typing import TYPE_CHECKING

from pyiceberg.expressions import BooleanExpression

from daft.expressions import Expression
from daft.expressions.visitor import PredicateVisitor


if TYPE_CHECKING:
    from daft.expressions import Expression


class IcebergPredicateVisitor(PredicateVisitor[IcebergExpression]):
    """Visitor for converting Daft expressions to Iceberg expressions."""

    def visit_and(self, left: Expression, right: Expression) -> R:
        """Visit an and expression."""
        raise NotImplementedError("visit_and is not implemented")

    def visit_or(self, left: Expression, right: Expression) -> R:
        """Visit an or expression."""
        raise NotImplementedError("visit_or is not implemented")

    def visit_not(self, expr: Expression) -> R:
        """Visit a not expression."""
        raise NotImplementedError("visit_not is not implemented")

    def visit_equal(self, left: Expression, right: Expression) -> R:
        """Visit an equals comparison predicate."""
        raise NotImplementedError("visit_equal is not implemented")

    def visit_not_equal(self, left: Expression, right: Expression) -> R:
        """Visit a not equals comparison predicate."""
        raise NotImplementedError("visit_not_equal is not implemented")

    def visit_less_than(self, left: Expression, right: Expression) -> R:
        """Visit a less than comparison predicate."""
        raise NotImplementedError("visit_less_than is not implemented")

    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> R:
        """Visit a less than or equal comparison predicate."""
        raise NotImplementedError("visit_less_than_or_equal is not implemented")

    def visit_greater_than(self, left: Expression, right: Expression) -> R:
        """Visit a greater than comparison predicate."""
        raise NotImplementedError("visit_greater_than is not implemented")

    def visit_greater_than_or_equal(self, left: Expression, right: Expression) -> R:
        """Visit a greater than or equal comparison predicate."""
        raise NotImplementedError("visit_greater_than_or_equal is not implemented")

    def visit_between(
        self, expr: Expression, lower: Expression, upper: Expression
    ) -> R:
        """Visit a between predicate."""
        raise NotImplementedError("visit_between is not implemented")

    def visit_is_in(self, expr: Expression, items: list[Expression]) -> R:
        """Visit an is_in predicate."""
        raise NotImplementedError("visit_is_in is not implemented")

    def visit_is_null(self, expr: Expression) -> R:
        """Visit an is_null predicate."""
        raise NotImplementedError("visit_is_null is not implemented")

    def visit_not_null(self, expr: Expression) -> R:
        """Visit an not_null predicate."""
        raise NotImplementedError("visit_not_null is not implemented")
