from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, List, TypeVar

from daft.expressions import Expression
from daft.logical.schema import DataType

R = TypeVar("R")

class ExpressionVisitor(ABC, Generic[R]):
    """BaseVisitor is used to process a Daft Expression object."""

    def visit(self, expr: Expression) -> R:
        return expr._expr.accept(self)

    @abstractmethod
    def visit_col(self, name: str) -> R:
        """Visit a col expression."""
        pass

    @abstractmethod
    def visit_lit(self, value: Any) -> R:
        """Visit a lit expression."""
        pass

    @abstractmethod
    def visit_alias(self, expr: Expression, alias: str) -> R:
        """Visit an alias expression."""
        pass

    @abstractmethod
    def visit_cast(self, expr: Expression, dtype: DataType) -> R:
        """Visit a cast expression."""
        pass

    ###
    # logical
    ###

    @abstractmethod
    def visit_and(self, left: Expression, right: Expression) -> R:
        """Visit an and expression."""
        pass

    @abstractmethod
    def visit_or(self, left: Expression, right: Expression) -> R:
        """Visit an or expression."""
        pass

    @abstractmethod
    def visit_not(self, expr: Expression) -> R:
        """Visit a not expression."""
        pass

    ###
    # predicates
    ###
    
    @abstractmethod
    def visit_equal(self, left: Expression, right: Expression) -> R:
        """Visit an equals comparison predicate."""
        pass

    @abstractmethod
    def visit_not_equal(self, left: Expression, right: Expression) -> R:
        """Visit a not equals comparison predicate."""
        pass

    @abstractmethod
    def visit_less_than(self, left: Expression, right: Expression) -> R:
        """Visit a less than comparison predicate."""
        pass

    @abstractmethod
    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> R:
        """Visit a less than or equals comparison predicate."""
        pass

    @abstractmethod
    def visit_greater_than(self, left: Expression, right: Expression) -> R:
        """Visit a greater than comparison predicate."""
        pass

    @abstractmethod
    def visit_greater_than_or_equals(self, left: Expression, right: Expression) -> R:
        """Visit a greater than or equals comparison predicate."""
        pass

    @abstractmethod
    def visit_between(self, expr: Expression, lower: Expression, upper: Expression) -> R:
        """Visit a between predicate."""
        pass

    @abstractmethod
    def visit_is_in(self, expr: Expression, values: list[Expression]) -> R:
        """Visit an is_in predicate."""
        pass

    @abstractmethod
    def visit_is_null(self, expr: Expression) -> R:
        """Visit an is_null predicate."""
        pass

    @abstractmethod
    def visit_not_null(self, expr: Expression) -> R:
        """Visit an not_null predicate."""
        pass

    @abstractmethod
    def visit_is_nan(self, expr: Expression) -> R:
        """Visit an is_nan predicate."""
        pass

    @abstractmethod
    def visit_not_nan(self, expr: Expression) -> R:
        """Visit an not_nan predicate."""
        pass

    ###
    # function covers any Expression.<function> not listed above
    ###

    @abstractmethod
    def visit_function(self, name: str, args: List[Expression]) -> R:
        """Visit a function expression."""
        pass
