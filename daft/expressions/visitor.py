from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

R = TypeVar("R")


if TYPE_CHECKING:
    from daft.expressions import Expression
    from daft.logical.schema import DataType


class ExpressionVisitor(ABC, Generic[R]):
    """ExpressionVisitor is an abstract base class for implementing the visitor pattern on expressions.

    Examples:
        >>> from daft.expressions import ExpressionVisitor, col, lit
        >>>
        >>> class PrintVisitor(ExpressionVisitor[None]):
        ...     def visit_col(self, name: str) -> None:
        ...         print(f"Column: {name}")
        ...
        ...     def visit_lit(self, value: Any) -> None:
        ...         print(f"Literal: {value}")
        ...
        ...     def visit_alias(self, expr: Expression, alias: str) -> None:
        ...         print(f"Alias: {alias}")
        ...         self.visit(expr)
        ...
        ...     def visit_cast(self, expr: Expression, dtype: DataType) -> None:
        ...         print(f"Cast: {dtype}")
        ...         self.visit(expr)
        ...
        ...     def visit_function(self, name: str, args: list[Expression]) -> None:
        ...         print(f"Function: {name}")
        ...         for arg in args:
        ...             self.visit(arg)
        >>> # Create an expression
        >>> expr = col("x").cast("int64").alias("y")
        >>>
        >>> # Visit the expression
        >>> visitor = PrintVisitor()
        >>> visitor.visit(expr)
        Alias: y
        Cast: Int64
        Column: x
    """

    def visit(self, expr: Expression) -> R:
        """Visit an arbitrary expression, invoking this visitor."""
        return expr._expr.accept(self)

    def visit_(self, name: str, args: list[Expression]) -> R:
        """Visit an arbitrary expression function, dispatching to an override if one exists."""
        if override := getattr(self, f"visit_{name}", None):
            return override(*args)
        else:
            return self.visit_function(name, args)

    @abstractmethod
    def visit_col(self, name: str) -> R:
        """Visit a col expression."""
        ...

    @abstractmethod
    def visit_lit(self, value: Any) -> R:
        """Visit a lit expression."""
        ...

    @abstractmethod
    def visit_alias(self, expr: Expression, alias: str) -> R:
        """Visit an alias expression."""
        ...

    @abstractmethod
    def visit_cast(self, expr: Expression, dtype: DataType) -> R:
        """Visit a cast expression."""
        ...

    @abstractmethod
    def visit_function(self, name: str, args: list[Expression]) -> R:
        """Visit a function call expression."""
        ...


class PredicateVisitor(ExpressionVisitor[R]):
    """PredicateVisitor is an ExpressionVisitor with helper methods for predicates."""

    @abstractmethod
    def visit_and(self, left: Expression, right: Expression) -> R:
        """Visit an and expression."""
        ...

    @abstractmethod
    def visit_or(self, left: Expression, right: Expression) -> R:
        """Visit an or expression."""
        ...

    @abstractmethod
    def visit_not(self, expr: Expression) -> R:
        """Visit a not expression."""
        ...

    @abstractmethod
    def visit_equal(self, left: Expression, right: Expression) -> R:
        """Visit an equals comparison predicate."""
        ...

    @abstractmethod
    def visit_not_equal(self, left: Expression, right: Expression) -> R:
        """Visit a not equals comparison predicate."""
        ...

    @abstractmethod
    def visit_less_than(self, left: Expression, right: Expression) -> R:
        """Visit a less than comparison predicate."""
        ...

    @abstractmethod
    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> R:
        """Visit a less than or equal comparison predicate."""
        ...

    @abstractmethod
    def visit_greater_than(self, left: Expression, right: Expression) -> R:
        """Visit a greater than comparison predicate."""
        ...

    @abstractmethod
    def visit_greater_than_or_equal(self, left: Expression, right: Expression) -> R:
        """Visit a greater than or equal comparison predicate."""
        ...

    @abstractmethod
    def visit_between(self, expr: Expression, lower: Expression, upper: Expression) -> R:
        """Visit a between predicate."""
        ...

    @abstractmethod
    def visit_is_in(self, expr: Expression, items: list[Expression]) -> R:
        """Visit an is_in predicate."""
        ...

    @abstractmethod
    def visit_is_null(self, expr: Expression) -> R:
        """Visit an is_null predicate."""
        ...

    @abstractmethod
    def visit_not_null(self, expr: Expression) -> R:
        """Visit an not_null predicate."""
        ...


class _ColumnVisitor(ExpressionVisitor[set[str]]):
    """Visitor which returns a set of columns in the expression."""

    def visit_col(self, name: str) -> set[str]:
        return {name}

    def visit_lit(self, value: Any) -> set[str]:
        return set()

    def visit_alias(self, expr: Expression, alias: str) -> set[str]:
        return self.visit(expr)

    def visit_cast(self, expr: Expression, dtype: DataType) -> set[str]:
        return self.visit(expr)

    def visit_function(self, name: str, args: list[Expression]) -> set[str]:
        return set().union(*(self.visit(arg) for arg in args))
