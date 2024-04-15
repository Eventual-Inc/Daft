from __future__ import annotations

from daft import Expression, lit
from daft.daft import CountMode
from daft.expressions.expressions import ExpressionNamespace


class ExpressionListNamespace(ExpressionNamespace):
    def join(self, delimiter: str | Expression) -> Expression:
        """Joins every element of a list using the specified string delimiter

        Args:
            delimiter (str | Expression): the delimiter to use to join lists with

        Returns:
            Expression: a String expression which is every element of the list joined on the delimiter
        """
        delimiter_expr = Expression._to_expression(delimiter)
        return Expression._from_pyexpr(self._expr.list_join(delimiter_expr._expr))

    def count(self, mode: CountMode = CountMode.Valid) -> Expression:
        """Counts the number of elements in each list

        Args:
            mode: The mode to use for counting. Defaults to CountMode.Valid

        Returns:
            Expression: a UInt64 expression which is the length of each list
        """
        return Expression._from_pyexpr(self._expr.list_count(mode))

    def lengths(self) -> Expression:
        """Gets the length of each list

        Returns:
            Expression: a UInt64 expression which is the length of each list
        """
        return Expression._from_pyexpr(self._expr.list_count(CountMode.All))

    def get(self, idx: int | Expression, default: object = None) -> Expression:
        """Gets the element at an index in each list

        Args:
            idx: index or indices to retrieve from each list
            default: the default value if the specified index is out of bounds

        Returns:
            Expression: an expression with the type of the list values
        """
        idx_expr = Expression._to_expression(idx)
        default_expr = lit(default)
        return Expression._from_pyexpr(self._expr.list_get(idx_expr._expr, default_expr._expr))

    def sum(self) -> Expression:
        """Sums each list. Empty lists and lists with all nulls yield null.

        Returns:
            Expression: an expression with the type of the list values
        """
        return Expression._from_pyexpr(self._expr.list_sum())

    def mean(self) -> Expression:
        """Calculates the mean of each list. If no non-null values in a list, the result is null.

        Returns:
            Expression: a Float64 expression with the type of the list values
        """
        return Expression._from_pyexpr(self._expr.list_mean())

    def min(self) -> Expression:
        """Calculates the minimum of each list. If no non-null values in a list, the result is null.

        Returns:
            Expression: a Float64 expression with the type of the list values
        """
        return Expression._from_pyexpr(self._expr.list_min())

    def max(self) -> Expression:
        """Calculates the maximum of each list. If no non-null values in a list, the result is null.

        Returns:
            Expression: a Float64 expression with the type of the list values
        """
        return Expression._from_pyexpr(self._expr.list_max())
