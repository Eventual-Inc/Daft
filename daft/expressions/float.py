from __future__ import annotations

from daft import Expression
from daft.expressions.expressions import ExpressionNamespace


class ExpressionFloatNamespace(ExpressionNamespace):
    def is_nan(self) -> Expression:
        """Checks if values are NaN (a special float value indicating not-a-number)

        .. NOTE::
            Nulls will be propagated! I.e. this operation will return a null for null values.

        Example:
            >>> # [1., None, NaN] -> [False, None, True]
            >>> col("x").is_nan()

        Returns:
            Expression: Boolean Expression indicating whether values are invalid.
        """
        return Expression._from_pyexpr(self._expr.is_nan())
