from __future__ import annotations

from daft import Expression
from daft.expressions.expressions import ExpressionNamespace


class ExpressionStructNamespace(ExpressionNamespace):
    def get(self, name: str) -> Expression:
        """Retrieves one field from a struct column

        Args:
            name: the name of the field to retrieve

        Returns:
            Expression: the field expression
        """
        return Expression._from_pyexpr(self._expr.struct_get(name))
