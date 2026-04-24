from __future__ import annotations

from pyiceberg.expressions import BooleanExpression as IcebergBooleanExpression

from daft.daft import PyExpr
from daft.expressions.expressions import Expression
from daft.io.iceberg.visitors import IcebergPredicateVisitor


def convert_expression_to_iceberg(expression: Expression | PyExpr) -> IcebergBooleanExpression:
    """Convert a Daft expression to an unbound Iceberg BooleanExpression."""
    if isinstance(expression, PyExpr):
        expression = Expression._from_pyexpr(expression)
    visitor = IcebergPredicateVisitor()
    result = visitor.visit(expression)
    assert isinstance(result, IcebergBooleanExpression), f"Expected BooleanExpression, got {type(result)}"
    return result
