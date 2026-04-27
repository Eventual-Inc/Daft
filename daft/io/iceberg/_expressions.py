from __future__ import annotations

from typing import TYPE_CHECKING

from pyiceberg.expressions import BooleanExpression as IcebergBooleanExpression

from daft.daft import PyExpr
from daft.expressions.expressions import Expression
from daft.io.iceberg.visitors import IcebergPredicateVisitor

if TYPE_CHECKING:
    from pyiceberg.schema import Schema as IcebergSchema


def convert_expression_to_iceberg(
    expression: Expression | PyExpr,
    schema: IcebergSchema | None = None,
) -> IcebergBooleanExpression:
    """Convert a Daft expression to an unbound Iceberg BooleanExpression."""
    if isinstance(expression, PyExpr):
        expression = Expression._from_pyexpr(expression)
    visitor = IcebergPredicateVisitor(schema)
    result = visitor.visit(expression)
    assert isinstance(result, IcebergBooleanExpression), f"Expected BooleanExpression, got {type(result)}"
    return result
