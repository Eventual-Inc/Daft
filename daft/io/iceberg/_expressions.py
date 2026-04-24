from __future__ import annotations

from typing import TYPE_CHECKING

from pyiceberg.expressions import BooleanExpression as IcebergBooleanExpression

from daft.io.iceberg.visitors import IcebergPredicateVisitor

if TYPE_CHECKING:
    from pyiceberg.schema import Schema as IcebergSchema

    from daft.expressions import Expression


def convert_expression_to_iceberg(expression: Expression, schema: IcebergSchema) -> IcebergBooleanExpression:
    """Convert a Daft expression to a bound Iceberg BooleanExpression."""
    visitor = IcebergPredicateVisitor(schema)
    result = visitor.visit(expression)
    assert isinstance(result, IcebergBooleanExpression), f"Expected BooleanExpression, got {type(result)}"
    return result
