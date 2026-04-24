from __future__ import annotations

from typing import TYPE_CHECKING

from pyiceberg.expressions import BooleanExpression as IcebergBooleanExpression

if TYPE_CHECKING:
    from daft.expressions import Expression


def convert_expression_to_iceberg(expression: Expression) -> IcebergBooleanExpression:
    """Convert a Daft expression to an Iceberg expression."""
    raise NotImplementedError("convert_expression_to_iceberg is not implemented")

