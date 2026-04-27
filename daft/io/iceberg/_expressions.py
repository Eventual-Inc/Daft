from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyiceberg.expressions import AlwaysTrue
from pyiceberg.expressions import BooleanExpression as IcebergBooleanExpression

from daft.daft import PyExpr
from daft.expressions.expressions import Expression
from daft.io.iceberg._visitors import IcebergPredicateVisitor

if TYPE_CHECKING:
    from pyiceberg.schema import Schema as IcebergSchema

    from daft.daft import PyPushdowns

logger = logging.getLogger(__name__)


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


def convert_row_filter(pushdowns: PyPushdowns, schema: IcebergSchema) -> IcebergBooleanExpression:
    """Convert a row filter to an Iceberg expression for the row filter pushdown."""
    row_filter = convert_filter(pushdowns.filters, schema)
    par_filter = convert_filter(pushdowns.partition_filters, schema)
    if row_filter is not None and par_filter is not None:
        return row_filter & par_filter
    elif row_filter is not None:
        return row_filter
    elif par_filter is not None:
        return par_filter
    else:
        return AlwaysTrue()


def convert_filter(filter: PyExpr | None, schema: IcebergSchema) -> IcebergBooleanExpression | None:
    """Convert a Daft expression to an Iceberg expression for a row filter pushdown."""
    if filter is None:
        return None
    try:
        return convert_expression_to_iceberg(filter, schema)
    except Exception as e:
        logger.warning("Could not convert filter to Iceberg expression, skipping pushdown: %s", e)
        return None
