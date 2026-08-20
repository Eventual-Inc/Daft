from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyiceberg.expressions import AlwaysTrue
from pyiceberg.expressions import BooleanExpression as IcebergBooleanExpression

from daft.daft import PyExpr
from daft.expressions.expressions import Expression
from daft.expressions.visitor import _ColumnVisitor
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


def convert_row_filter(
    pushdowns: PyPushdowns,
    schema: IcebergSchema,
) -> IcebergBooleanExpression:
    """Convert Daft pushdowns into a pyiceberg ``row_filter``.

    We have to check both 'filters' and 'partition_filters' for identity
    transforms because Daft will move them from 'filters' to 'partition_filters'
    yet we need to forward them to PyIceberg for metadata-level and file-level pruning.

    Daft has its own optimization logic for predicates, but fortunately we can
    just leverage PyIceberg's pushdowns which accepts the single predicate filter
    and computes partition predicates and residuals for us. However, because Daft's
    optimizer will move some predicates from 'filters' to 'partition_filters', we need to
    check both fields and forward them to PyIceberg.

    We can only check the 'partition_filters' for identity transforms because Daft
    only removes predicates from 'filters' for identity transforms, and we need to
    add these back for PyIceberg to use.

    Identity transforms (e.g., ts partitioned by identity(ts)):
        pushdowns.filters             : <None>          ← Daft DROPPED the predicate
        pushdowns.partition_filters   : (ts == 2024-01-01)

    Non-identity transforms (e.g., number partitioned by bucket(16, number)):
        pushdowns.filters             : (number == 4)         ← Daft KEPT the predicate
        pushdowns.partition_filters   : (number_bucket_16 == bucket(4, 16))

    Related Issues:
        - https://github.com/Eventual-Inc/Daft/issues/6854
    """
    row_filter = convert_filter(pushdowns.filters, schema)
    par_filter = convert_partition_filter(pushdowns.partition_filters, schema)
    if row_filter is not None and par_filter is not None:
        return row_filter & par_filter
    elif row_filter is not None:
        return row_filter
    elif par_filter is not None:
        return par_filter
    else:
        return AlwaysTrue()


def convert_partition_filter(
    partition_filters: PyExpr | None,
    schema: IcebergSchema,
) -> IcebergBooleanExpression | None:
    """Convert only identity-transformed partition predicates to Iceberg expressions.

    We will do a "best-effort" conversion and forward the partition predicates to PyIceberg.
    If the conversion fails, we return None and PyIceberg will not use this filter. This is
    fine because we will still prune tasks internally. This could technically be more optimized
    by only extractig the identity transforms and adding the row_filter, but the happy case
    here will do that anyways, it just means we won't have support for composite partition
    predicate pruning at the metadata-level which is a reasonable trade-off for now, considering
    no metadata-level pruning had been supported in Daft yet.

    Conversion alone is not sufficient: predicates like ``is_null(number_bucket_16)`` convert
    cleanly (no transform function call to trip the visitor) but reference a derived
    partition-field name absent from the data schema, so pyiceberg's ``bind`` would fail
    later. We validate every column reference resolves in the data schema before forwarding.
    """
    if partition_filters is None:
        return None
    try:
        result = convert_filter(partition_filters, schema)
    except Exception as e:
        logger.warning("Could not convert partition filter to Iceberg expression, skipping pushdown: %s", e)
        return None
    if result is None:
        return None
    refs = _ColumnVisitor().visit(Expression._from_pyexpr(partition_filters))
    if not refs.issubset(set(schema.column_names)):
        return None
    return result


def convert_filter(filter: PyExpr | None, schema: IcebergSchema) -> IcebergBooleanExpression | None:
    """Convert a Daft expression to an Iceberg expression for a row filter pushdown."""
    if filter is None:
        return None
    try:
        return convert_expression_to_iceberg(filter, schema)
    except Exception as e:
        logger.warning("Could not convert filter to Iceberg expression, skipping pushdown: %s", e)
        return None


def convert_overwrite_filter(
    overwrite_filter: Expression | PyExpr | str | IcebergBooleanExpression,
    schema: IcebergSchema,
) -> IcebergBooleanExpression:
    """Normalize a user-provided overwrite filter into an unbound Iceberg BooleanExpression.

    Unlike the read-side helpers, an unconvertible predicate raises instead of falling back
    to a best-effort result: the filter decides which rows an overwrite deletes, so widening
    it to ``AlwaysTrue`` or dropping it would destroy data.

    The expression is also bound against ``schema`` so that unknown column references are
    reported before any data files are written.
    """
    from pyiceberg.expressions.parser import parse as parse_iceberg_predicate
    from pyiceberg.expressions.visitors import bind

    if isinstance(overwrite_filter, IcebergBooleanExpression):
        expr = overwrite_filter
    elif isinstance(overwrite_filter, str):
        expr = parse_iceberg_predicate(overwrite_filter)
    elif isinstance(overwrite_filter, (Expression, PyExpr)):
        expr = convert_expression_to_iceberg(overwrite_filter, schema)
    else:
        raise TypeError(
            "overwrite_filter must be a Daft Expression, an Iceberg predicate string, or a "
            f"pyiceberg BooleanExpression, got {type(overwrite_filter).__name__}"
        )

    # Raises if the predicate references fields that are not in the table schema.
    bind(schema, expr, case_sensitive=True)
    return expr
