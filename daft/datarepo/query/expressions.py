from icebridge.client import IceBridgeClient, IcebergExpression

import pyarrow.dataset as pads
import pyarrow.compute as pc

from typing import Any, Literal, Union, Dict, Callable, Optional, List, Tuple

# Columns are just aliases for strings
QueryColumn = str

# Filters are expressed in DNF form for now until we have a better user-facing API
DNFFilters = List[List[Tuple[QueryColumn, str, Any]]]

Comparator = Union[
    Literal[">"],
    Literal[">="],
    Literal["<"],
    Literal["<="],
    Literal["="],
]
COMPARATOR_MAP: Dict[Comparator, str] = {
    ">": "__gt__",
    ">=": "__ge__",
    "<": "__lt__",
    "<=": "__le__",
    "=": "__eq__",
}
ICEBRIDGE_COMPARATOR_MAP: Dict[Comparator, Callable[[IceBridgeClient, str, Any], IcebergExpression]] = {
    ">": IcebergExpression.gt,
    ">=": IcebergExpression.gte,
    "<": IcebergExpression.lt,
    "<=": IcebergExpression.lte,
    "=": IcebergExpression.equal,
}


def get_arrow_filter_expression(filters: Optional[DNFFilters]) -> Optional[pads.Expression]:
    if filters is None:
        return None

    final_expr = None

    for conjunction_filters in filters:
        conjunctive_expr = None
        for raw_filter in conjunction_filters:
            col, op, val = raw_filter
            expr = getattr(pc.field(f"root.{col}"), COMPARATOR_MAP[op])(val)
            if conjunctive_expr is None:
                conjunctive_expr = expr
            else:
                conjunctive_expr = conjunctive_expr & expr

        if final_expr is None:
            final_expr = conjunctive_expr
        else:
            final_expr = final_expr | conjunctive_expr

    return final_expr

def get_iceberg_filter_expression(filters: Optional[DNFFilters], client: IceBridgeClient) -> Optional[IcebergExpression]:
    if filters is None:
        return None

    final_expr = IcebergExpression.always_false(client)

    for conjunction_filters in filters:
        conjunctive_expr = IcebergExpression.always_true(client)
        for raw_filter in conjunction_filters:
            col, op, val = raw_filter
            expr = ICEBRIDGE_COMPARATOR_MAP[op](client, f"root.{col}", val)
            conjunctive_expr = conjunctive_expr.AND(expr)
        final_expr = final_expr.OR(conjunctive_expr)

    return final_expr
