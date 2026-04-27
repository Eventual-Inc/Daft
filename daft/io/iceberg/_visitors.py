from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from pyiceberg.expressions import (
    And,
    BooleanExpression,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEqualTo,
    NotNull,
    Or,
    Reference,
)
from pyiceberg.expressions.literals import DateLiteral, Literal, StringLiteral, literal
from pyiceberg.types import TimestampType, TimestamptzType
from pyiceberg.utils.datetime import days_to_date

from daft.expressions.visitor import PredicateVisitor

if TYPE_CHECKING:
    from pyiceberg.schema import Schema as IcebergSchema

    from daft.datatype import DataType
    from daft.expressions import Expression

try:
    from pyiceberg.types import TimestampNanoType, TimestamptzNanoType

    _TIMESTAMP_TYPES = (
        TimestampType,
        TimestamptzType,
        TimestampNanoType,
        TimestamptzNanoType,
    )
    _TIMESTAMPTZ_TYPES = (TimestamptzType, TimestamptzNanoType)
except ImportError:
    _TIMESTAMP_TYPES = (TimestampType, TimestamptzType)  # type: ignore[assignment]
    _TIMESTAMPTZ_TYPES = (TimestamptzType,)  # type: ignore[assignment]

_DATE_ONLY_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


class IcebergPredicateVisitor(PredicateVisitor[BooleanExpression]):
    def __init__(self, schema: IcebergSchema | None = None) -> None:
        self._schema = schema

    def visit_col(self, name: str) -> BooleanExpression:
        return Reference(name)

    def visit_lit(self, value: Any) -> BooleanExpression:
        return literal(value)

    def visit_alias(self, expr: Expression, alias: str) -> BooleanExpression:
        return self.visit(expr)

    def visit_cast(self, expr: Expression, dtype: DataType) -> BooleanExpression:
        return self.visit(expr)

    def visit_function(self, name: str, args: list[Expression]) -> BooleanExpression:
        raise ValueError(f"Iceberg does not support function '{name}' in filter expressions")

    def visit_coalesce(self, args: list[Expression]) -> BooleanExpression:
        raise ValueError("Iceberg does not support coalesce in filter expressions")

    def visit_and(self, left: Expression, right: Expression) -> BooleanExpression:
        return And(left=self.visit(left), right=self.visit(right))

    def visit_or(self, left: Expression, right: Expression) -> BooleanExpression:
        return Or(left=self.visit(left), right=self.visit(right))

    def visit_not(self, expr: Expression) -> BooleanExpression:
        return Not(child=self.visit(expr))

    def visit_equal(self, left: Expression, right: Expression) -> BooleanExpression:
        ref, lit, _ = self.visit_lhs_rhs(left, right)
        return EqualTo(term=ref, value=lit)

    def visit_not_equal(self, left: Expression, right: Expression) -> BooleanExpression:
        ref, lit, _ = self.visit_lhs_rhs(left, right)
        return NotEqualTo(term=ref, value=lit)

    def visit_less_than(self, left: Expression, right: Expression) -> BooleanExpression:
        ref, lit, swapped = self.visit_lhs_rhs(left, right)
        if swapped:
            return GreaterThan(term=ref, value=lit)
        else:
            return LessThan(term=ref, value=lit)

    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> BooleanExpression:
        ref, lit, swapped = self.visit_lhs_rhs(left, right)
        if swapped:
            return GreaterThanOrEqual(term=ref, value=lit)
        else:
            return LessThanOrEqual(term=ref, value=lit)

    def visit_greater_than(self, left: Expression, right: Expression) -> BooleanExpression:
        ref, lit, swapped = self.visit_lhs_rhs(left, right)
        if swapped:
            return LessThan(term=ref, value=lit)
        else:
            return GreaterThan(term=ref, value=lit)

    def visit_greater_than_or_equal(self, left: Expression, right: Expression) -> BooleanExpression:
        ref, lit, swapped = self.visit_lhs_rhs(left, right)
        if swapped:
            return LessThanOrEqual(term=ref, value=lit)
        else:
            return GreaterThanOrEqual(term=ref, value=lit)

    def visit_between(self, expr: Expression, lower: Expression, upper: Expression) -> BooleanExpression:
        ref = self.visit_as_ref(expr)
        lo = self.coerce(ref, self.visit_as_lit(lower))
        hi = self.coerce(ref, self.visit_as_lit(upper))
        return And(
            left=GreaterThanOrEqual(term=ref, value=lo),
            right=LessThanOrEqual(term=ref, value=hi),
        )

    def visit_is_in(self, expr: Expression, items: list[Expression]) -> BooleanExpression:
        ref = self.visit_as_ref(expr)
        literals = [self.coerce(ref, self.visit_as_lit(item)) for item in items]
        return In(term=ref, values=set(literals))

    def visit_is_null(self, expr: Expression) -> BooleanExpression:
        return IsNull(term=self.visit_as_ref(expr))

    def visit_not_null(self, expr: Expression) -> BooleanExpression:
        return NotNull(term=self.visit_as_ref(expr))

    ##
    # Helpers
    ##

    def coerce(self, ref: Reference, lit: Literal) -> Literal:
        """Coerce a literal to match the referenced column's type when pyiceberg can't."""
        if self._schema is None:
            return lit
        try:
            field = self._schema.find_field(ref.name)
        except ValueError:
            return lit
        field_type = field.field_type
        if not isinstance(field_type, _TIMESTAMP_TYPES):
            return lit
        # date-only string → full ISO-8601 timestamp string
        if isinstance(lit, StringLiteral) and _DATE_ONLY_RE.fullmatch(lit.value):
            suffix = "T00:00:00+00:00" if isinstance(field_type, _TIMESTAMPTZ_TYPES) else "T00:00:00"
            return literal(lit.value + suffix)
        # DateLiteral → TimestampLiteral via datetime
        if isinstance(lit, DateLiteral):
            dt = datetime.combine(days_to_date(lit.value), datetime.min.time())
            if isinstance(field_type, _TIMESTAMPTZ_TYPES):
                dt = dt.replace(tzinfo=timezone.utc)
            return literal(dt)
        return lit

    def visit_lhs_rhs(
        self,
        lhs: Expression,
        rhs: Expression,
    ) -> tuple[Reference, Literal, bool]:
        """Visit a left-hand side and right-hand side expression, returning the reference, literal, and whether the left-hand side is the reference."""
        lv, rv = self.visit(lhs), self.visit(rhs)
        if isinstance(lv, Reference) and isinstance(rv, Literal):
            return lv, self.coerce(lv, rv), False
        if isinstance(rv, Reference) and isinstance(lv, Literal):
            return rv, self.coerce(rv, lv), True
        raise ValueError(
            f"Expected one column reference and one literal, got {type(lv).__name__} and {type(rv).__name__}"
        )

    def visit_as_ref(self, expr: Expression) -> Reference:
        result = self.visit(expr)
        if not isinstance(result, Reference):
            raise ValueError(f"Expected a column reference, got {type(result).__name__}")
        return result

    def visit_as_lit(self, expr: Expression) -> Literal:
        result = self.visit(expr)
        if not isinstance(result, Literal):
            raise ValueError(f"Expected a literal value, got {type(result).__name__}")
        return result
