from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

from pyiceberg.expressions import (
    And,
    BooleanExpression,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNaN,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEqualTo,
    NotNaN,
    NotNull,
    Or,
    Reference,
    StartsWith,
)
from pyiceberg.expressions.literals import DateLiteral, Literal, StringLiteral, TimestampLiteral, literal
from pyiceberg.types import TimestampType, TimestamptzType
from pyiceberg.utils.datetime import date_to_days, datetime_to_micros, days_to_date

from daft.datatype import DataType
from daft.expressions.visitor import PredicateVisitor
from daft.io.iceberg._metadata import convert_iceberg_data_type

if TYPE_CHECKING:
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.types import IcebergType

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


def _cast_preserves_values(source_type: IcebergType, target: DataType) -> bool:
    """Whether casting a column of ``source_type`` to ``target`` leaves every value unchanged.

    Dropping a cast from a pushed-down predicate is only safe when it cannot change which
    rows match: a lossy cast would make the pushed-down filter narrower than the user's
    predicate, so Iceberg could prune files that contain matching rows.
    """
    source = convert_iceberg_data_type(source_type)
    if source == target:
        return True
    # Integer widening is value-preserving, and pyiceberg binds integer literals against
    # the narrower field type (returning AlwaysFalse for out-of-range literals), so
    # comparisons keep the same semantics.
    return source == DataType.int32() and target == DataType.int64()


class IcebergPredicateVisitor(PredicateVisitor[BooleanExpression]):
    def __init__(self, schema: IcebergSchema | None = None) -> None:
        self._schema = schema

    def visit_col(self, name: str) -> BooleanExpression:
        return Reference(name)

    def visit_lit(self, value: Any) -> BooleanExpression:
        if isinstance(value, datetime):
            return TimestampLiteral(datetime_to_micros(value))
        if isinstance(value, date):
            return DateLiteral(date_to_days(value))
        return literal(value)

    def visit_alias(self, expr: Expression, alias: str) -> BooleanExpression:
        return self.visit(expr)

    def visit_cast(self, expr: Expression, dtype: DataType) -> BooleanExpression:
        return self._visit_cast(expr, dtype)

    def visit_try_cast(self, expr: Expression, dtype: DataType) -> BooleanExpression:
        return self._visit_cast(expr, dtype)

    def _visit_cast(self, expr: Expression, dtype: DataType) -> Reference:
        ref = self.visit(expr)
        if not isinstance(ref, Reference):
            raise ValueError(f"Expected a column reference inside cast, got {type(ref).__name__}")
        if self._schema is not None:
            try:
                source_type = self._schema.find_field(ref.name).field_type
            except ValueError:
                source_type = None
            if source_type is not None and _cast_preserves_values(source_type, dtype):
                return ref
        raise ValueError(
            f"Iceberg does not support cast(col({ref.name}) as {dtype}) in filter expressions: "
            "the cast can change values, so the pushed-down predicate would miss matching rows"
        )

    def visit_function(self, name: str, args: list[Expression]) -> BooleanExpression:
        # `is_nan`/`not_nan` have no dedicated `visit_*` hook on the base visitor,
        # so they arrive here as generic function calls with a single argument.
        if name in ("is_nan", "not_nan") and len(args) == 1:
            ref = self.visit_as_ref(args[0])
            return IsNaN(term=ref) if name == "is_nan" else NotNaN(term=ref)
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
        return EqualTo(term=ref, literal=lit)

    def visit_not_equal(self, left: Expression, right: Expression) -> BooleanExpression:
        ref, lit, _ = self.visit_lhs_rhs(left, right)
        return NotEqualTo(term=ref, literal=lit)

    def visit_less_than(self, left: Expression, right: Expression) -> BooleanExpression:
        ref, lit, swapped = self.visit_lhs_rhs(left, right)
        if swapped:
            return GreaterThan(term=ref, literal=lit)
        else:
            return LessThan(term=ref, literal=lit)

    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> BooleanExpression:
        ref, lit, swapped = self.visit_lhs_rhs(left, right)
        if swapped:
            return GreaterThanOrEqual(term=ref, literal=lit)
        else:
            return LessThanOrEqual(term=ref, literal=lit)

    def visit_greater_than(self, left: Expression, right: Expression) -> BooleanExpression:
        ref, lit, swapped = self.visit_lhs_rhs(left, right)
        if swapped:
            return LessThan(term=ref, literal=lit)
        else:
            return GreaterThan(term=ref, literal=lit)

    def visit_greater_than_or_equal(self, left: Expression, right: Expression) -> BooleanExpression:
        ref, lit, swapped = self.visit_lhs_rhs(left, right)
        if swapped:
            return LessThanOrEqual(term=ref, literal=lit)
        else:
            return GreaterThanOrEqual(term=ref, literal=lit)

    def visit_between(self, expr: Expression, lower: Expression, upper: Expression) -> BooleanExpression:
        ref = self.visit_as_ref(expr)
        lo = self.coerce(ref, self.visit_as_lit(lower))
        hi = self.coerce(ref, self.visit_as_lit(upper))
        return And(
            left=GreaterThanOrEqual(term=ref, literal=lo),
            right=LessThanOrEqual(term=ref, literal=hi),
        )

    def visit_is_in(self, expr: Expression, items: list[Expression]) -> BooleanExpression:
        ref = self.visit_as_ref(expr)
        literals = [self.coerce(ref, self.visit_as_lit(item)) for item in items]
        return In(term=ref, literals=set(literals))

    def visit_is_null(self, expr: Expression) -> BooleanExpression:
        return IsNull(term=self.visit_as_ref(expr))

    def visit_not_null(self, expr: Expression) -> BooleanExpression:
        return NotNull(term=self.visit_as_ref(expr))

    def visit_starts_with(self, input: Expression, prefix: Expression) -> BooleanExpression:
        ref = self.visit_as_ref(input)
        lit = self.visit_as_lit(prefix)
        return StartsWith(term=ref, literal=lit)

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
            return TimestampLiteral(datetime_to_micros(dt))
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
