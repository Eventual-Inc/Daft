from __future__ import annotations

import datetime
import logging
import math
from typing import TYPE_CHECKING, Any

from daft.dependencies import pa, pc
from daft.expressions.visitor import PredicateVisitor

if TYPE_CHECKING:
    from daft.datatype import DataType
    from daft.expressions import Expression

logger = logging.getLogger(__name__)

# Delta writers may truncate timestamp stats to milliseconds, so a recorded max can sit below the true max.
_TIMESTAMP_MAX_SLACK = datetime.timedelta(milliseconds=1)


def _keep() -> pc.Expression:
    return pc.scalar(True)


class _Unsupported(Exception):
    """Raised for a subexpression that can't be turned into a stats check."""


class _Col:
    def __init__(self, physical: str, dtype: pa.DataType) -> None:
        self.physical = physical
        self.dtype = dtype

    @property
    def min(self) -> pc.Expression:
        return pc.field("min", self.physical)

    @property
    def max(self) -> pc.Expression | None:
        """Upper bound on the column's values, or None when the recorded max can't be trusted as one."""
        # String stats may be truncated prefixes, and Daft orders NaN above every float but Delta stats omit NaN.
        t = self.dtype
        if pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_binary(t) or pa.types.is_floating(t):
            return None
        if pa.types.is_timestamp(self.dtype):
            return pc.add(pc.field("max", self.physical), pa.scalar(_TIMESTAMP_MAX_SLACK))
        return pc.field("max", self.physical)

    @property
    def eq_max(self) -> pc.Expression | None:
        """Upper bound for equality-style checks, where a NaN row can never match a non-NaN literal."""
        if pa.types.is_floating(self.dtype):
            return pc.field("max", self.physical)
        return self.max

    @property
    def null_count(self) -> pc.Expression:
        return pc.field("null_count", self.physical)


class _Lit:
    def __init__(self, value: Any) -> None:
        self.value = value


def _keep_if_unknown(expr: pc.Expression) -> pc.Expression:
    # A file without stats for this column yields null here, and must be kept.
    return pc.if_else(pc.is_null(expr), True, expr)


class DeltaStatsPredicateVisitor(PredicateVisitor[Any]):
    """Rewrites a Daft predicate into a check over Delta add-action stats that is true when a file may match.

    Anything it can't prove becomes True, so a file is only dropped when its stats rule it out.
    """

    def __init__(self, columns: dict[str, tuple[str, pa.DataType]]) -> None:
        # logical name -> (physical stats name, stats arrow type)
        self._columns = columns

    def visit_col(self, name: str) -> Any:
        if name not in self._columns:
            raise _Unsupported(f"no stats for column {name!r}")
        physical, dtype = self._columns[name]
        if pa.types.is_nested(dtype):
            raise _Unsupported(f"nested column {name!r}")
        return _Col(physical, dtype)

    def visit_lit(self, value: Any) -> Any:
        return _Lit(value)

    def visit_alias(self, expr: Expression, alias: str) -> Any:
        return self.visit(expr)

    def visit_cast(self, expr: Expression, dtype: DataType) -> Any:
        inner = self.visit(expr)
        if isinstance(inner, _Lit):
            return inner
        raise _Unsupported("cast on a column")

    def visit_function(self, name: str, args: list[Expression]) -> Any:
        raise _Unsupported(f"function {name!r}")

    def visit_coalesce(self, args: list[Expression]) -> Any:
        raise _Unsupported("coalesce")

    def visit_and(self, left: Expression, right: Expression) -> pc.Expression:
        return self._predicate(left) & self._predicate(right)

    def visit_or(self, left: Expression, right: Expression) -> pc.Expression:
        return self._predicate(left) | self._predicate(right)

    def visit_not(self, expr: Expression) -> pc.Expression:
        return _keep()

    def visit_equal(self, left: Expression, right: Expression) -> pc.Expression:
        col, lit, _ = self._col_lit(left, right)
        return self._may_equal(col, lit)

    def visit_not_equal(self, left: Expression, right: Expression) -> pc.Expression:
        col, lit, _ = self._col_lit(left, right)
        # Only exact bounds can prove every row equals the literal.
        if pa.types.is_integer(col.dtype) or pa.types.is_date(col.dtype) or pa.types.is_boolean(col.dtype):
            v = self._scalar(col, lit)
            return _keep_if_unknown(~((col.min == v) & (pc.field("max", col.physical) == v)))
        return _keep()

    def visit_less_than(self, left: Expression, right: Expression) -> pc.Expression:
        col, lit, swapped = self._col_lit(left, right)
        return self._may_be_greater(col, lit, strict=True) if swapped else self._may_be_less(col, lit, strict=True)

    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> pc.Expression:
        col, lit, swapped = self._col_lit(left, right)
        return self._may_be_greater(col, lit, strict=False) if swapped else self._may_be_less(col, lit, strict=False)

    def visit_greater_than(self, left: Expression, right: Expression) -> pc.Expression:
        col, lit, swapped = self._col_lit(left, right)
        return self._may_be_less(col, lit, strict=True) if swapped else self._may_be_greater(col, lit, strict=True)

    def visit_greater_than_or_equal(self, left: Expression, right: Expression) -> pc.Expression:
        col, lit, swapped = self._col_lit(left, right)
        return self._may_be_less(col, lit, strict=False) if swapped else self._may_be_greater(col, lit, strict=False)

    def visit_between(self, expr: Expression, lower: Expression, upper: Expression) -> pc.Expression:
        col = self._as(expr, _Col)
        lo = self._scalar(col, self._as(lower, _Lit))
        hi = self._scalar(col, self._as(upper, _Lit))
        check = _keep_if_unknown(col.min <= hi)
        if col.eq_max is not None:
            check = check & _keep_if_unknown(col.eq_max >= lo)
        return check

    def visit_is_in(self, expr: Expression, items: list[Expression]) -> pc.Expression:
        col = self._as(expr, _Col)
        if not items:
            return _keep()
        checks = [self._may_equal(col, self._as(item, _Lit)) for item in items]
        result = checks[0]
        for check in checks[1:]:
            result = result | check
        return result

    def visit_is_null(self, expr: Expression) -> pc.Expression:
        col = self._as(expr, _Col)
        return _keep_if_unknown(col.null_count > 0)

    def visit_not_null(self, expr: Expression) -> pc.Expression:
        col = self._as(expr, _Col)
        return _keep_if_unknown(col.null_count < pc.field("num_records"))

    ##
    # Helpers
    ##

    def _predicate(self, expr: Expression) -> pc.Expression:
        # Each side of AND/OR falls back independently, so one opaque branch doesn't disable the other.
        try:
            result = self.visit(expr)
        except _Unsupported:
            return _keep()
        if not isinstance(result, pc.Expression):
            return _keep()
        return result

    def _may_equal(self, col: _Col, lit: _Lit) -> pc.Expression:
        v = self._scalar(col, lit)
        check = _keep_if_unknown(col.min <= v)
        if col.eq_max is not None:
            check = check & _keep_if_unknown(col.eq_max >= v)
        return check

    def _may_be_less(self, col: _Col, lit: _Lit, strict: bool) -> pc.Expression:
        v = self._scalar(col, lit)
        return _keep_if_unknown(col.min < v if strict else col.min <= v)

    def _may_be_greater(self, col: _Col, lit: _Lit, strict: bool) -> pc.Expression:
        v = self._scalar(col, lit)
        if col.max is None:
            return _keep()
        return _keep_if_unknown(col.max > v if strict else col.max >= v)

    def _scalar(self, col: _Col, lit: _Lit) -> pa.Scalar:
        """Convert a literal to the column's stats type, refusing any conversion that could change its value."""
        value, dtype = lit.value, col.dtype
        if value is None or isinstance(value, bool) != pa.types.is_boolean(dtype):
            raise _Unsupported("null or boolean mismatch")
        if isinstance(value, float) and (math.isnan(value) or pa.types.is_integer(dtype)):
            # pyarrow silently truncates floats into integer types.
            raise _Unsupported("float literal against integer column")
        if isinstance(value, int) and pa.types.is_floating(dtype) and abs(value) > 2**53:
            raise _Unsupported("int literal not exactly representable as float")
        if pa.types.is_timestamp(dtype):
            if not isinstance(value, datetime.datetime):
                raise _Unsupported("non-datetime literal against timestamp column")
            if (value.tzinfo is None) != (dtype.tz is None):
                raise _Unsupported("timezone mismatch")
        try:
            return pa.scalar(value, type=dtype)
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError, OverflowError, TypeError) as e:
            raise _Unsupported(f"cannot convert {value!r} to {dtype}") from e

    def _col_lit(self, left: Expression, right: Expression) -> tuple[_Col, _Lit, bool]:
        lv, rv = self.visit(left), self.visit(right)
        if isinstance(lv, _Col) and isinstance(rv, _Lit):
            return lv, rv, False
        if isinstance(rv, _Col) and isinstance(lv, _Lit):
            return rv, lv, True
        raise _Unsupported("expected one column and one literal")

    def _as(self, expr: Expression, kind: type) -> Any:
        result = self.visit(expr)
        if not isinstance(result, kind):
            raise _Unsupported(f"expected {kind.__name__}")
        return result


def convert_filter_to_stats_predicate(
    expr: Expression, columns: dict[str, tuple[str, pa.DataType]]
) -> pc.Expression | None:
    """Best-effort conversion of a Daft filter into a Delta add-action file-skipping predicate."""
    try:
        return DeltaStatsPredicateVisitor(columns)._predicate(expr)
    except Exception as e:
        logger.warning("Could not convert filter to Delta stats predicate, skipping file pruning: %s", e)
        return None
