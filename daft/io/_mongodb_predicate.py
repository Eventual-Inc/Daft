from __future__ import annotations

import datetime
import logging
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from daft.expressions.visitor import PredicateVisitor

if TYPE_CHECKING:
    from daft.daft import PyExpr
    from daft.expressions import Expression

logger = logging.getLogger(__name__)

MongoPredicate = dict[str, Any]
_I64_MIN = -(2**63)
_I64_MAX = 2**63 - 1
_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


@dataclass(frozen=True, slots=True)
class _ColRef:
    name: str


@dataclass(frozen=True, slots=True)
class _Lit:
    value: Any


class _Unsupported:
    pass


UNSUPPORTED = _Unsupported()


class MongoPredicateVisitor(PredicateVisitor[Any]):
    """Converts safe Daft predicate expressions to MongoDB filter documents."""

    def visit_col(self, name: str) -> _ColRef:
        return _ColRef(name)

    def visit_lit(self, value: Any) -> _Lit | _Unsupported:
        value = _json_safe_value(value)
        if value is UNSUPPORTED:
            return UNSUPPORTED
        return _Lit(value)

    def visit_alias(self, expr: Expression, alias: str) -> Any:
        return self.visit(expr)

    def visit_cast(self, expr: Expression, dtype: Any) -> _Unsupported:
        return UNSUPPORTED

    def visit_try_cast(self, expr: Expression, dtype: Any) -> _Unsupported:
        return UNSUPPORTED

    def visit_coalesce(self, args: list[Expression]) -> _Unsupported:
        return UNSUPPORTED

    def visit_function(self, name: str, args: list[Expression]) -> _Unsupported:
        return UNSUPPORTED

    def visit_list(self, items: list[Expression]) -> _Lit | _Unsupported:
        values = [self.visit(item) for item in items]
        if any(not isinstance(value, _Lit) for value in values):
            return UNSUPPORTED
        return _Lit([value.value for value in values])

    def visit_and(self, left: Expression, right: Expression) -> MongoPredicate | _Unsupported:
        left_pred = self.visit(left)
        right_pred = self.visit(right)
        if _is_predicate(left_pred) and _is_predicate(right_pred):
            return {"$and": [left_pred, right_pred]}
        return UNSUPPORTED

    def visit_or(self, left: Expression, right: Expression) -> MongoPredicate | _Unsupported:
        left_pred = self.visit(left)
        right_pred = self.visit(right)
        if _is_predicate(left_pred) and _is_predicate(right_pred):
            return {"$or": [left_pred, right_pred]}
        return UNSUPPORTED

    def visit_not(self, expr: Expression) -> _Unsupported:
        return UNSUPPORTED

    def visit_equal(self, left: Expression, right: Expression) -> MongoPredicate | _Unsupported:
        return self._cmp(left, right, "$eq")

    def visit_not_equal(self, left: Expression, right: Expression) -> MongoPredicate | _Unsupported:
        lhs, rhs = self.visit(left), self.visit(right)
        if isinstance(lhs, _ColRef) and isinstance(rhs, _Lit) and rhs.value is not None:
            return _not_equal_predicate(lhs.name, rhs.value)
        if isinstance(rhs, _ColRef) and isinstance(lhs, _Lit) and lhs.value is not None:
            return _not_equal_predicate(rhs.name, lhs.value)
        return UNSUPPORTED

    def visit_less_than(self, left: Expression, right: Expression) -> MongoPredicate | _Unsupported:
        return self._cmp(left, right, "$lt")

    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> MongoPredicate | _Unsupported:
        return self._cmp(left, right, "$lte")

    def visit_greater_than(self, left: Expression, right: Expression) -> MongoPredicate | _Unsupported:
        return self._cmp(left, right, "$gt")

    def visit_greater_than_or_equal(self, left: Expression, right: Expression) -> MongoPredicate | _Unsupported:
        return self._cmp(left, right, "$gte")

    def visit_between(self, expr: Expression, lower: Expression, upper: Expression) -> MongoPredicate | _Unsupported:
        col = self.visit(expr)
        lower_value = self.visit(lower)
        upper_value = self.visit(upper)
        if (
            isinstance(col, _ColRef)
            and isinstance(lower_value, _Lit)
            and isinstance(upper_value, _Lit)
            and lower_value.value is not None
            and upper_value.value is not None
        ):
            return {col.name: {"$gte": lower_value.value, "$lte": upper_value.value}}
        return UNSUPPORTED

    def visit_is_in(self, expr: Expression, items: list[Expression]) -> MongoPredicate | _Unsupported:
        col = self.visit(expr)
        values = [self.visit(item) for item in items]
        if isinstance(col, _ColRef) and all(isinstance(value, _Lit) for value in values):
            literal_values = [value.value for value in values]
            if all(value is not None for value in literal_values):
                return {col.name: {"$in": literal_values}}
        return UNSUPPORTED

    def visit_is_null(self, expr: Expression) -> MongoPredicate | _Unsupported:
        col = self.visit(expr)
        if isinstance(col, _ColRef):
            return {col.name: None}
        return UNSUPPORTED

    def visit_not_null(self, expr: Expression) -> MongoPredicate | _Unsupported:
        col = self.visit(expr)
        if isinstance(col, _ColRef):
            return {col.name: {"$exists": True, "$ne": None}}
        return UNSUPPORTED

    def _cmp(self, left: Expression, right: Expression, op: str) -> MongoPredicate | _Unsupported:
        lhs, rhs = self.visit(left), self.visit(right)
        if isinstance(lhs, _ColRef) and isinstance(rhs, _Lit) and rhs.value is not None:
            if op == "$eq":
                return {lhs.name: rhs.value}
            return {lhs.name: {op: rhs.value}}
        if isinstance(rhs, _ColRef) and isinstance(lhs, _Lit) and lhs.value is not None:
            reversed_op = _REVERSE_COMPARISON_OPS[op]
            if reversed_op == "$eq":
                return {rhs.name: lhs.value}
            return {rhs.name: {reversed_op: lhs.value}}
        return UNSUPPORTED


class _ConjunctionSplitter(PredicateVisitor[Any]):
    def visit_col(self, name: str) -> None:
        return None

    def visit_lit(self, value: Any) -> None:
        return None

    def visit_alias(self, expr: Expression, alias: str) -> None:
        return None

    def visit_cast(self, expr: Expression, dtype: Any) -> None:
        return None

    def visit_try_cast(self, expr: Expression, dtype: Any) -> None:
        return None

    def visit_coalesce(self, args: list[Expression]) -> None:
        return None

    def visit_function(self, name: str, args: list[Expression]) -> None:
        return None

    def visit_list(self, items: list[Expression]) -> None:
        return None

    def visit_and(self, left: Expression, right: Expression) -> list[Expression]:
        return _split_conjunction(left) + _split_conjunction(right)

    def visit_or(self, left: Expression, right: Expression) -> None:
        return None

    def visit_not(self, expr: Expression) -> None:
        return None

    def visit_equal(self, left: Expression, right: Expression) -> None:
        return None

    def visit_not_equal(self, left: Expression, right: Expression) -> None:
        return None

    def visit_less_than(self, left: Expression, right: Expression) -> None:
        return None

    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> None:
        return None

    def visit_greater_than(self, left: Expression, right: Expression) -> None:
        return None

    def visit_greater_than_or_equal(self, left: Expression, right: Expression) -> None:
        return None

    def visit_between(self, expr: Expression, lower: Expression, upper: Expression) -> None:
        return None

    def visit_is_in(self, expr: Expression, items: list[Expression]) -> None:
        return None

    def visit_is_null(self, expr: Expression) -> None:
        return None

    def visit_not_null(self, expr: Expression) -> None:
        return None


def convert_filters_to_mongo(
    py_filters: list[PyExpr] | PyExpr,
) -> tuple[list[PyExpr], list[PyExpr], MongoPredicate | None]:
    from daft.expressions import Expression

    if not isinstance(py_filters, list):
        py_filters = [py_filters]

    if not py_filters:
        return [], [], None

    visitor = MongoPredicateVisitor()
    pushed_filters: list[PyExpr] = []
    remaining_filters: list[PyExpr] = []
    predicates: list[MongoPredicate] = []

    for py_expr in py_filters:
        expr = Expression._from_pyexpr(py_expr)
        for conjunct in _split_conjunction(expr):
            predicate = visitor.visit(conjunct)
            if _is_predicate(predicate):
                pushed_filters.append(conjunct._expr)
                predicates.append(predicate)
            else:
                remaining_filters.append(conjunct._expr)
                logger.debug("Filter %s cannot be pushed down to MongoDB", conjunct)

    return pushed_filters, remaining_filters, _and_predicates(predicates)


def _split_conjunction(expr: Expression) -> list[Expression]:
    split = _ConjunctionSplitter().visit(expr)
    return split if split is not None else [expr]


def _and_predicates(predicates: list[MongoPredicate]) -> MongoPredicate | None:
    if not predicates:
        return None
    if len(predicates) == 1:
        return predicates[0]
    return {"$and": predicates}


def _is_predicate(value: Any) -> bool:
    return isinstance(value, dict)


def _not_equal_predicate(field: str, value: Any) -> MongoPredicate:
    return {"$and": [{field: {"$exists": True}}, {field: {"$ne": None}}, {field: {"$ne": value}}]}


def _json_safe_value(value: Any) -> Any:
    if value is None or isinstance(value, str | bool):
        return value
    if isinstance(value, int):
        return value if _I64_MIN <= value <= _I64_MAX else UNSUPPORTED
    if isinstance(value, float):
        return value if math.isfinite(value) else UNSUPPORTED
    if isinstance(value, datetime.datetime):
        return {"$date": {"$numberLong": str(_datetime_to_epoch_millis(value))}}
    if isinstance(value, datetime.date):
        value = datetime.datetime.combine(value, datetime.time(), tzinfo=datetime.timezone.utc)
        return {"$date": {"$numberLong": str(_datetime_to_epoch_millis(value))}}
    if isinstance(value, bytes):
        return UNSUPPORTED
    if isinstance(value, Mapping):
        converted: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                return UNSUPPORTED
            item = _json_safe_value(item)
            if item is UNSUPPORTED:
                return UNSUPPORTED
            converted[key] = item
        return converted
    if isinstance(value, Sequence):
        converted_values = [_json_safe_value(item) for item in value]
        if any(item is UNSUPPORTED for item in converted_values):
            return UNSUPPORTED
        return converted_values
    return UNSUPPORTED


def _datetime_to_epoch_millis(value: datetime.datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)
    else:
        value = value.astimezone(datetime.timezone.utc)
    delta = value - _EPOCH
    return ((delta.days * 86_400 + delta.seconds) * 1_000) + (delta.microseconds // 1_000)


_REVERSE_COMPARISON_OPS = {
    "$eq": "$eq",
    "$lt": "$gt",
    "$lte": "$gte",
    "$gt": "$lt",
    "$gte": "$lte",
}
