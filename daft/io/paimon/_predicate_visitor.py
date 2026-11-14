"""Filter conversion utilities for Paimon pushdowns.

This module provides utilities to convert Daft expressions to Paimon predicates
for filter pushdown optimization using the Visitor pattern.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from daft.expressions.visitor import PredicateVisitor

if TYPE_CHECKING:
    from pypaimon.common.predicate import Predicate
    from pypaimon.common.predicate_builder import PredicateBuilder
    from pypaimon.table.file_store_table import FileStoreTable

    from daft.daft import PyExpr
    from daft.expressions import Expression


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _ColRef:
    """Column reference marker to distinguish columns from literal values in the tree fold."""

    name: str


@dataclass(frozen=True, slots=True)
class _Lit:
    """Literal value marker, so that only real literals (not columns or computed values) are pushed down."""

    value: Any


class PaimonPredicateVisitor(PredicateVisitor[Any]):
    """Tree fold visitor that converts Daft expressions to Paimon predicates.

    Leaf nodes return markers (_ColRef for columns, _Lit for literals).
    Predicate nodes return Paimon Predicate objects, or None if unsupported.

    Supported operations:
    - Comparison: ==, !=, <, <=, >, >=
    - Is null / Is not null
    - Is in
    - Between (inclusive)
    - String: startswith, endswith, contains
    - Logical: and, or
    """

    def __init__(self, builder: PredicateBuilder) -> None:
        self._builder = builder

    # -- Leaf nodes --

    def visit_col(self, name: str) -> _ColRef:
        return _ColRef(name)

    def visit_lit(self, value: Any) -> _Lit:
        return _Lit(value)

    def visit_alias(self, expr: Expression, alias: str) -> Any:
        return self.visit(expr)

    def visit_cast(self, expr: Expression, dtype: Any) -> None:
        return None

    def visit_try_cast(self, expr: Expression, dtype: Any) -> None:
        return None

    def visit_coalesce(self, args: list[Expression]) -> None:
        return None

    def visit_function(self, name: str, args: list[Expression]) -> None:
        logger.debug("Function '%s' is not supported for Paimon pushdown", name)

    # -- Logical operators --

    def visit_and(self, left: Expression, right: Expression) -> Predicate | None:
        left_pred = self.visit(left)
        right_pred = self.visit(right)
        if left_pred is not None and right_pred is not None:
            return self._builder.and_predicates([left_pred, right_pred])
        return None

    def visit_or(self, left: Expression, right: Expression) -> Predicate | None:
        left_pred = self.visit(left)
        right_pred = self.visit(right)
        if left_pred is not None and right_pred is not None:
            return self._builder.or_predicates([left_pred, right_pred])
        return None

    def visit_not(self, expr: Expression) -> None:
        return None

    # -- Comparison operators --

    def _cmp(self, left: Expression, right: Expression, fn: Any, flipped_fn: Any) -> Predicate | None:
        """Fold a binary comparison of a column and a literal.

        ``fn`` handles ``col <op> lit`` and ``flipped_fn`` handles ``lit <op> col``
        (``lit < col`` is ``col > lit``). Anything else, such as two columns, is not pushed down.
        """
        lhs, rhs = self.visit(left), self.visit(right)
        if isinstance(lhs, _ColRef) and isinstance(rhs, _Lit):
            return fn(lhs.name, rhs.value)
        if isinstance(lhs, _Lit) and isinstance(rhs, _ColRef):
            return flipped_fn(rhs.name, lhs.value)
        return None

    def visit_equal(self, left: Expression, right: Expression) -> Predicate | None:
        return self._cmp(left, right, self._builder.equal, self._builder.equal)

    def visit_not_equal(self, left: Expression, right: Expression) -> Predicate | None:
        return self._cmp(left, right, self._builder.not_equal, self._builder.not_equal)

    def visit_less_than(self, left: Expression, right: Expression) -> Predicate | None:
        return self._cmp(left, right, self._builder.less_than, self._builder.greater_than)

    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> Predicate | None:
        return self._cmp(left, right, self._builder.less_or_equal, self._builder.greater_or_equal)

    def visit_greater_than(self, left: Expression, right: Expression) -> Predicate | None:
        return self._cmp(left, right, self._builder.greater_than, self._builder.less_than)

    def visit_greater_than_or_equal(self, left: Expression, right: Expression) -> Predicate | None:
        return self._cmp(left, right, self._builder.greater_or_equal, self._builder.less_or_equal)

    # -- Set/range predicates --

    def visit_between(self, expr: Expression, lower: Expression, upper: Expression) -> Predicate | None:
        col = self.visit(expr)
        if not isinstance(col, _ColRef):
            return None
        lower_val = self.visit(lower)
        upper_val = self.visit(upper)
        if not isinstance(lower_val, _Lit) or not isinstance(upper_val, _Lit):
            return None
        if lower_val.value is None or upper_val.value is None:
            return None
        return self._builder.between(col.name, lower_val.value, upper_val.value)

    def visit_is_in(self, expr: Expression, items: list[Expression]) -> Predicate | None:
        col = self.visit(expr)
        if not isinstance(col, _ColRef):
            return None
        values = [self.visit(item) for item in items]
        if any(not isinstance(v, _Lit) or v.value is None for v in values):
            return None
        return self._builder.is_in(col.name, [v.value for v in values])

    # -- Null predicates --

    def visit_is_null(self, expr: Expression) -> Predicate | None:
        col = self.visit(expr)
        return self._builder.is_null(col.name) if isinstance(col, _ColRef) else None

    def visit_not_null(self, expr: Expression) -> Predicate | None:
        col = self.visit(expr)
        return self._builder.is_not_null(col.name) if isinstance(col, _ColRef) else None

    # -- String predicates (dispatched via visit_ mechanism) --

    def visit_starts_with(self, input: Expression, prefix: Expression) -> Predicate | None:
        col = self.visit(input)
        if not isinstance(col, _ColRef):
            return None
        pattern = self.visit(prefix)
        if not isinstance(pattern, _Lit) or not isinstance(pattern.value, str):
            return None
        return self._builder.startswith(col.name, pattern.value)

    def visit_ends_with(self, input: Expression, suffix: Expression) -> Predicate | None:
        col = self.visit(input)
        if not isinstance(col, _ColRef):
            return None
        pattern = self.visit(suffix)
        if not isinstance(pattern, _Lit) or not isinstance(pattern.value, str):
            return None
        return self._builder.endswith(col.name, pattern.value)

    def visit_contains(self, input: Expression, substring: Expression) -> Predicate | None:
        col = self.visit(input)
        if not isinstance(col, _ColRef):
            return None
        pattern = self.visit(substring)
        if not isinstance(pattern, _Lit) or not isinstance(pattern.value, str):
            return None
        return self._builder.contains(col.name, pattern.value)


def convert_filters_to_paimon(
    table: FileStoreTable,
    py_filters: list[PyExpr] | PyExpr,
) -> tuple[list[PyExpr], list[PyExpr], Predicate | None]:
    """Convert Daft filters to Paimon predicate.

    This function takes Daft filter expressions and attempts to convert them
    to Paimon predicates for filter pushdown optimization. Filters that cannot
    be converted are returned for post-scan evaluation.

    Args:
        table: Paimon table object (used to create predicate builder)
        py_filters: Single PyExpr filter or list of PyExpr filters to convert

    Returns:
        Tuple of (pushed_filters, remaining_filters, combined_predicate):
        - pushed_filters: Filters that were successfully converted to Paimon predicates
        - remaining_filters: Filters that need post-scan evaluation
        - combined_predicate: Combined Paimon predicate for pushdown, or None if none could be converted
    """
    from daft.expressions import Expression

    # Handle single filter case
    if not isinstance(py_filters, list):
        py_filters = [py_filters]

    if not py_filters:
        return [], [], None

    # Create predicate builder
    read_builder = table.new_read_builder()
    predicate_builder = read_builder.new_predicate_builder()
    converter = PaimonPredicateVisitor(predicate_builder)

    pushed_filters: list[PyExpr] = []
    remaining_filters: list[PyExpr] = []
    predicates: list[Predicate] = []

    for py_expr in py_filters:
        expr = Expression._from_pyexpr(py_expr)
        predicate = converter.visit(expr)

        if predicate is not None:
            pushed_filters.append(py_expr)
            predicates.append(predicate)
        else:
            remaining_filters.append(py_expr)
            logger.debug("Filter %s cannot be pushed down to Paimon", expr)

    # Combine all predicates with AND
    combined_predicate: Predicate | None = None
    if predicates:
        combined_predicate = predicates[0]
        for pred in predicates[1:]:
            combined_predicate = predicate_builder.and_predicates([combined_predicate, pred])

    if pushed_filters:
        logger.debug(
            "Paimon filter pushdown: %d filters pushed, %d remaining",
            len(pushed_filters),
            len(remaining_filters),
        )

    return pushed_filters, remaining_filters, combined_predicate
