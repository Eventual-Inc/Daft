"""Filter conversion utilities for Paimon pushdowns.

This module provides utilities to convert Daft expressions to Paimon predicates
for filter pushdown optimization using the Visitor pattern.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from daft.expressions.visitor import PredicateVisitor

if TYPE_CHECKING:
    from pypaimon.common.predicate import Predicate
    from pypaimon.common.predicate_builder import PredicateBuilder
    from pypaimon.table.file_store_table import FileStoreTable

    from daft.daft import PyExpr
    from daft.expressions import Expression


logger = logging.getLogger(__name__)


# Use Any as type parameter since Predicate comes from optional pypaimon dependency
# At runtime, the actual Predicate type from pypaimon will be used
class PaimonPredicateVisitor(PredicateVisitor[Any]):
    """Converts Daft expressions to Paimon predicates using the Visitor pattern.

    This converter supports a subset of Daft expressions that can be
    translated to Paimon's predicate API. Unsupported expressions
    return None for post-scan evaluation.

    Supported operations:
    - Comparison: ==, !=, <, <=, >, >=
    - Is null / Is not null
    - Is in
    - Between (inclusive)
    - String: startswith, endswith, contains
    - Logical: and, or
    """

    def __init__(self, builder: PredicateBuilder) -> None:
        """Initialize the converter with a Paimon PredicateBuilder.

        Args:
            builder: Paimon's predicate builder instance
        """
        self._builder = builder

    # -------------------------------------------------------------------------
    # Base expression handlers (from ExpressionVisitor)
    # -------------------------------------------------------------------------

    def visit_col(self, name: str) -> Predicate | None:
        """Column references are not valid predicates on their own."""
        return None

    def visit_lit(self, value: Any) -> Predicate | None:
        """Literals are not valid predicates on their own."""
        return None

    def visit_alias(self, expr: Expression, alias: str) -> Predicate | None:
        """Strip alias and visit the underlying expression."""
        return self.visit(expr)

    def visit_cast(self, expr: Expression, dtype: Any) -> Predicate | None:
        """Cast expressions are not directly supported for pushdown."""
        return None

    def visit_coalesce(self, args: list[Expression]) -> Predicate | None:
        """Coalesce expressions are not supported for pushdown."""
        return None

    def visit_function(self, name: str, args: list[Expression]) -> Predicate | None:
        """Handle function calls like string operations.

        This is called for functions that don't have a dedicated visit_{name} method.
        """
        # String operations
        if name == "starts_with" and len(args) == 2:
            return self._convert_string_op(args[0], args[1], "startswith")
        elif name == "ends_with" and len(args) == 2:
            return self._convert_string_op(args[0], args[1], "endswith")
        elif name == "contains" and len(args) == 2:
            return self._convert_string_op(args[0], args[1], "contains")

        # Unsupported function
        logger.debug("Function '%s' is not supported for Paimon pushdown", name)
        return None

    # -------------------------------------------------------------------------
    # Predicate handlers (from PredicateVisitor)
    # -------------------------------------------------------------------------

    def visit_and(self, left: Expression, right: Expression) -> Predicate | None:
        """Convert AND logical operation."""
        left_pred = self.visit(left)
        right_pred = self.visit(right)

        # Only push down if both sides can be converted
        if left_pred is not None and right_pred is not None:
            return self._builder.and_predicates([left_pred, right_pred])

        # If either side cannot be converted, we don't push down this predicate
        # The parent expression will handle post-scan filtering
        return None

    def visit_or(self, left: Expression, right: Expression) -> Predicate | None:
        """Convert OR logical operation."""
        left_pred = self.visit(left)
        right_pred = self.visit(right)

        # Only push down if both sides can be converted
        if left_pred is not None and right_pred is not None:
            return self._builder.or_predicates([left_pred, right_pred])

        return None

    def visit_not(self, expr: Expression) -> Predicate | None:
        """NOT expressions are not directly supported for pushdown."""
        logger.debug("NOT expression is not supported for Paimon pushdown")
        return None

    def visit_equal(self, left: Expression, right: Expression) -> Predicate | None:
        """Convert equality comparison."""
        col_name, value = self._extract_col_and_value(left, right)
        if col_name is not None and value is not None:
            return self._builder.equal(col_name, value)
        return None

    def visit_not_equal(self, left: Expression, right: Expression) -> Predicate | None:
        """Convert not-equal comparison."""
        col_name, value = self._extract_col_and_value(left, right)
        if col_name is not None and value is not None:
            return self._builder.not_equal(col_name, value)
        return None

    def visit_less_than(self, left: Expression, right: Expression) -> Predicate | None:
        """Convert less-than comparison."""
        col_name, value = self._extract_col_and_value(left, right)
        if col_name is not None and value is not None:
            return self._builder.less_than(col_name, value)
        return None

    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> Predicate | None:
        """Convert less-than-or-equal comparison."""
        col_name, value = self._extract_col_and_value(left, right)
        if col_name is not None and value is not None:
            return self._builder.less_or_equal(col_name, value)
        return None

    def visit_greater_than(self, left: Expression, right: Expression) -> Predicate | None:
        """Convert greater-than comparison."""
        col_name, value = self._extract_col_and_value(left, right)
        if col_name is not None and value is not None:
            return self._builder.greater_than(col_name, value)
        return None

    def visit_greater_than_or_equal(self, left: Expression, right: Expression) -> Predicate | None:
        """Convert greater-than-or-equal comparison."""
        col_name, value = self._extract_col_and_value(left, right)
        if col_name is not None and value is not None:
            return self._builder.greater_or_equal(col_name, value)
        return None

    def visit_between(self, expr: Expression, lower: Expression, upper: Expression) -> Predicate | None:
        """Convert BETWEEN predicate."""
        col_name = self._get_col_name(expr)
        lower_val = self._get_literal_value(lower)
        upper_val = self._get_literal_value(upper)

        if col_name is not None and lower_val is not None and upper_val is not None:
            return self._builder.between(col_name, lower_val, upper_val)
        return None

    def visit_is_in(self, expr: Expression, items: list[Expression]) -> Predicate | None:
        """Convert IS IN predicate."""
        col_name = self._get_col_name(expr)
        if col_name is None:
            return None

        values = []
        for item in items:
            val = self._get_literal_value(item)
            if val is None:
                # Cannot push down if any item is not a literal
                return None
            values.append(val)

        return self._builder.is_in(col_name, values)

    def visit_is_null(self, expr: Expression) -> Predicate | None:
        """Convert IS NULL predicate."""
        col_name = self._get_col_name(expr)
        if col_name is not None:
            return self._builder.is_null(col_name)
        return None

    def visit_not_null(self, expr: Expression) -> Predicate | None:
        """Convert IS NOT NULL predicate."""
        col_name = self._get_col_name(expr)
        if col_name is not None:
            return self._builder.is_not_null(col_name)
        return None

    # -------------------------------------------------------------------------
    # Helper methods
    # -------------------------------------------------------------------------

    def _get_col_name(self, expr: Expression) -> str | None:
        """Extract column name from an expression.

        Uses PyExpr API to check if expression is a column reference.
        """
        py_expr = expr._expr
        if py_expr.is_column():
            return py_expr.name()
        return None

    def _get_literal_value(self, expr: Expression) -> Any:
        """Extract literal value from an expression.

        Uses PyExpr API to check if expression is a literal and get its value.
        """
        py_expr = expr._expr
        if py_expr.is_literal():
            return py_expr.as_py()
        return None

    def _extract_col_and_value(self, left: Expression, right: Expression) -> tuple[str | None, Any]:
        """Extract column name and literal value from a binary expression.

        Handles both `col == lit` and `lit == col` orderings.

        Returns:
            Tuple of (column_name, value) or (None, None) if not extractable.
        """
        left_col = self._get_col_name(left)
        right_col = self._get_col_name(right)
        left_val = self._get_literal_value(left)
        right_val = self._get_literal_value(right)

        # Case 1: col op lit
        if left_col is not None and right_val is not None:
            return (left_col, right_val)

        # Case 2: lit op col
        if right_col is not None and left_val is not None:
            return (right_col, left_val)

        return (None, None)

    def _convert_string_op(self, input_expr: Expression, pattern_expr: Expression, op: str) -> Predicate | None:
        """Convert string operations (startswith, endswith, contains)."""
        col_name = self._get_col_name(input_expr)
        pattern = self._get_literal_value(pattern_expr)

        if col_name is None or pattern is None:
            return None

        if op == "startswith":
            return self._builder.startswith(col_name, str(pattern))
        elif op == "endswith":
            return self._builder.endswith(col_name, str(pattern))
        elif op == "contains":
            return self._builder.contains(col_name, str(pattern))

        return None


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
