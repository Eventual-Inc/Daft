"""Filter conversion utilities for Paimon pushdowns.

This module provides utilities to convert Daft expressions to Paimon predicates
for filter pushdown optimization.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder

if TYPE_CHECKING:
    from daft.daft import PyExpr
    from daft.expressions import Expression

logger = logging.getLogger(__name__)


class PaimonPredicateConverter:
    """Converts Daft expressions to Paimon predicates.

    This converter supports a subset of Daft expressions that can be
    translated to Paimon's predicate API. Unsupported expressions
    are returned as-is for post-scan evaluation.

    Supported operations:
    - Comparison: ==, !=, <, <=, >, >=
    - Is null / Is not null
    - Is in / Is not in
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

    def convert(self, expr: Expression) -> Predicate | None:
        """Convert a Daft expression to a Paimon predicate.

        Args:
            expr: Daft expression to convert

        Returns:
            Predicate if conversion succeeded, None otherwise
        """
        try:
            return self._convert_expr(expr)
        except Exception as e:
            logger.debug(f"Failed to convert expression {expr} to Paimon predicate: {e}")
            return None

    def _convert_expr(self, expr: Expression) -> Predicate | None:
        """Internal conversion logic."""
        # Get the underlying PyExpr to check its properties
        py_expr = expr._expr
        expr_str = str(py_expr)

        # Check for column reference
        if py_expr.is_column():
            return None  # Column references are handled in comparisons

        # Check for literal
        if py_expr.is_literal():
            return None  # Literals are handled in comparisons

        # Check for IS_NULL - must check string representation first
        # because is_null() returns an expression, not a boolean
        if expr_str.startswith("is_null("):
            return self._convert_is_null(expr)

        # Check for IS_NOT_NULL (not_null)
        if expr_str.startswith("not_null("):
            return self._convert_is_not_null(expr)

        # Check for IS_IN
        if expr_str.startswith("is_in(") or " IN " in expr_str:
            return self._convert_is_in(expr)

        # Try to convert comparison operations
        try:
            # Comparison operations
            if " == " in expr_str:
                return self._convert_comparison(expr, "==")
            elif " != " in expr_str:
                return self._convert_comparison(expr, "!=")
            elif " < " in expr_str and " <= " not in expr_str:
                return self._convert_comparison(expr, "<")
            elif " <= " in expr_str:
                return self._convert_comparison(expr, "<=")
            elif " > " in expr_str and " >= " not in expr_str:
                return self._convert_comparison(expr, ">")
            elif " >= " in expr_str:
                return self._convert_comparison(expr, ">=")

            # String operations
            if ".startswith(" in expr_str:
                return self._convert_string_op(expr, "startswith")
            elif ".endswith(" in expr_str:
                return self._convert_string_op(expr, "endswith")
            elif ".contains(" in expr_str:
                return self._convert_string_op(expr, "contains")

            # Logical operations
            if " AND " in expr_str:
                return self._convert_logical(expr, "and")
            elif " OR " in expr_str:
                return self._convert_logical(expr, "or")

            # BETWEEN
            if " BETWEEN " in expr_str:
                return self._convert_between(expr)

        except Exception:
            pass

        # Unsupported expression type
        return None

    def _get_column_and_literal(self, expr: Expression) -> tuple[str, object] | None:
        """Extract column name and literal value from a binary expression.

        Returns (column_name, literal_value) or None if not a valid comparison.
        """
        try:
            # Get operands - for binary ops, there should be two
            # We need to find which is column and which is literal
            expr_str = str(expr._expr)

            # Try to parse column name and value from string representation
            # Format is typically: col(name) == lit(value) or col(name) = lit(value)
            import re

            # Match col(name) and lit(value) patterns
            col_match = re.search(r"col\(([^)]+)\)", expr_str)
            lit_match = re.search(r"lit\(([^)]+)\)", expr_str)

            if col_match and lit_match:
                col_name = col_match.group(1)
                lit_str = lit_match.group(1)

                # Parse literal value
                # Handle string literals (quoted)
                if lit_str.startswith("'") and lit_str.endswith("'"):
                    value = lit_str[1:-1]
                elif lit_str.startswith('"') and lit_str.endswith('"'):
                    value = lit_str[1:-1]
                # Handle None/null
                elif lit_str.lower() in ("none", "null"):
                    value = None
                # Handle numeric
                else:
                    try:
                        if "." in lit_str:
                            value = float(lit_str)
                        else:
                            value = int(lit_str)
                    except ValueError:
                        value = lit_str  # Keep as string if can't parse

                return (col_name, value)

        except Exception:
            pass

        return None

    def _convert_comparison(self, expr: Expression, op: str) -> Predicate | None:
        """Convert comparison expressions (==, !=, <, <=, >, >=)."""
        result = self._get_column_and_literal(expr)
        if result is None:
            return None

        col_name, value = result

        # Build the predicate based on operator
        try:
            if op == "==":
                return self._builder.equal(col_name, value)
            elif op == "!=":
                return self._builder.not_equal(col_name, value)
            elif op == "<":
                return self._builder.less_than(col_name, value)
            elif op == "<=":
                return self._builder.less_or_equal(col_name, value)
            elif op == ">":
                return self._builder.greater_than(col_name, value)
            elif op == ">=":
                return self._builder.greater_or_equal(col_name, value)
        except Exception as e:
            logger.debug(f"Failed to build comparison predicate: {e}")
            return None

        return None

    def _convert_is_null(self, expr: Expression) -> Predicate | None:
        """Convert IS NULL expression."""
        try:
            expr_str = str(expr._expr)
            import re
            col_match = re.search(r"col\(([^)]+)\)", expr_str)
            if col_match:
                col_name = col_match.group(1)
                return self._builder.is_null(col_name)
        except Exception as e:
            logger.debug(f"Failed to convert IS NULL: {e}")
        return None

    def _convert_is_not_null(self, expr: Expression) -> Predicate | None:
        """Convert IS NOT NULL expression."""
        try:
            expr_str = str(expr._expr)
            import re
            col_match = re.search(r"col\(([^)]+)\)", expr_str)
            if col_match:
                col_name = col_match.group(1)
                return self._builder.is_not_null(col_name)
        except Exception as e:
            logger.debug(f"Failed to convert IS NOT NULL: {e}")
        return None

    def _convert_is_in(self, expr: Expression) -> Predicate | None:
        """Convert IS IN expression."""
        try:
            expr_str = str(expr._expr)
            import re

            col_match = re.search(r"col\(([^)]+)\)", expr_str)
            # Match lit([...]) pattern for list literal
            list_match = re.search(r"lit\(\[(.*?)\]\)", expr_str)

            if col_match and list_match:
                col_name = col_match.group(1)
                list_str = list_match.group(1)

                # Parse list values
                values = []
                for item in list_str.split(","):
                    item = item.strip()
                    if item:
                        # Handle quoted strings
                        if (item.startswith("'") and item.endswith("'")) or \
                           (item.startswith('"') and item.endswith('"')):
                            values.append(item[1:-1])
                        else:
                            try:
                                values.append(int(item))
                            except ValueError:
                                try:
                                    values.append(float(item))
                                except ValueError:
                                    values.append(item)

                return self._builder.is_in(col_name, values)
        except Exception as e:
            logger.debug(f"Failed to convert IS IN: {e}")
        return None

    def _convert_between(self, expr: Expression) -> Predicate | None:
        """Convert BETWEEN expression."""
        try:
            expr_str = str(expr._expr)
            import re

            col_match = re.search(r"col\(([^)]+)\)", expr_str)
            # Match BETWEEN lit(x) AND lit(y)
            between_match = re.search(r"BETWEEN lit\(([^)]+)\) AND lit\(([^)]+)\)", expr_str)

            if col_match and between_match:
                col_name = col_match.group(1)
                lower = self._parse_literal(between_match.group(1))
                upper = self._parse_literal(between_match.group(2))
                return self._builder.between(col_name, lower, upper)
        except Exception as e:
            logger.debug(f"Failed to convert BETWEEN: {e}")
        return None

    def _convert_string_op(self, expr: Expression, op: str) -> Predicate | None:
        """Convert string operations (startswith, endswith, contains)."""
        try:
            expr_str = str(expr._expr)
            import re

            col_match = re.search(r"col\(([^)]+)\)", expr_str)
            # Match .op(lit('value'))
            pattern = r"\." + op + r"\(lit\(([^)]+)\)\)"
            string_match = re.search(pattern, expr_str)

            if col_match and string_match:
                col_name = col_match.group(1)
                value = self._parse_literal(string_match.group(1))
                if op == "startswith":
                    return self._builder.startswith(col_name, value)
                elif op == "endswith":
                    return self._builder.endswith(col_name, value)
                elif op == "contains":
                    return self._builder.contains(col_name, value)
        except Exception as e:
            logger.debug(f"Failed to convert string op {op}: {e}")
        return None

    def _convert_logical(self, expr: Expression, op: str) -> Predicate | None:
        """Convert logical AND/OR operations."""
        try:
            # For logical ops, we need to recursively convert both sides
            # This is more complex and requires parsing the expression tree
            # For now, return None and let Daft handle post-scan
            return None
        except Exception:
            return None

    def _parse_literal(self, lit_str: str) -> object:
        """Parse a literal value from its string representation."""
        lit_str = lit_str.strip()
        if (lit_str.startswith("'") and lit_str.endswith("'")) or \
           (lit_str.startswith('"') and lit_str.endswith('"')):
            return lit_str[1:-1]
        if lit_str.lower() in ("none", "null"):
            return None
        try:
            if "." in lit_str:
                return float(lit_str)
            return int(lit_str)
        except ValueError:
            return lit_str


def convert_filters_to_paimon(
    table: "pypaimon.table.file_store_table.FileStoreTable",
    py_filters: "list[PyExpr] | PyExpr",
) -> tuple[list["PyExpr"], list["PyExpr"], Predicate | None]:
    """Convert Daft filters to Paimon predicate.

    Args:
        table: Paimon table object (used to create predicate builder)
        py_filters: Single PyExpr filter or list of PyExpr filters to convert

    Returns:
        Tuple of (pushed_filters, remaining_filters, combined_predicate):
        - pushed_filters: Filters that were successfully converted
        - remaining_filters: Filters that need post-scan evaluation
        - combined_predicate: Combined Paimon predicate for pushdown, or None
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
    converter = PaimonPredicateConverter(predicate_builder)

    pushed_filters: list["PyExpr"] = []
    remaining_filters: list["PyExpr"] = []
    predicates: list[Predicate] = []

    for py_expr in py_filters:
        expr = Expression._from_pyexpr(py_expr)
        predicate = converter.convert(expr)

        if predicate is not None:
            pushed_filters.append(py_expr)
            predicates.append(predicate)
        else:
            remaining_filters.append(py_expr)
            logger.debug(f"Filter {expr} cannot be pushed down to Paimon")

    # Combine all predicates with AND
    combined_predicate: Predicate | None = None
    if predicates:
        combined_predicate = predicates[0]
        for pred in predicates[1:]:
            combined_predicate = predicate_builder.and_predicates([combined_predicate, pred])

    return pushed_filters, remaining_filters, combined_predicate
