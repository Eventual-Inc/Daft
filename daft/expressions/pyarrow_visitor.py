from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.dependencies import pc
from daft.expressions.visitor import PredicateVisitor

if TYPE_CHECKING:
    from daft.datatype import DataType
    from daft.expressions import Expression


class _PyArrowExpressionVisitor(PredicateVisitor[pc.Expression]):
    """This visitor does a tree fold into the pyarrow.compute.Expression domain."""

    def visit_col(self, name: str) -> pc.Expression:
        """Convert the daft column to pc field reference by name."""
        return pc.field(name)

    def visit_lit(self, value: Any) -> pc.Expression:
        """Convert the Literal to a pyarrow.compute.Scalar without check the type.

        From the pyarrow scalar docs:
        > value : bool, int, float or string
        > Python value of the scalar. Note that only a subset of types are currently supported.

        Coincidentally, this is what term is currently limited to.
        """
        return pc.scalar(value)

    def visit_alias(self, expr: Expression, alias: str) -> pc.Expression:
        """Convert an alias 'expression' by ... ignoring it .. as these aren't supposed to be expressions."""
        return self.visit(expr)

    def visit_cast(self, expr: Expression, dtype: DataType) -> pc.Expression:
        """Converts the cast with default safety and cast options because daft does not have these options."""
        pc_expr = self.visit(expr)
        pc_type = dtype.to_arrow_dtype()
        return pc_expr.cast(pc_type)

    def visit_list(self, items: list[Expression]) -> pc.Expression:
        raise ValueError("pyarrow.compute does not have a make_list function.")

    def visit_and(self, left: Expression, right: Expression) -> pc.Expression:
        pc_lhs = self.visit(left)
        pc_rhs = self.visit(right)
        return pc_lhs & pc_rhs

    def visit_or(self, left: Expression, right: Expression) -> pc.Expression:
        pc_lhs = self.visit(left)
        pc_rhs = self.visit(right)
        return pc_lhs | pc_rhs

    def visit_not(self, expr: Expression) -> pc.Expression:
        pc_expr = self.visit(expr)
        return ~pc_expr

    def visit_equal(self, left: Expression, right: Expression) -> pc.Expression:
        pc_lhs = self.visit(left)
        pc_rhs = self.visit(right)
        return pc_lhs == pc_rhs

    def visit_not_equal(self, left: Expression, right: Expression) -> pc.Expression:
        pc_lhs = self.visit(left)
        pc_rhs = self.visit(right)
        return pc_lhs != pc_rhs

    def visit_less_than(self, left: Expression, right: Expression) -> pc.Expression:
        pc_lhs = self.visit(left)
        pc_rhs = self.visit(right)
        return pc_lhs < pc_rhs

    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> pc.Expression:
        pc_lhs = self.visit(left)
        pc_rhs = self.visit(right)
        return pc_lhs <= pc_rhs

    def visit_greater_than(self, left: Expression, right: Expression) -> pc.Expression:
        pc_lhs = self.visit(left)
        pc_rhs = self.visit(right)
        return pc_lhs > pc_rhs

    def visit_greater_than_or_equal(self, left: Expression, right: Expression) -> pc.Expression:
        pc_lhs = self.visit(left)
        pc_rhs = self.visit(right)
        return pc_lhs >= pc_rhs

    def visit_between(self, expr: Expression, lower: Expression, upper: Expression) -> pc.Expression:
        """Convert between using le and ge since the bounds are inclusive."""
        pc_expr = self.visit(expr)
        pc_lower = self.visit(lower)
        pc_upper = self.visit(upper)
        return (pc_lower <= pc_expr) & (pc_expr <= pc_upper)

    def visit_is_in(self, expr: Expression, items: list[Expression]) -> pc.Expression:
        pc_expr = self.visit(expr)
        pc_items = [item.as_py() for item in items]
        return pc_expr.isin(pc_items)

    def visit_is_null(self, expr: Expression) -> pc.Expression:
        pc_expr = self.visit(expr)
        return pc_expr.is_null()

    def visit_not_null(self, expr: Expression) -> pc.Expression:
        pc_expr = self.visit(expr)
        return ~pc_expr.is_null()

    def visit_function(self, name: str, args: list[Expression]) -> pc.Expression:
        """Converting using either the EXACT function name or an override, otherwise error."""
        if hasattr(self, "_" + name):
            # special form, call the overriding method
            return getattr(self, "_" + name)(*args)
        else:
            # normal form, call the pyarrow.compute method
            pc_func = self._get_pc_func(name)
            pc_args = [self.visit(arg) for arg in args]
            return pc_func(*pc_args)

    def _get_pc_func(self, name: str) -> Any:
        """Resolve the pyarrow.compute function from the module, otherwise error."""
        try:
            pc_name = self._PC_FUNCTION_OVERRIDES.get(name, name)
            pc_func = getattr(pc, pc_name)
            return pc_func
        except AttributeError:
            raise ValueError(
                f"pyarrow.compute has no function '{name}', please see: https://arrow.apache.org/docs/python/api/compute.html."
            )

    # FUNCTION OVERRIDES
    #
    #   This maps daft functions to pyarrow.compute expression functions.
    #   https://arrow.apache.org/docs/python/api/compute.html
    #
    #   Normal forms map to strings.
    #   Special forms map to the special translation functions.
    #
    _PC_FUNCTION_OVERRIDES: dict[str, str] = {
        "plus": "add",
        "minus": "subtract",
        # math overrides
        "log": "logb",
        "negative": "negate",
        "arcsin": "asin",
        "arccos": "acos",
        "arctan": "atan",
        "arctan2": "atan2",
        # string overrides
        "capitalize": "utf8_capitalize",
    }

    def _round(self, input: Expression, precision: Expression) -> pc.Expression:
        pc_input = self.visit(input)
        pc_precision = precision.as_py()  # must be a literal
        return pc.round(pc_input, pc_precision)

    def _count_matches(
        self, input: Expression, pattern: Expression, whole_words: Expression, case_sensitive: Expression
    ) -> pc.Expression:
        pc_strings = self.visit(input)
        pc_pattern = pattern.as_py()  # must be literal
        pc_ignore_case = not case_sensitive.as_py()
        return pc.count_substring(pc_strings, pc_pattern, ignore_case=pc_ignore_case)

    def _contains(self, input: Expression, substring: Expression) -> pc.Expression:
        pc_strings = self.visit(input)
        pc_pattern = substring.as_py()  # must be literal
        return pc.match_substring(pc_strings, pc_pattern)

    def _ends_with(self, input: Expression, suffix: Expression) -> pc.Expression:
        pc_strings = self.visit(input)
        pc_pattern = suffix.as_py()  # must be literal
        return pc.ends_with(pc_strings, pc_pattern)

    def _starts_with(self, input: Expression, prefix: Expression) -> pc.Expression:
        pc_strings = self.visit(input)
        pc_pattern = prefix.as_py()  # must be literal
        return pc.starts_with(pc_strings, pc_pattern)
