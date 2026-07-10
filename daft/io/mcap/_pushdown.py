from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from daft.expressions.visitor import PredicateVisitor

if TYPE_CHECKING:
    from daft import Expression
    from daft.logical.schema import DataType


UINT64_MAX = (1 << 64) - 1


@dataclass(frozen=True)
class McapFilter:
    topics: frozenset[str] | None = None
    start_time: int | None = None
    end_time: int | None = None
    exact: bool = True
    empty: bool = False

    def intersect(self, other: McapFilter) -> McapFilter:
        if self.topics is None:
            topics = other.topics
        elif other.topics is None:
            topics = self.topics
        else:
            topics = self.topics & other.topics

        starts = [value for value in (self.start_time, other.start_time) if value is not None]
        ends = [value for value in (self.end_time, other.end_time) if value is not None]
        start_time = max(starts) if starts else None
        end_time = min(ends) if ends else None
        empty = (
            self.empty
            or other.empty
            or topics == frozenset()
            or (start_time is not None and end_time is not None and start_time >= end_time)
        )
        return McapFilter(
            topics=topics,
            start_time=start_time,
            end_time=end_time,
            exact=self.exact and other.exact,
            empty=empty,
        )


UNSUPPORTED = McapFilter(exact=False)


def explicit_mcap_filter(
    topics: list[str] | None,
    start_time: int | None,
    end_time: int | None,
) -> McapFilter:
    if start_time is not None and (
        isinstance(start_time, bool) or not isinstance(start_time, int) or not 0 <= start_time <= UINT64_MAX
    ):
        raise ValueError(f"start_time must fit in an unsigned 64-bit integer, got {start_time}")
    if end_time is not None and (
        isinstance(end_time, bool) or not isinstance(end_time, int) or not 0 <= end_time <= UINT64_MAX
    ):
        raise ValueError(f"end_time must fit in an unsigned 64-bit integer, got {end_time}")
    if topics is not None and any(not isinstance(topic, str) for topic in topics):
        raise ValueError("topics must contain only strings")
    return McapFilter(
        topics=None if topics is None else frozenset(topics),
        start_time=start_time,
        end_time=end_time,
        empty=(topics == [])
        or end_time == 0
        or (start_time is not None and end_time is not None and start_time >= end_time),
    )


class _McapPredicateVisitor(PredicateVisitor[McapFilter]):
    @staticmethod
    def _operand(expr: Expression) -> tuple[str, Any] | None:
        if expr.is_column():
            return ("column", expr.column_name())
        if expr.is_literal():
            return ("literal", expr.as_py())
        return None

    def _comparison(self, left: Expression, right: Expression, operator: str) -> McapFilter:
        lhs = self._operand(left)
        rhs = self._operand(right)
        if lhs is None or rhs is None:
            return UNSUPPORTED
        if lhs[0] == "literal" and rhs[0] == "column":
            lhs, rhs = rhs, lhs
            operator = {
                "equal": "equal",
                "less_than": "greater_than",
                "less_than_or_equal": "greater_than_or_equal",
                "greater_than": "less_than",
                "greater_than_or_equal": "less_than_or_equal",
            }[operator]
        if lhs[0] != "column" or rhs[0] != "literal":
            return UNSUPPORTED

        column = lhs[1]
        value = rhs[1]
        if column == "topic" and operator == "equal" and isinstance(value, str):
            return McapFilter(topics=frozenset({value}))
        if column != "log_time" or isinstance(value, bool) or not isinstance(value, int):
            return UNSUPPORTED
        if not 0 <= value <= UINT64_MAX:
            return UNSUPPORTED

        if operator == "equal":
            return McapFilter(start_time=value, end_time=None if value == UINT64_MAX else value + 1)
        if operator == "less_than":
            return McapFilter(end_time=value, empty=value == 0)
        if operator == "less_than_or_equal":
            return McapFilter(end_time=None if value == UINT64_MAX else value + 1)
        if operator == "greater_than":
            return McapFilter(start_time=value + 1, empty=value == UINT64_MAX)
        if operator == "greater_than_or_equal":
            return McapFilter(start_time=value)
        return UNSUPPORTED

    def visit_and(self, left: Expression, right: Expression) -> McapFilter:
        return self.visit(left).intersect(self.visit(right))

    def visit_or(self, left: Expression, right: Expression) -> McapFilter:
        return UNSUPPORTED

    def visit_not(self, expr: Expression) -> McapFilter:
        return UNSUPPORTED

    def visit_equal(self, left: Expression, right: Expression) -> McapFilter:
        return self._comparison(left, right, "equal")

    def visit_not_equal(self, left: Expression, right: Expression) -> McapFilter:
        return UNSUPPORTED

    def visit_less_than(self, left: Expression, right: Expression) -> McapFilter:
        return self._comparison(left, right, "less_than")

    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> McapFilter:
        return self._comparison(left, right, "less_than_or_equal")

    def visit_greater_than(self, left: Expression, right: Expression) -> McapFilter:
        return self._comparison(left, right, "greater_than")

    def visit_greater_than_or_equal(self, left: Expression, right: Expression) -> McapFilter:
        return self._comparison(left, right, "greater_than_or_equal")

    def visit_between(self, expr: Expression, lower: Expression, upper: Expression) -> McapFilter:
        return self._comparison(expr, lower, "greater_than_or_equal").intersect(
            self._comparison(expr, upper, "less_than_or_equal")
        )

    def visit_is_in(self, expr: Expression, items: list[Expression]) -> McapFilter:
        operand = self._operand(expr)
        if operand != ("column", "topic"):
            return UNSUPPORTED
        topics: list[str] = []
        for item in items:
            value = self._operand(item)
            if value is None or value[0] != "literal" or not isinstance(value[1], str):
                return UNSUPPORTED
            topics.append(value[1])
        return McapFilter(topics=frozenset(topics), empty=not topics)

    def visit_is_null(self, expr: Expression) -> McapFilter:
        return UNSUPPORTED

    def visit_not_null(self, expr: Expression) -> McapFilter:
        return UNSUPPORTED

    def visit_col(self, name: str) -> McapFilter:
        return UNSUPPORTED

    def visit_lit(self, value: Any) -> McapFilter:
        return UNSUPPORTED

    def visit_alias(self, expr: Expression, alias: str) -> McapFilter:
        return UNSUPPORTED

    def visit_cast(self, expr: Expression, dtype: DataType) -> McapFilter:
        return UNSUPPORTED

    def visit_try_cast(self, expr: Expression, dtype: DataType) -> McapFilter:
        return UNSUPPORTED

    def visit_function(self, name: str, args: list[Expression]) -> McapFilter:
        return UNSUPPORTED

    def visit_coalesce(self, args: list[Expression]) -> McapFilter:
        return UNSUPPORTED


def extract_mcap_filter(expr: Expression | None) -> McapFilter:
    return McapFilter() if expr is None else _McapPredicateVisitor().visit(expr)
