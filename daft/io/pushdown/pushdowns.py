from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import TYPE_CHECKING

from daft.daft import PyPushdowns
from daft.expressions import Expression
from daft.io.scan import ScanOperator

if TYPE_CHECKING:
    from collections.abc import Iterator

    from daft.daft import PyExpr, PyPartitionField, ScanTask
    from daft.schema import Schema


@dataclass(frozen=True)
class Pushdowns:
    """Pushdowns are sent to scan sources during query planning.

    Attributes:
        filters (Expression | None): Optional filter predicate to apply to rows.
        partition_filters (Expression | None): Optional partition filter predicate to apply to partitions or files.
        columns (list[str] | None): Optional list of column names to project.
        limit (int | None): Optional limit on the number of rows to return.
    """

    filters: Expression | None = None
    partition_filters: Expression | None = None
    columns: list[str] | None = None
    limit: int | None = None

    @classmethod
    def _from_pypushdowns(cls, pushdowns: PyPushdowns) -> Pushdowns:
        return Pushdowns(
            filters=(Expression._from_pyexpr(pushdowns.filters) if pushdowns.filters else None),
            partition_filters=(
                Expression._from_pyexpr(pushdowns.partition_filters) if pushdowns.partition_filters else None
            ),
            columns=pushdowns.columns,
            limit=pushdowns.limit,
        )

    @classmethod
    def empty(cls) -> Pushdowns:
        return cls()

    # TODO(Check if implementation is needed)
    def merge(self, other: Pushdowns) -> Pushdowns:
        return Pushdowns(
            filters=self.filters or other.filters,
            limit=min(self.limit, other.limit) if self.limit and other.limit else None,
            columns=self.columns or other.columns,
        )


class SupportsPushdownFilters(abc.ABC):
    """Mixin interface for ScanBuilder to enable filter pushdown functionality."""

    @abc.abstractmethod
    def push_filters(self, filters: list[PyExpr]) -> tuple[list[PyExpr], list[PyExpr]]:
        """Pushes down filters and returns filters that need post-scan evaluation.

        return tuple : (pushed_filters, post_filters)
        """
        raise NotImplementedError

    @abc.abstractmethod
    def pushed_filters(self) -> list[PyExpr]:
        """Returns all filters that were pushed to the data source.

        Includes:
        1. Fully pushed filters (no post-scan evaluation needed)
        2. Partially pushed filters (still need post-scan evaluation)
        3. Returns empty list if no filters were pushed
        """
        raise NotImplementedError


class PushdownEnabledScanOperator(ScanOperator):
    """Wrapper that applies pushdown configurations without modifying original operator."""

    def __init__(self, delegate: ScanOperator, pushdowns: PyPushdowns):
        self._delegate = delegate
        self._pushdowns = pushdowns

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        return self._delegate.to_scan_tasks(pushdowns)

    # Proxy all required methods
    def schema(self) -> Schema:
        return self._delegate.schema()

    def display_name(self) -> str:
        return self._delegate.display_name()

    def partitioning_keys(self) -> list[PyPartitionField]:
        return self._delegate.partitioning_keys()

    def can_absorb_filter(self) -> bool:
        return self._delegate.can_absorb_filter()

    def can_absorb_limit(self) -> bool:
        return self._delegate.can_absorb_limit()

    def can_absorb_select(self) -> bool:
        return self._delegate.can_absorb_select()

    def multiline_display(self) -> list[str]:
        return self._delegate.multiline_display()


def wrap_scan_operator(operator: ScanOperator, pushdowns: PyPushdowns) -> ScanOperator:
    """Wrap the original operator with pushdown information."""
    # Collect already pushed filters (requires operator to implement the interface)
    pushed = []
    if operator.can_absorb_filter() and isinstance(operator, SupportsPushdownFilters):
        pushed = operator.pushed_filters()

    combined_filter = None
    if pushed:
        combined_filter = pushed[0]
        for expr in pushed[1:]:
            combined_filter = combined_filter.__and__(expr)

    new_pushdowns = PyPushdowns(
        filters=combined_filter,
        limit=pushdowns.limit if pushdowns else None,
        columns=pushdowns.columns if pushdowns else None,
        partition_filters=pushdowns.partition_filters if pushdowns else None,
    )

    return PushdownEnabledScanOperator(operator, new_pushdowns)
