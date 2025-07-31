from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import TYPE_CHECKING

from daft.expressions import Expression

if TYPE_CHECKING:
    from daft.daft import PyExpr, PyPushdowns


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
