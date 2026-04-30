from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from daft.expressions import Expression
from daft.expressions.visitor import _ColumnVisitor

if TYPE_CHECKING:
    from daft.daft import PyPushdowns


@dataclass(frozen=True)
class Pushdowns:
    """Pushdowns are sent to scan sources during query planning.

    Attributes:
        filters (Expression | None): Optional filter predicate to apply to rows.
        partition_filters (Expression | None): Optional partition filter predicate to apply to partitions or files.
        columns (list[str] | None): Optional list of column names to project.
        limit (int | None): Optional limit on the number of rows to return.
        aggregation (Expression | None): Optional aggregation expression for count pushdown.
    """

    filters: Expression | None = None
    partition_filters: Expression | None = None
    columns: list[str] | None = None
    limit: int | None = None
    aggregation: Expression | None = None

    @classmethod
    def _from_pypushdowns(cls, pushdowns: PyPushdowns) -> Pushdowns:
        return Pushdowns(
            filters=(Expression._from_pyexpr(pushdowns.filters) if pushdowns.filters else None),
            partition_filters=(
                Expression._from_pyexpr(pushdowns.partition_filters) if pushdowns.partition_filters else None
            ),
            columns=pushdowns.columns,
            limit=pushdowns.limit,
            aggregation=(Expression._from_pyexpr(pushdowns.aggregation) if pushdowns.aggregation else None),
        )

    def _to_pypushdowns(self) -> PyPushdowns:
        from daft.daft import PyPushdowns

        return PyPushdowns(
            filters=self.filters._expr if self.filters is not None else None,
            partition_filters=self.partition_filters._expr if self.partition_filters is not None else None,
            columns=list(self.columns) if self.columns is not None else None,
            limit=self.limit,
            aggregation=self.aggregation._expr if self.aggregation is not None else None,
        )

    def filter_required_column_names(self) -> set[str]:
        """Returns a set of field names that are required by the filter predicate."""
        return set() if self.filters is None else _ColumnVisitor().visit(self.filters)

    @classmethod
    def empty(cls) -> Pushdowns:
        return cls()
