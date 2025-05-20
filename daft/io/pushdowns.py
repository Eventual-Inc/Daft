from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from daft.expressions import Expression

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
