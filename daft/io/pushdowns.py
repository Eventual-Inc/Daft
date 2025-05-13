from __future__ import annotations

from dataclasses import dataclass

from daft.daft import Pushdowns as PyPushdowns
from daft.expressions import Expression, col


@dataclass(frozen=True)
class Pushdowns:
    """Pushdowns is a python-friendly representation of daft's rust Pushdown type.

    The existing Pushdowns class comes from pyo3 which holds references to wrapper
    classes for daft's expression enum. This is not ammenable for python consumption.
    Here we have renamed Pushdowns to PyPushdowns for consistency with daft's other
    pyo3 wrapper classes.

    In Daft 0.5.0 we will change the pushdown parameter from daft.daft.Pushdowns to
    daft.io.Pushdowns. For now, please use `Pushdowns._from_pypushdowns(py_pushdowns)`
    to convert the rust expressions to this python pushdowns class.

    Attributes:
        projections (list[Expression] | None): Optional list of expressions (typically column references) to project.
        predicate (Expressions | None): Optional filter predicate to apply to rows.
        limit (int | None): Optional limit on the number of rows to return.
    """

    projections: list[Expression] | None = None
    predicate: Expression | None = None
    limit: int | None = None

    @classmethod
    def _from_pypushdowns(cls, pushdowns: PyPushdowns) -> Pushdowns:
        # use both the 'filters' and 'partition_filters' as the predicate
        predicate: Expression | None = None
        p1 = pushdowns.filters
        p2 = pushdowns.partition_filters

        # combine the two predicates with 'and' if necessary
        if p1 is not None and p2 is not None:
            predicate = Expression._from_pyexpr(p1 & p2)
        elif p1 is not None:
            predicate = Expression._from_pyexpr(p1)
        elif p2 is not None:
            predicate = Expression._from_pyexpr(p2)

        # convert each column name in PyPushdowns to an Expression.col
        if pushdowns.columns:
            projections = [col(column) for column in pushdowns.columns]
        else:
            projections = None

        # limit is just an int, so no conversion necessary
        limit = pushdowns.limit

        return Pushdowns(projections, predicate, limit)
