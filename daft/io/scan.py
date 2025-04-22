from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import TYPE_CHECKING

from daft.daft import (
    PartitionField,
    PartitionTransform,
    ScanTask,
)
from daft.daft import (
    Pushdowns as PyPushdowns,
)
from daft.io.pushdowns import Pushdowns, Reference, Term

if TYPE_CHECKING:
    from collections.abc import Iterator

    from daft.logical.schema import Field, Schema


def make_partition_field(
    field: Field, source_field: Field | None = None, transform: PartitionTransform | None = None
) -> PartitionField:
    return PartitionField(
        field._field,
        source_field._field if source_field is not None else None,
        transform,
    )


class ScanOperator(abc.ABC):
    @abc.abstractmethod
    def schema(self) -> Schema:
        """Returns the schema of the data source."""
        raise NotImplementedError()

    @abc.abstractmethod
    def display_name(self) -> str:
        """Returns a human-readable name for this scan operator."""
        return self.__class__.__name__

    @abc.abstractmethod
    def partitioning_keys(self) -> list[PartitionField]:
        """Returns the partitioning keys for this data source."""
        raise NotImplementedError()

    @abc.abstractmethod
    def can_absorb_filter(self) -> bool:
        """Returns true if this scan can accept predicate pushdowns."""
        raise NotImplementedError()

    @abc.abstractmethod
    def can_absorb_limit(self) -> bool:
        """Returns true if this scan can accept limit pushdowns."""
        raise NotImplementedError()

    @abc.abstractmethod
    def can_absorb_select(self) -> bool:
        """Returns true if this scan can accept projection pushdowns."""
        raise NotImplementedError()

    @abc.abstractmethod
    def multiline_display(self) -> list[str]:
        """Returns a multi-line string representation of this scan operator."""
        raise NotImplementedError()

    @abc.abstractmethod
    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        """Converts this scan operator into scan tasks with the given pushdowns."""
        raise NotImplementedError()


@dataclass(frozen=True)
class ScanPushdowns(Pushdowns):
    """ScanPushdowns is a python-friendly representation of daft's rust Pushdown type.

    The existing Pushdowns class comes from pyo3 which holds references to wrapper
    classes for daft's expression enum. This is not ammenable for python consumption.

    In Daft 0.5.0 we will change the pushdown parameter from daft.daft.Pushdowns to
    daft.io.Pushdowns. For now, please use `ScanPushdowns._from_pypushdowns(py_pushdowns)`
    to convert the rust expressions to this python pushdowns class.

    As part of a migration plan to python-friendly pushdowns, we've introduced
    a python "ScanPushdowns" object which uses the long-term "Pushdowns" ABC.
    Additionally, we have renamed Pushdowns to PyPushdowns for consistency with
    daft's other pyo3 wrapper classes.

    Attributes:
        projections (list[Term] | None): Optional list of Terms (typically column references) to project.
        predicate (Term | None): Optional filter predicate to apply to rows.
        limit (int | None): Optional limit on the number of rows to return.
    """

    projections: list[Reference] | None = None
    predicate: Term | None = None
    limit: int | None = None

    @classmethod
    def _from_pypushdowns(cls, pypushdowns: PyPushdowns, schema: Schema) -> ScanPushdowns:
        # need the PySchema to send to rust side
        _schema = schema._schema

        # use both the 'filters' and 'partition_filters' as the predicate
        predicate: Term | None = None
        p1 = pypushdowns.filters
        p2 = pypushdowns.partition_filters

        # combine the two predicates with 'and' if necessary
        if p1 is not None and p2 is not None:
            predicate = PyPushdowns._to_term(p1 & p2, _schema)
        elif p1 is not None:
            predicate = PyPushdowns._to_term(p1, _schema)
        elif p2 is not None:
            predicate = PyPushdowns._to_term(p2, _schema)

        # field to index map is case-sensitive .. to do it proper we'll need rust-side work.
        fields = {field.name: index for index, field in enumerate(schema)}

        # convert each column name to a reference term, this requires resolving in the schema.
        if pypushdowns.columns:
            projections = [Reference(path=col, index=fields[col]) for col in pypushdowns.columns]
        else:
            projections = None

        # limit is just an int, so no conversion necessary
        limit = pypushdowns.limit

        return ScanPushdowns(projections, predicate, limit)
