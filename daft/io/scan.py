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
from daft.io.pushdowns import Pushdowns, Term

if TYPE_CHECKING:
    from collections.abc import Iterator

    from daft.daft import PyExpr
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
        raise NotImplementedError()

    @abc.abstractmethod
    def display_name(self) -> str:
        return self.__class__.__name__

    @abc.abstractmethod
    def partitioning_keys(self) -> list[PartitionField]:
        raise NotImplementedError()

    @abc.abstractmethod
    def can_absorb_filter(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def can_absorb_limit(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def can_absorb_select(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def multiline_display(self) -> list[str]:
        raise NotImplementedError()

    @abc.abstractmethod
    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
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
        columns (list[str] | None): Optional list of column names to project.
        predicate (Sexp | None): Optional filter predicate to apply to rows.
        partition_predicate (Sexp | None): Optional filter predicate to apply to partitions.
        limit (int | None): Optional limit on the number of rows to return.
    """

    columns: list[str] | None = None
    predicate: Term | None = None
    partition_predicate: Term | None = None
    limit: int | None = None

    @classmethod
    def _from_pypushdowns(cls, pushdowns: PyPushdowns) -> ScanPushdowns:
        return ScanPushdowns(
            columns=pushdowns.columns,
            predicate=cls._to_term(pushdowns.filters),
            partition_predicate=cls._to_term(pushdowns.partition_filters),
            limit=pushdowns.limit,
        )

    @classmethod
    def _to_term(cls, pyexpr: PyExpr | None) -> Term | None:
        return PyPushdowns._to_term(pyexpr) if pyexpr else None
