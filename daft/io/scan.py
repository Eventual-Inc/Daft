from __future__ import annotations

import abc
from typing import TYPE_CHECKING

from daft.daft import (
    PyPartitionField,
    PyPartitionTransform,
    PyPushdowns,
    ScanTask,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from daft.logical.schema import Field, Schema

    from .pushdowns import SupportsPushdownFilters


def make_partition_field(
    field: Field, source_field: Field | None = None, transform: PyPartitionTransform | None = None
) -> PyPartitionField:
    return PyPartitionField(
        field._field,
        source_field._field if source_field is not None else None,
        transform,
    )


class ScanOperator(abc.ABC):
    """ScanOperator is the legacy python DataSource ABC and is being migrated to daft.io.source.DataSource.

    In Daft 0.5.0 we will change the pushdown parameter from daft.daft.Pushdowns to
    daft.io.Pushdowns. For now, please use `Pushdowns._from_pypushdowns(py_pushdowns)`
    to convert the rust expressions to this python pushdowns class.
    """

    @abc.abstractmethod
    def schema(self) -> Schema:
        """Returns the schema of the data source."""
        raise NotImplementedError()

    @abc.abstractmethod
    def display_name(self) -> str:
        """Returns a human-readable name for this scan operator."""
        return self.__class__.__name__

    @abc.abstractmethod
    def partitioning_keys(self) -> list[PyPartitionField]:
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

    def as_pushdown_filter(self) -> SupportsPushdownFilters | None:
        """Returns this scan operator as a SupportsPushdownFilters if it supports pushdown filters."""
        raise NotImplementedError()

    def supports_count_pushdown(self) -> bool:
        """Returns true if this scan can accept count pushdowns."""
        return False
