from __future__ import annotations

import abc
from collections.abc import Iterator

from daft.daft import PartitionField, PartitionTransform, Pushdowns, ScanTask
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
    def to_scan_tasks(self, pushdowns: Pushdowns) -> Iterator[ScanTask]:
        raise NotImplementedError()
