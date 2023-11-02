from __future__ import annotations

import abc

from daft.daft import PartitionField
from daft.expressions.expressions import Expression
from daft.logical.schema import Field, Schema


def make_partition_field(
    field: Field, source_field: Field | None = None, transform: Expression | None = None
) -> PartitionField:
    return PartitionField(
        field._field,
        source_field._field if source_field is not None else None,
        transform._expr if transform is not None else None,
    )


class ScanOperator(abc.ABC):
    @abc.abstractmethod
    def schema(self) -> Schema:
        raise NotImplementedError()

    @abc.abstractmethod
    def partitioning_keys(self) -> list[PartitionField]:
        raise NotImplementedError()

    # @abc.abstractmethod
    # def num_partitions(self) -> int:
    #     raise NotImplementedError()

    # @abc.abstractmethod
    # def filter(self, predicate: Expression) -> tuple[bool, ScanOperator]:
    #     raise NotImplementedError()

    # @abc.abstractmethod
    # def limit(self, num: int) -> ScanOperator:
    #     raise NotImplementedError()

    # @abc.abstractmethod
    # def select(self, columns: list[str]) -> ScanOperator:
    #     raise NotImplementedError()

    # @abc.abstractmethod
    # def to_scan_tasks(self) -> Iterator[Any]:
    #     raise NotImplementedError()
