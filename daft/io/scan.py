from __future__ import annotations

import abc
from dataclasses import dataclass

from daft.expressions.expressions import Expression
from daft.logical.schema import Field, Schema


@dataclass(frozen=True)
class ScanTask:
    file_type: str
    columns: list[str] | None
    limit: int | None


@dataclass(frozen=True)
class PartitionField:
    field: Field
    source_field: Field
    transform: Expression


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
