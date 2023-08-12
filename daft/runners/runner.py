from __future__ import annotations

from abc import abstractmethod
from typing import Generic, Iterator, TypeVar

from daft.logical.builder import LogicalPlanBuilder
from daft.runners.partitioning import (
    PartitionCacheEntry,
    PartitionSet,
    PartitionSetCache,
)
from daft.runners.runner_io import RunnerIO
from daft.table import Table

PartitionT = TypeVar("PartitionT")


class Runner(Generic[PartitionT]):
    def __init__(self) -> None:
        self._part_set_cache = PartitionSetCache()

    def get_partition_set_from_cache(self, pset_id: str) -> PartitionCacheEntry:
        return self._part_set_cache.get_partition_set(pset_id=pset_id)

    def put_partition_set_into_cache(self, pset: PartitionSet) -> PartitionCacheEntry:
        return self._part_set_cache.put_partition_set(pset=pset)

    @abstractmethod
    def runner_io(self) -> RunnerIO:
        ...

    @abstractmethod
    def run(self, builder: LogicalPlanBuilder) -> PartitionCacheEntry:
        ...

    @abstractmethod
    def run_iter(self, builder: LogicalPlanBuilder) -> Iterator[PartitionT]:
        """Similar to run(), but yield the individual partitions as they are completed."""
        ...

    @abstractmethod
    def run_iter_tables(self, builder: LogicalPlanBuilder) -> Iterator[Table]:
        """Similar to run_iter(), but always dereference and yield Table objects."""
        ...
