from __future__ import annotations

from abc import abstractmethod

from daft.logical.logical_plan import LogicalPlan
from daft.runners.partitioning import (
    PartitionCacheEntry,
    PartitionSet,
    PartitionSetCache,
    PartitionSetFactory,
)


class Runner:
    def __init__(self) -> None:
        self._part_set_cache = PartitionSetCache()

    def get_partition_set_from_cache(self, pset_id: str) -> PartitionCacheEntry:
        return self._part_set_cache.get_partition_set(pset_id=pset_id)

    def put_partition_set_into_cache(self, pset: PartitionSet) -> PartitionCacheEntry:
        return self._part_set_cache.put_partition_set(pset=pset)

    @abstractmethod
    def partition_set_factory(self) -> PartitionSetFactory:
        ...

    @abstractmethod
    def run(self, plan: LogicalPlan) -> PartitionCacheEntry:
        ...

    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        return plan
