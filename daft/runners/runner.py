from __future__ import annotations

from abc import abstractmethod

from daft.logical.logical_plan import LogicalPlan
from daft.runners.partitioning import PartitionSetCache


class Runner:
    def __init__(self) -> None:
        self._part_set_cache = PartitionSetCache()

    def partition_cache(self) -> PartitionSetCache:
        return self._part_set_cache

    @abstractmethod
    def run(self, plan: LogicalPlan) -> str:
        ...
