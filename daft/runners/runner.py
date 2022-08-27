from abc import abstractmethod

from daft.logical.logical_plan import LogicalPlan
from daft.runners.partitioning import PartitionSet


class Runner:
    @abstractmethod
    def run(self, plan: LogicalPlan) -> PartitionSet:
        ...
