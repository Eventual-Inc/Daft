from __future__ import annotations

from abc import ABC, abstractmethod

from daft.execution import physical_plan
from daft.runners.partitioning import PartitionT


class QueryPlanner(ABC):
    """
    An interface for translating a logical plan to a physical plan, and generating
    executable tasks for the latter.
    """

    @abstractmethod
    def plan(self, psets: dict[str, list[PartitionT]]) -> physical_plan.MaterializedPhysicalPlan:
        pass
