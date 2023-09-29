from __future__ import annotations

from abc import ABC, abstractmethod

from daft.execution import physical_plan
from daft.runners.partitioning import PartitionT


class PhysicalPlanScheduler(ABC):
    """
    An interface for generating executable tasks for an underlying physical plan.
    """

    @abstractmethod
    def to_partition_tasks(
        self, psets: dict[str, list[PartitionT]], is_ray_runner: bool
    ) -> physical_plan.MaterializedPhysicalPlan:
        pass
