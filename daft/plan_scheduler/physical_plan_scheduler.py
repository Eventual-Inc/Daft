from __future__ import annotations

from daft.daft import PhysicalPlanScheduler as _PhysicalPlanScheduler
from daft.execution import physical_plan
from daft.runners.partitioning import PartitionT


class PhysicalPlanScheduler:
    """
    Generates executable tasks for an underlying physical plan.
    """

    def __init__(self, scheduler: _PhysicalPlanScheduler):
        self._scheduler = scheduler

    def num_partitions(self) -> int:
        return self._scheduler.num_partitions()

    def to_partition_tasks(
        self, psets: dict[str, list[PartitionT]], is_ray_runner: bool
    ) -> physical_plan.MaterializedPhysicalPlan:
        return physical_plan.materialize(self._scheduler.to_partition_tasks(psets, is_ray_runner))
