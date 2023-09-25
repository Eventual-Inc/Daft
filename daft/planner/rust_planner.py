from __future__ import annotations

from daft.daft import PhysicalPlanScheduler as _PhysicalPlanScheduler
from daft.execution import physical_plan
from daft.planner.planner import PartitionT, PhysicalPlanScheduler


class RustPhysicalPlanScheduler(PhysicalPlanScheduler):
    def __init__(self, scheduler: _PhysicalPlanScheduler):
        self._scheduler = scheduler

    def to_partition_tasks(
        self, psets: dict[str, list[PartitionT]], is_ray_runner: bool
    ) -> physical_plan.MaterializedPhysicalPlan:
        return physical_plan.materialize(self._scheduler.to_partition_tasks(psets, is_ray_runner))
