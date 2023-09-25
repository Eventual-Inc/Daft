from __future__ import annotations

from daft.execution import physical_plan, physical_plan_factory
from daft.logical import logical_plan
from daft.planner.planner import PartitionT, PhysicalPlanScheduler


class PyPhysicalPlanScheduler(PhysicalPlanScheduler):
    def __init__(self, plan: logical_plan.LogicalPlan):
        self._plan = plan

    def to_partition_tasks(
        self, psets: dict[str, list[PartitionT]], is_ray_runner: bool
    ) -> physical_plan.MaterializedPhysicalPlan:
        return physical_plan.materialize(physical_plan_factory._get_physical_plan(self._plan, psets))
