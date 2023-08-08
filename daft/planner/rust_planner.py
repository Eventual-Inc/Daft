from __future__ import annotations

from daft.daft import LogicalPlanBuilder as _LogicalPlanBuilder
from daft.execution import physical_plan
from daft.planner.planner import PartitionT, QueryPlanner


class RustQueryPlanner(QueryPlanner):
    def __init__(self, builder: _LogicalPlanBuilder):
        self._builder = builder

    def plan(self, psets: dict[str, list[PartitionT]]) -> physical_plan.MaterializedPhysicalPlan:
        # TODO(Clark): Integrate partition set cache.
        return physical_plan.materialize(self._builder.to_partition_tasks())
