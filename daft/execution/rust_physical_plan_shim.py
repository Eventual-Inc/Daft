from __future__ import annotations

from typing import TypeVar

from daft.daft import PyExpr
from daft.execution import execution_step, physical_plan
from daft.expressions import Expression
from daft.resource_request import ResourceRequest

PartitionT = TypeVar("PartitionT")


def local_aggregate(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    agg_exprs: list[PyExpr],
    group_by: list[PyExpr],
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:

    aggregation_step = execution_step.Aggregate(
        to_agg=[Expression._from_pyexpr(pyexpr) for pyexpr in agg_exprs],
        group_by=group_by,
    )

    return physical_plan.pipeline_instruction(
        child_plan=input,
        pipeable_instruction=aggregation_step,
        resource_request=ResourceRequest(),  # TODO use real resource request
    )
