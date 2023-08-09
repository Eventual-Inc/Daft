from __future__ import annotations

from typing import Iterator, TypeVar, cast

from daft.context import get_context
from daft.daft import FileFormatConfig, PyExpr, PySchema, PyTable
from daft.execution import execution_step, physical_plan
from daft.expressions import Expression, ExpressionsProjection
from daft.logical.schema import Schema
from daft.resource_request import ResourceRequest
from daft.table import Table

PartitionT = TypeVar("PartitionT")


def local_aggregate(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    agg_exprs: list[PyExpr],
    group_by: list[PyExpr],
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    aggregation_step = execution_step.Aggregate(
        to_agg=[Expression._from_pyexpr(pyexpr) for pyexpr in agg_exprs],
        group_by=ExpressionsProjection([Expression._from_pyexpr(pyexpr) for pyexpr in group_by]),
    )

    return physical_plan.pipeline_instruction(
        child_plan=input,
        pipeable_instruction=aggregation_step,
        resource_request=ResourceRequest(),  # TODO use real resource request
    )


def tabular_scan(
    schema: PySchema, file_info_table: PyTable, file_format_config: FileFormatConfig, limit: int
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    parts = cast(Iterator[PartitionT], [Table._from_pytable(file_info_table)])
    file_info_iter = physical_plan.partition_read(iter(parts))
    filepaths_column_name = get_context().runner().runner_io().FS_LISTING_PATH_COLUMN_NAME
    return physical_plan.file_read(
        file_info_iter, limit, Schema._from_pyschema(schema), None, None, file_format_config, filepaths_column_name
    )
