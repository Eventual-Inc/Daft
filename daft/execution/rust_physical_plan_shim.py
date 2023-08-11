from __future__ import annotations

from typing import Iterator, TypeVar, cast

from daft.context import get_context
from daft.daft import FileFormat, FileFormatConfig, PyExpr, PySchema, PyTable
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


def project(
    input: physical_plan.InProgressPhysicalPlan[PartitionT], projection: list[PyExpr]
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    expr_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in projection])
    return physical_plan.pipeline_instruction(
        child_plan=input,
        pipeable_instruction=execution_step.Project(expr_projection),
        resource_request=ResourceRequest(),  # TODO(Clark): Use real ResourceRequest.
    )


def sort(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    sort_by: list[PyExpr],
    descending: list[bool],
    num_partitions: int,
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    expr_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in sort_by])
    return physical_plan.sort(
        child_plan=input,
        sort_by=expr_projection,
        descending=descending,
        num_partitions=num_partitions,
    )


def split_by_hash(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    num_partitions: int,
    partition_by: list[PyExpr],
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    expr_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in partition_by])
    fanout_instruction = execution_step.FanoutHash(
        _num_outputs=num_partitions,
        partition_by=expr_projection,
    )
    return physical_plan.pipeline_instruction(
        input,
        fanout_instruction,
        ResourceRequest(),  # TODO(Clark): Propagate resource request.
    )


def reduce_merge(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    reduce_instruction = execution_step.ReduceMerge()
    return physical_plan.reduce(input, reduce_instruction)


def write_file(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    file_format: FileFormat,
    schema: PySchema,
    root_dir: str,
    compression: str | None,
    partition_cols: list[PyExpr] | None,
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    if partition_cols is not None:
        expr_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in partition_cols])
    else:
        expr_projection = None
    return physical_plan.file_write(
        input, file_format, Schema._from_pyschema(schema), root_dir, compression, expr_projection
    )
