from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, cast

from daft.daft import (
    FileFormat,
    FileFormatConfig,
    IOConfig,
    JoinType,
    PyExpr,
    PySchema,
    PyTable,
    ResourceRequest,
    ScanTask,
    StorageConfig,
)
from daft.execution import execution_step, physical_plan
from daft.expressions import Expression, ExpressionsProjection
from daft.logical.map_partition_ops import MapPartitionOp
from daft.logical.schema import Schema
from daft.runners.partitioning import PartialPartitionMetadata, PartitionT
from daft.table import MicroPartition


def scan_with_tasks(
    scan_tasks: list[ScanTask],
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    """child_plan represents partitions with filenames.

    Yield a plan to read those filenames.
    """
    # TODO(Clark): Currently hardcoded to have 1 file per instruction
    # We can instead right-size and bundle the ScanTask into single-instruction bulk reads.
    for scan_task in scan_tasks:
        scan_step = execution_step.PartitionTaskBuilder[PartitionT](inputs=[], partial_metadatas=None,).add_instruction(
            instruction=ScanWithTask(scan_task),
            # Set the filesize as the memory request.
            # (Note: this is very conservative; file readers empirically use much more peak memory than 1x file size.)
            resource_request=ResourceRequest(memory_bytes=scan_task.size_bytes()),
        )
        yield scan_step


def empty_scan(
    schema: Schema,
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    """yield a plan to create an empty Partition"""
    scan_step = execution_step.PartitionTaskBuilder[PartitionT](inputs=[], partial_metadatas=None,).add_instruction(
        instruction=EmptyScan(schema=schema),
        resource_request=ResourceRequest(memory_bytes=0),
    )
    yield scan_step


@dataclass(frozen=True)
class ScanWithTask(execution_step.SingleOutputInstruction):
    scan_task: ScanTask

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._scan(inputs)

    def _scan(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        assert len(inputs) == 0
        table = MicroPartition._from_scan_task(self.scan_task)
        return [table]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        assert len(input_metadatas) == 0

        return [
            PartialPartitionMetadata(
                num_rows=self.scan_task.num_rows(),
                size_bytes=self.scan_task.size_bytes(),
            )
        ]


@dataclass(frozen=True)
class EmptyScan(execution_step.SingleOutputInstruction):
    schema: Schema

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return [MicroPartition.empty(self.schema)]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        assert len(input_metadatas) == 0

        return [
            PartialPartitionMetadata(
                num_rows=0,
                size_bytes=0,
            )
        ]


def tabular_scan(
    schema: PySchema,
    columns_to_read: list[str] | None,
    file_info_table: PyTable,
    file_format_config: FileFormatConfig,
    storage_config: StorageConfig,
    limit: int,
    is_ray_runner: bool,
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    # TODO(Clark): Fix this Ray runner hack.
    part = MicroPartition._from_pytable(file_info_table)
    if is_ray_runner:
        import ray

        parts = [ray.put(part)]
    else:
        parts = [part]
    parts_t = cast(Iterator[PartitionT], parts)

    file_info_iter = physical_plan.partition_read(iter(parts_t))
    return physical_plan.file_read(
        child_plan=file_info_iter,
        limit_rows=limit,
        schema=Schema._from_pyschema(schema),
        storage_config=storage_config,
        columns_to_read=columns_to_read,
        file_format_config=file_format_config,
    )


def project(
    input: physical_plan.InProgressPhysicalPlan[PartitionT], projection: list[PyExpr], resource_request: ResourceRequest
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    expr_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in projection])
    return physical_plan.pipeline_instruction(
        child_plan=input,
        pipeable_instruction=execution_step.Project(expr_projection),
        resource_request=resource_request,
    )


class ShimExplodeOp(MapPartitionOp):
    explode_columns: ExpressionsProjection

    def __init__(self, explode_columns: ExpressionsProjection) -> None:
        self.explode_columns = explode_columns

    def get_output_schema(self) -> Schema:
        raise NotImplementedError("Output schema shouldn't be needed at execution time")

    def run(self, input_partition: MicroPartition) -> MicroPartition:
        return input_partition.explode(self.explode_columns)


def explode(
    input: physical_plan.InProgressPhysicalPlan[PartitionT], explode_exprs: list[PyExpr]
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    explode_expr_projection = ExpressionsProjection(
        [Expression._from_pyexpr(expr)._explode() for expr in explode_exprs]
    )
    explode_op = ShimExplodeOp(explode_expr_projection)
    return physical_plan.pipeline_instruction(
        child_plan=input,
        pipeable_instruction=execution_step.MapPartition(explode_op),
        resource_request=ResourceRequest(),
    )


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
        resource_request=ResourceRequest(),
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
        ResourceRequest(),
    )


def reduce_merge(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    reduce_instruction = execution_step.ReduceMerge()
    return physical_plan.reduce(input, reduce_instruction)


def hash_join(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    right: physical_plan.InProgressPhysicalPlan[PartitionT],
    left_on: list[PyExpr],
    right_on: list[PyExpr],
    join_type: JoinType,
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    left_on_expr_proj = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in left_on])
    right_on_expr_proj = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in right_on])
    return physical_plan.hash_join(
        left_plan=input,
        right_plan=right,
        left_on=left_on_expr_proj,
        right_on=right_on_expr_proj,
        how=join_type,
    )


def broadcast_join(
    broadcaster: physical_plan.InProgressPhysicalPlan[PartitionT],
    receiver: physical_plan.InProgressPhysicalPlan[PartitionT],
    left_on: list[PyExpr],
    right_on: list[PyExpr],
    join_type: JoinType,
    is_swapped: bool,
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    left_on_expr_proj = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in left_on])
    right_on_expr_proj = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in right_on])
    return physical_plan.broadcast_join(
        broadcaster_plan=broadcaster,
        receiver_plan=receiver,
        left_on=left_on_expr_proj,
        right_on=right_on_expr_proj,
        how=join_type,
        is_swapped=is_swapped,
    )


def write_file(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    file_format: FileFormat,
    schema: PySchema,
    root_dir: str,
    compression: str | None,
    partition_cols: list[PyExpr] | None,
    io_config: IOConfig | None,
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    if partition_cols is not None:
        expr_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in partition_cols])
    else:
        expr_projection = None
    return physical_plan.file_write(
        input,
        file_format,
        Schema._from_pyschema(schema),
        root_dir,
        compression,
        expr_projection,
        io_config,
    )
