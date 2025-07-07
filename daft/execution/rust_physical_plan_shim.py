from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.context import get_context
from daft.daft import (
    FileFormat,
    IOConfig,
    JoinType,
    PyExpr,
    PySchema,
    ResourceRequest,
    ScanTask,
    WriteMode,
)
from daft.execution import execution_step, physical_plan
from daft.expressions import Expression, ExpressionsProjection
from daft.logical.map_partition_ops import MapPartitionOp
from daft.logical.schema import Schema
from daft.runners.partitioning import PartitionT

if TYPE_CHECKING:
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.table import TableProperties as IcebergTableProperties

    from daft.io import DataSink
    from daft.recordbatch import MicroPartition


def scan_with_tasks(
    scan_tasks: list[ScanTask],
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    """child_plan represents partitions with filenames.

    Yield a plan to read those filenames.
    """
    # TODO(Clark): Currently hardcoded to have 1 file per instruction
    # We can instead right-size and bundle the ScanTask into single-instruction bulk reads.

    cfg = get_context().daft_execution_config

    for scan_task in scan_tasks:
        scan_step = execution_step.PartitionTaskBuilder[PartitionT](
            inputs=[],
            partial_metadatas=None,
        ).add_instruction(
            instruction=execution_step.ScanWithTask(scan_task),
            # Set the estimated in-memory size as the memory request.
            resource_request=ResourceRequest(memory_bytes=scan_task.estimate_in_memory_size_bytes(cfg)),
        )
        yield scan_step


def empty_scan(
    schema: Schema,
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    """Yield a plan to create an empty Partition."""
    scan_step = execution_step.PartitionTaskBuilder[PartitionT](
        inputs=[],
        partial_metadatas=None,
    ).add_instruction(
        instruction=execution_step.EmptyScan(schema=schema),
        resource_request=ResourceRequest(memory_bytes=0),
    )
    yield scan_step


def project(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    projection: list[PyExpr],
    resource_request: ResourceRequest | None,
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    expr_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in projection])
    resource_request = (
        ResourceRequest(num_cpus=1, num_gpus=None, memory_bytes=None) if resource_request is None else resource_request
    )
    return physical_plan.pipeline_instruction(
        child_plan=input,
        pipeable_instruction=execution_step.Project(expr_projection),
        resource_request=resource_request,
    )


def actor_pool_project(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    projection: list[PyExpr],
    actor_pool_manager: physical_plan.ActorPoolManager,
    resource_request: ResourceRequest | None,
    num_actors: int,
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    expr_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in projection])
    resource_request = (
        ResourceRequest(num_cpus=1, num_gpus=None, memory_bytes=None) if resource_request is None else resource_request
    )
    return physical_plan.actor_pool_project(
        child_plan=input,
        projection=expr_projection,
        actor_pool_manager=actor_pool_manager,
        resource_request=resource_request,
        num_actors=num_actors,
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


def unpivot(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    ids: list[PyExpr],
    values: list[PyExpr],
    variable_name: str,
    value_name: str,
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    ids_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in ids])
    values_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in values])

    unpivot_step = execution_step.Unpivot(
        ids=ids_projection,
        values=values_projection,
        variable_name=variable_name,
        value_name=value_name,
    )

    return physical_plan.pipeline_instruction(
        child_plan=input,
        pipeable_instruction=unpivot_step,
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


def local_dedup(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    columns: list[PyExpr],
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    dedup_step = execution_step.Dedup(
        columns=ExpressionsProjection([Expression._from_pyexpr(pyexpr) for pyexpr in columns]),
    )
    return physical_plan.pipeline_instruction(
        child_plan=input,
        pipeable_instruction=dedup_step,
        resource_request=ResourceRequest(),
    )


def pivot(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    group_by: list[PyExpr],
    pivot_col: PyExpr,
    value_col: PyExpr,
    names: list[str],
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    pivot_step = execution_step.Pivot(
        group_by=ExpressionsProjection([Expression._from_pyexpr(pyexpr) for pyexpr in group_by]),
        pivot_col=Expression._from_pyexpr(pivot_col),
        value_col=Expression._from_pyexpr(value_col),
        names=names,
    )

    return physical_plan.pipeline_instruction(
        child_plan=input,
        pipeable_instruction=pivot_step,
        resource_request=ResourceRequest(),
    )


def sample(
    input: physical_plan.InProgressPhysicalPlan[PartitionT], fraction: float, with_replacement: bool, seed: int | None
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    return physical_plan.pipeline_instruction(
        child_plan=input,
        pipeable_instruction=execution_step.Sample(fraction=fraction, with_replacement=with_replacement, seed=seed),
        resource_request=ResourceRequest(),
    )


def sort(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    sort_by: list[PyExpr],
    descending: list[bool],
    nulls_first: list[bool],
    num_partitions: int,
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    expr_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in sort_by])
    return physical_plan.sort(
        child_plan=input,
        sort_by=expr_projection,
        descending=descending,
        nulls_first=nulls_first,
        num_partitions=num_partitions,
    )


def top_n(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    sort_by: list[PyExpr],
    descending: list[bool],
    nulls_first: list[bool],
    limit: int,
    num_partitions: int,
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    expr_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in sort_by])
    return physical_plan.top_n(
        child_plan=input,
        sort_by=expr_projection,
        descending=descending,
        nulls_first=nulls_first,
        limit=limit,
        num_partitions=num_partitions,
    )


def fanout_by_hash(
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
    null_equals_nulls: list[bool] | None,
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
        null_equals_nulls=null_equals_nulls,
    )


def merge_join_sorted(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    right: physical_plan.InProgressPhysicalPlan[PartitionT],
    left_on: list[PyExpr],
    right_on: list[PyExpr],
    join_type: JoinType,
    left_is_larger: bool,
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    left_on_expr_proj = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in left_on])
    right_on_expr_proj = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in right_on])
    return physical_plan.merge_join_sorted(
        left_plan=input,
        right_plan=right,
        left_on=left_on_expr_proj,
        right_on=right_on_expr_proj,
        how=join_type,
        left_is_larger=left_is_larger,
    )


def sort_merge_join_aligned_boundaries(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    right: physical_plan.InProgressPhysicalPlan[PartitionT],
    left_on: list[PyExpr],
    right_on: list[PyExpr],
    join_type: JoinType,
    num_partitions: int,
    left_is_larger: bool,
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    left_on_expr_proj = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in left_on])
    right_on_expr_proj = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in right_on])
    return physical_plan.sort_merge_join_aligned_boundaries(
        left_plan=input,
        right_plan=right,
        left_on=left_on_expr_proj,
        right_on=right_on_expr_proj,
        how=join_type,
        num_partitions=num_partitions,
        left_is_larger=left_is_larger,
    )


def broadcast_join(
    broadcaster: physical_plan.InProgressPhysicalPlan[PartitionT],
    receiver: physical_plan.InProgressPhysicalPlan[PartitionT],
    left_on: list[PyExpr],
    right_on: list[PyExpr],
    null_equals_nulls: list[bool] | None,
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
        null_equals_nulls=null_equals_nulls,
    )


def write_file(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    write_mode: WriteMode,
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
        write_mode,
        file_format,
        Schema._from_pyschema(schema),
        root_dir,
        compression,
        expr_projection,
        io_config,
    )


def write_iceberg(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    base_path: str,
    iceberg_schema: IcebergSchema,
    iceberg_properties: IcebergTableProperties,
    partition_spec_id: int,
    partition_cols: list[PyExpr],
    io_config: IOConfig | None,
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    return physical_plan.iceberg_write(
        input,
        base_path=base_path,
        iceberg_schema=iceberg_schema,
        iceberg_properties=iceberg_properties,
        partition_spec_id=partition_spec_id,
        partition_cols=ExpressionsProjection([Expression._from_pyexpr(expr) for expr in partition_cols]),
        io_config=io_config,
    )


def write_deltalake(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    path: str,
    large_dtypes: bool,
    version: int,
    partition_cols: list[PyExpr] | None,
    io_config: IOConfig | None,
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    return physical_plan.deltalake_write(
        input,
        path,
        large_dtypes,
        version,
        ExpressionsProjection([Expression._from_pyexpr(expr) for expr in partition_cols]) if partition_cols else None,
        io_config,
    )


def write_lance(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    path: str,
    mode: str,
    io_config: IOConfig | None,
    kwargs: dict[str, Any] | None,
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    return physical_plan.lance_write(input, path, mode, io_config, kwargs)


def write_data_sink(
    input: physical_plan.InProgressPhysicalPlan[PartitionT],
    sink: DataSink[Any],
) -> physical_plan.InProgressPhysicalPlan[PartitionT]:
    return physical_plan.data_sink_write(input, sink)
