from __future__ import annotations

import contextlib
import logging
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

from daft.daft import IOConfig, PyDaftContext, PyDaftExecutionConfig, PyDaftPlanningConfig, PyQueryResult
from daft.daft import get_context as _get_context

if TYPE_CHECKING:
    from collections.abc import Generator

    from daft.daft import PyQueryMetadata
    from daft.runners.partitioning import PartitionT
    from daft.subscribers import Subscriber

logger = logging.getLogger(__name__)


@dataclass
class DaftContext:
    """Global context for the current Daft execution environment."""

    _ctx: PyDaftContext

    _lock: ClassVar[threading.Lock] = threading.Lock()

    @staticmethod
    def _from_native(ctx: PyDaftContext) -> DaftContext:
        return DaftContext(ctx=ctx)

    def __init__(self, ctx: PyDaftContext | None = None):
        if ctx is not None:
            self._ctx = ctx
        else:
            self._ctx = PyDaftContext()

    @property
    def daft_execution_config(self) -> PyDaftExecutionConfig:
        return self._ctx._daft_execution_config

    @property
    def daft_planning_config(self) -> PyDaftPlanningConfig:
        return self._ctx._daft_planning_config

    def attach_subscriber(self, alias: str, subscriber: Subscriber) -> None:
        """Attaches a subscriber to this context.

        Subscribers listen to events emitted during runtime, particularly during query execution.
        See the Subscriber class for more details.

        Args:
            alias (str): alias for the subscriber
            subscriber (Subscriber): subscriber instance
        """
        self._ctx.attach_subscriber(alias, subscriber)

    def detach_subscriber(self, alias: str) -> None:
        """Detaches a subscriber from this context.

        Args:
            alias (str): alias for the subscriber
        """
        self._ctx.detach_subscriber(alias)

    def _notify_query_start(self, query_id: str, metadata: PyQueryMetadata) -> None:
        self._ctx.notify_query_start(query_id, metadata)

    def _notify_query_end(self, query_id: str, query_result: PyQueryResult) -> None:
        self._ctx.notify_query_end(query_id, query_result)

    def _notify_optimization_start(self, query_id: str) -> None:
        self._ctx.notify_optimization_start(query_id)

    def _notify_optimization_end(self, query_id: str, optimized_plan: str) -> None:
        self._ctx.notify_optimization_end(query_id, optimized_plan)

    def _notify_result_out(self, query_id: str, result: PartitionT) -> None:
        from daft.recordbatch.micropartition import MicroPartition

        if not isinstance(result, MicroPartition):
            raise ValueError("Query Managers only support the Native Runner for now")
        self._ctx.notify_result_out(query_id, result._micropartition)


def get_context() -> DaftContext:
    """Returns the global singleton daft context."""
    return DaftContext(_get_context())


@contextlib.contextmanager
def planning_config_ctx(**kwargs: Any) -> Generator[None, None, None]:
    """Context manager that wraps set_planning_config to reset the config to its original setting afternwards."""
    original_config = get_context().daft_planning_config
    try:
        set_planning_config(**kwargs)
        yield
    finally:
        set_planning_config(config=original_config)


def set_planning_config(
    config: PyDaftPlanningConfig | None = None,
    default_io_config: IOConfig | None = None,
    enable_strict_filter_pushdown: bool | None = None,
) -> DaftContext:
    """Globally sets various configuration parameters which control Daft plan construction behavior.

    These configuration values are used when a Dataframe is being constructed (e.g. calls to create a Dataframe, or to build on an existing Dataframe).

    Args:
        config: A PyDaftPlanningConfig object to set the config to, before applying other kwargs. Defaults to None which indicates
            that the old (current) config should be used.
        default_io_config: A default IOConfig to use in the absence of one being explicitly passed into any Expression (e.g. `.download()`)
            or Dataframe operation (e.g. `daft.read_parquet()`).
    """
    # Replace values in the DaftPlanningConfig with user-specified overrides
    ctx = get_context()
    with ctx._lock:
        old_daft_planning_config = ctx._ctx._daft_planning_config if config is None else config
        new_daft_planning_config = old_daft_planning_config.with_config_values(
            default_io_config=default_io_config, enable_strict_filter_pushdown=enable_strict_filter_pushdown
        )

        ctx._ctx._daft_planning_config = new_daft_planning_config
        return ctx


@contextlib.contextmanager
def execution_config_ctx(**kwargs: Any) -> Generator[None, None, None]:
    """Context manager that wraps set_execution_config to reset the config to its original setting afternwards."""
    original_config = get_context()._ctx._daft_execution_config
    try:
        set_execution_config(**kwargs)
        yield
    finally:
        set_execution_config(config=original_config)


def set_execution_config(
    config: PyDaftExecutionConfig | None = None,
    enable_scan_task_split_and_merge: bool | None = None,
    scan_tasks_min_size_bytes: int | None = None,
    scan_tasks_max_size_bytes: int | None = None,
    max_sources_per_scan_task: int | None = None,
    broadcast_join_size_bytes_threshold: int | None = None,
    parquet_split_row_groups_max_files: int | None = None,
    hash_join_partition_size_leniency: float | None = None,
    sample_size_for_sort: int | None = None,
    num_preview_rows: int | None = None,
    parquet_target_filesize: int | None = None,
    parquet_target_row_group_size: int | None = None,
    parquet_inflation_factor: float | None = None,
    csv_target_filesize: int | None = None,
    csv_inflation_factor: float | None = None,
    json_inflation_factor: float | None = None,
    shuffle_aggregation_default_partitions: int | None = None,
    partial_aggregation_threshold: int | None = None,
    high_cardinality_aggregation_threshold: float | None = None,
    read_sql_partition_size_bytes: int | None = None,
    default_morsel_size: int | None = None,
    shuffle_algorithm: str | None = None,
    pre_shuffle_merge_threshold: int | None = None,
    scantask_max_parallel: int | None = None,
    native_parquet_writer: bool | None = None,
    min_cpu_per_task: float | None = None,
    actor_udf_ready_timeout: int | None = None,
    maintain_order: bool | None = None,
    enable_dynamic_batching: bool | None = None,
    dynamic_batching_strategy: str | None = None,
) -> DaftContext:
    """Globally sets various configuration parameters which control various aspects of Daft execution.

    These configuration values
    are used when a Dataframe is executed (e.g. calls to `DataFrame.write_*`, [DataFrame.collect()](https://docs.daft.ai/en/stable/api/dataframe/#daft.DataFrame.collect) or [DataFrame.show()](https://docs.daft.ai/en/stable/api/dataframe/#daft.DataFrame.select)).

    Args:
        config: A PyDaftExecutionConfig object to set the config to, before applying other kwargs. Defaults to None which indicates
            that the old (current) config should be used.
        enable_scan_task_split_and_merge: Whether to enable scan task split and merge. Defaults to True.
        scan_tasks_min_size_bytes: Minimum size of scan tasks in bytes. Defaults to 96MB.
        scan_tasks_max_size_bytes: Maximum size of scan tasks in bytes. Defaults to 384MB.
        max_sources_per_scan_task: Maximum number of sources per scan task. Defaults to 10.
        parquet_split_row_groups_max_files: Maximum number of files to read in which the row group splitting should happen. (Defaults to 10)
        broadcast_join_size_bytes_threshold: If one side of a join is smaller than this threshold, a broadcast join will be used.
            Default is 10 MiB.
        hash_join_partition_size_leniency: If the left side of a hash join is already correctly partitioned and the right side isn't,
            and the ratio between the left and right size is at least this value, then the right side is repartitioned to have an equal
            number of partitions as the left. Defaults to 0.5.
        sample_size_for_sort: number of elements to sample from each partition when running sort,
            Default is 20.
        num_preview_rows: number of rows to when showing a dataframe preview,
            Default is 8.
        parquet_target_filesize: Target File Size when writing out Parquet Files. Defaults to 512MB
        parquet_target_row_group_size: Target Row Group Size when writing out Parquet Files. Defaults to 128MB
        parquet_inflation_factor: Inflation Factor of parquet files (In-Memory-Size / File-Size) ratio. Defaults to 3.0
        csv_target_filesize: Target File Size when writing out CSV Files. Defaults to 512MB
        csv_inflation_factor: Inflation Factor of CSV files (In-Memory-Size / File-Size) ratio. Defaults to 0.5
        json_inflation_factor: Inflation Factor of JSON files (In-Memory-Size / File-Size) ratio. Defaults to 0.25
        shuffle_aggregation_default_partitions: Maximum number of partitions to create when performing aggregations on the Ray Runner. Defaults to 200, unless the number of input partitions is less than 200.
        partial_aggregation_threshold: Threshold for performing partial aggregations on the Native Runner. Defaults to 10000 rows.
        high_cardinality_aggregation_threshold: Threshold selectivity for performing high cardinality aggregations on the Native Runner. Defaults to 0.8.
        read_sql_partition_size_bytes: Target size of partition when reading from SQL databases. Defaults to 512MB
        default_morsel_size: Default size of morsels used for the new local executor. Defaults to 131072 rows.
        shuffle_algorithm: The shuffle algorithm to use. Defaults to "auto", which will let Daft determine the algorithm. Options are "map_reduce" and "pre_shuffle_merge".
        pre_shuffle_merge_threshold: Memory threshold in bytes for pre-shuffle merge. Defaults to 1GB
        scantask_max_parallel: Set the max parallelism for running scan tasks simultaneously. Currently, this only works for Native Runner. If set to 0, all available CPUs will be used. Defaults to 8.
        native_parquet_writer: Whether to use the native parquet writer vs the pyarrow parquet writer. Defaults to `True`.
        min_cpu_per_task: Minimum CPU per task in the Ray runner. Defaults to 0.5.
        actor_udf_ready_timeout: Timeout for UDF actors to be ready. Defaults to 120 seconds.
        maintain_order: Whether to maintain order during execution. Defaults to True. Some blocking sink operators (e.g. write_parquet) won't respect this flag and will always keep maintain_order as false, and propagate to child operators. It's useful to set this to False for running df.collect() when no ordering is required.
        enable_dynamic_batching: Whether to enable dynamic batching. Defaults to False.
        dynamic_batching_strategy: The strategy to use for dynamic batching. Defaults to 'auto'.
    """
    # Replace values in the DaftExecutionConfig with user-specified overrides
    ctx = get_context()
    with ctx._lock:
        old_daft_execution_config = ctx._ctx._daft_execution_config if config is None else config

        new_daft_execution_config = old_daft_execution_config.with_config_values(
            enable_scan_task_split_and_merge=enable_scan_task_split_and_merge,
            scan_tasks_min_size_bytes=scan_tasks_min_size_bytes,
            scan_tasks_max_size_bytes=scan_tasks_max_size_bytes,
            max_sources_per_scan_task=max_sources_per_scan_task,
            parquet_split_row_groups_max_files=parquet_split_row_groups_max_files,
            broadcast_join_size_bytes_threshold=broadcast_join_size_bytes_threshold,
            hash_join_partition_size_leniency=hash_join_partition_size_leniency,
            sample_size_for_sort=sample_size_for_sort,
            num_preview_rows=num_preview_rows,
            parquet_target_filesize=parquet_target_filesize,
            parquet_target_row_group_size=parquet_target_row_group_size,
            parquet_inflation_factor=parquet_inflation_factor,
            csv_target_filesize=csv_target_filesize,
            csv_inflation_factor=csv_inflation_factor,
            json_inflation_factor=json_inflation_factor,
            shuffle_aggregation_default_partitions=shuffle_aggregation_default_partitions,
            partial_aggregation_threshold=partial_aggregation_threshold,
            high_cardinality_aggregation_threshold=high_cardinality_aggregation_threshold,
            read_sql_partition_size_bytes=read_sql_partition_size_bytes,
            default_morsel_size=default_morsel_size,
            shuffle_algorithm=shuffle_algorithm,
            pre_shuffle_merge_threshold=pre_shuffle_merge_threshold,
            scantask_max_parallel=scantask_max_parallel,
            native_parquet_writer=native_parquet_writer,
            min_cpu_per_task=min_cpu_per_task,
            actor_udf_ready_timeout=actor_udf_ready_timeout,
            maintain_order=maintain_order,
            enable_dynamic_batching=enable_dynamic_batching,
            dynamic_batching_strategy=dynamic_batching_strategy,
        )

        ctx._ctx._daft_execution_config = new_daft_execution_config
        return ctx
