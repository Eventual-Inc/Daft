from __future__ import annotations

import contextlib
import logging
import threading
import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

from daft import runners
from daft.daft import IOConfig, PyDaftContext, PyDaftExecutionConfig, PyDaftPlanningConfig
from daft.daft import get_context as _get_context

if TYPE_CHECKING:
    from collections.abc import Generator

    from daft.daft import PyQueryMetadata
    from daft.runners.partitioning import PartitionT
    from daft.runners.runner import Runner
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

    def get_or_infer_runner_type(self) -> str:
        """DEPRECATED: Use daft.get_or_infer_runner_type instead. This method will be removed in v0.7.0.

        Get or infer the runner type.

        This API will get or infer the currently used runner type according to the following strategies:
        1. If the `runner` has been set, return its type directly;
        2. Try to determine whether it's currently running on a ray cluster. If so, consider it to be a ray type;
        3. Try to determine based on `DAFT_RUNNER` env variable.

        :return: runner type string ("native" or "ray")
        """
        warnings.warn(
            "This method is deprecated and will be removed in v0.7.0. Use daft.get_or_infer_runner_type instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return runners.get_or_infer_runner_type()

    def get_or_create_runner(self) -> Runner[PartitionT]:
        warnings.warn(
            "This method is deprecated and will be removed in v0.7.0. Use daft.get_or_create_runner instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return runners.get_or_create_runner()

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

    def _notify_query_end(self, query_id: str) -> None:
        self._ctx.notify_query_end(query_id)

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


def set_runner_ray(
    address: str | None = None,
    noop_if_initialized: bool = False,
    max_task_backlog: int | None = None,
    force_client_mode: bool = False,
) -> DaftContext:
    """DEPRECATED: Use daft.set_runner_ray instead. This method will be removed in v0.7.0.

    Configure Daft to execute dataframes using the Ray distributed computing framework.

    Args:
        address: Ray cluster address to connect to. If None, connects to or starts a local Ray instance.
        noop_if_initialized: If True, skip initialization if Ray is already running.
        max_task_backlog: Maximum number of tasks that can be queued. None means Daft will automatically determine a good default.
        force_client_mode: If True, forces Ray to run in client mode.

    Returns:
        DaftContext: Updated Daft execution context configured for Ray.

    Note:
        Can also be configured via environment variable: DAFT_RUNNER=ray
    """
    warnings.warn(
        "This method is deprecated and will be removed in v0.7.0. Use daft.set_runner_ray instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    _ = runners.set_runner_ray(
        address=address,
        noop_if_initialized=noop_if_initialized,
        max_task_backlog=max_task_backlog,
        force_client_mode=force_client_mode,
    )

    return DaftContext._from_native(_get_context())


def set_runner_native(num_threads: int | None = None) -> DaftContext:
    """DEPRECATED: Use daft.set_runner_native instead. This method will be removed in v0.7.0.

    Configure Daft to execute dataframes using native multi-threaded processing.

    This is the default execution mode for Daft.

    Returns:
        DaftContext: Updated Daft execution context configured for native execution.

    Note:
        Can also be configured via environment variable: DAFT_RUNNER=native
    """
    warnings.warn(
        "This method is deprecated and will be removed in v0.7.0. Use daft.set_runner_native instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    _ = runners.set_runner_native(num_threads=num_threads)
    return DaftContext._from_native(_get_context())


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
        default_io_config: A default IOConfig to use in the absence of one being explicitly passed into any Expression (e.g. `.url.download()`)
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
    enable_aqe: bool | None = None,
    default_morsel_size: int | None = None,
    shuffle_algorithm: str | None = None,
    pre_shuffle_merge_threshold: int | None = None,
    flight_shuffle_dirs: list[str] | None = None,
    enable_ray_tracing: bool | None = None,
    scantask_splitting_level: int | None = None,
    scantask_max_parallel: int | None = None,
    native_parquet_writer: bool | None = None,
    use_legacy_ray_runner: bool | None = None,
    min_cpu_per_task: float | None = None,
    actor_udf_ready_timeout: int | None = None,
    maintain_order: bool | None = None,
) -> DaftContext:
    """Globally sets various configuration parameters which control various aspects of Daft execution.

    These configuration values
    are used when a Dataframe is executed (e.g. calls to `DataFrame.write_*`, [DataFrame.collect()](https://docs.daft.ai/en/stable/api/dataframe/#daft.DataFrame.collect) or [DataFrame.show()](https://docs.daft.ai/en/stable/api/dataframe/#daft.DataFrame.select)).

    Args:
        config: A PyDaftExecutionConfig object to set the config to, before applying other kwargs. Defaults to None which indicates
            that the old (current) config should be used.
        scan_tasks_min_size_bytes: Minimum size in bytes when merging ScanTasks when reading files from storage.
            Increasing this value will make Daft perform more merging of files into a single partition before yielding,
            which leads to bigger but fewer partitions. (Defaults to 96 MiB)
        scan_tasks_max_size_bytes: Maximum size in bytes when merging ScanTasks when reading files from storage.
            Increasing this value will increase the upper bound of the size of merged ScanTasks, which leads to bigger but
            fewer partitions. (Defaults to 384 MiB)
        max_sources_per_scan_task: Maximum number of sources in a single ScanTask. (Defaults to 10)
        broadcast_join_size_bytes_threshold: If one side of a join is smaller than this threshold, a broadcast join will be used.
            Default is 10 MiB.
        parquet_split_row_groups_max_files: Maximum number of files to read in which the row group splitting should happen. (Defaults to 10)
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
        enable_aqe: Enables Adaptive Query Execution, Defaults to False
        default_morsel_size: Default size of morsels used for the new local executor. Defaults to 131072 rows.
        shuffle_algorithm: The shuffle algorithm to use. Defaults to "auto", which will let Daft determine the algorithm. Options are "map_reduce" and "pre_shuffle_merge".
        pre_shuffle_merge_threshold: Memory threshold in bytes for pre-shuffle merge. Defaults to 1GB
        flight_shuffle_dirs: The directories to use for flight shuffle. Defaults to ["/tmp"].
        enable_ray_tracing: Enable tracing for Ray. Accessible in `/tmp/ray/session_latest/logs/daft` after the run completes. Defaults to False.
        scantask_splitting_level: How aggressively to split scan tasks. Setting this to `2` will use a more aggressive ScanTask splitting algorithm which might be more expensive to run but results in more even splits of partitions. Defaults to 1.
        scantask_max_parallel: Set the max parallelism for running scan tasks simultaneously. Currently, this only works for Native Runner. If set to 0, all available CPUs will be used. Defaults to 8.
        native_parquet_writer: Whether to use the native parquet writer vs the pyarrow parquet writer. Defaults to `True`.
        use_legacy_ray_runner: Whether to use the legacy ray runner. Defaults to `False`.
        min_cpu_per_task: Minimum CPU per task in the Ray runner. Defaults to 0.5.
        actor_udf_ready_timeout: Timeout for UDF actors to be ready. Defaults to 120 seconds.
        maintain_order: Whether to maintain order during execution. Defaults to True. Some blocking sink operators (e.g. write_parquet) won't respect this flag and will always keep maintain_order as false, and propagate to child operators. It's useful to set this to False for running df.collect() when no ordering is required.
    """
    # Replace values in the DaftExecutionConfig with user-specified overrides
    ctx = get_context()
    with ctx._lock:
        old_daft_execution_config = ctx._ctx._daft_execution_config if config is None else config

        new_daft_execution_config = old_daft_execution_config.with_config_values(
            scan_tasks_min_size_bytes=scan_tasks_min_size_bytes,
            scan_tasks_max_size_bytes=scan_tasks_max_size_bytes,
            max_sources_per_scan_task=max_sources_per_scan_task,
            broadcast_join_size_bytes_threshold=broadcast_join_size_bytes_threshold,
            parquet_split_row_groups_max_files=parquet_split_row_groups_max_files,
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
            enable_aqe=enable_aqe,
            default_morsel_size=default_morsel_size,
            shuffle_algorithm=shuffle_algorithm,
            flight_shuffle_dirs=flight_shuffle_dirs,
            pre_shuffle_merge_threshold=pre_shuffle_merge_threshold,
            enable_ray_tracing=enable_ray_tracing,
            scantask_splitting_level=scantask_splitting_level,
            scantask_max_parallel=scantask_max_parallel,
            native_parquet_writer=native_parquet_writer,
            use_legacy_ray_runner=use_legacy_ray_runner,
            min_cpu_per_task=min_cpu_per_task,
            actor_udf_ready_timeout=actor_udf_ready_timeout,
            maintain_order=maintain_order,
        )

        ctx._ctx._daft_execution_config = new_daft_execution_config
        return ctx
