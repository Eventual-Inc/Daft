from __future__ import annotations

import contextlib
import dataclasses
import logging
from typing import TYPE_CHECKING, Any, ClassVar

from daft.daft import IOConfig, PyDaftContext, PyDaftExecutionConfig, PyDaftPlanningConfig
from daft.daft import get_context as _get_context
from daft.daft import set_runner_native as _set_runner_native
from daft.daft import set_runner_ray as _set_runner_ray

if TYPE_CHECKING:
    from collections.abc import Generator

    from daft.runners.partitioning import PartitionT
    from daft.runners.runner import Runner

logger = logging.getLogger(__name__)

import threading


@dataclasses.dataclass
class DaftContext:
    """Global context for the current Daft execution environment."""

    _ctx: PyDaftContext

    _lock: ClassVar[threading.Lock] = threading.Lock()

    @property
    def _runner(self) -> Runner[PartitionT]:
        return self._ctx._runner

    @_runner.setter
    def _runner(self, runner: Runner[PartitionT]) -> None:
        self._ctx._runner = runner

    @staticmethod
    def _from_native(ctx: PyDaftContext) -> DaftContext:
        return DaftContext(ctx=ctx)

    def __init__(self, ctx: PyDaftContext | None = None):
        if ctx is not None:
            self._ctx = ctx
        else:
            self._ctx = PyDaftContext()

    def get_or_create_runner(self) -> Runner[PartitionT]:
        return self._ctx.get_or_create_runner()

    @property
    def daft_execution_config(self) -> PyDaftExecutionConfig:
        return self._ctx._daft_execution_config

    @property
    def daft_planning_config(self) -> PyDaftPlanningConfig:
        return self._ctx._daft_planning_config


def get_context() -> DaftContext:
    return DaftContext(_get_context())


def set_runner_ray(
    address: str | None = None,
    noop_if_initialized: bool = False,
    max_task_backlog: int | None = None,
    force_client_mode: bool = False,
) -> DaftContext:
    """Configure Daft to execute dataframes using the Ray distributed computing framework.

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
    py_ctx = _set_runner_ray(
        address=address,
        noop_if_initialized=noop_if_initialized,
        max_task_backlog=max_task_backlog,
        force_client_mode=force_client_mode,
    )

    return DaftContext._from_native(py_ctx)


def set_runner_native(num_threads: int | None = None) -> DaftContext:
    """Configure Daft to execute dataframes using native multi-threaded processing.

    This is the default execution mode for Daft.

    Returns:
        DaftContext: Updated Daft execution context configured for native execution.

    Note:
        Can also be configured via environment variable: DAFT_RUNNER=native
    """
    py_ctx = _set_runner_native(num_threads=num_threads)

    return DaftContext._from_native(py_ctx)


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
            default_io_config=default_io_config,
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
    sort_merge_join_sort_with_aligned_boundaries: bool | None = None,
    hash_join_partition_size_leniency: float | None = None,
    sample_size_for_sort: int | None = None,
    num_preview_rows: int | None = None,
    parquet_target_filesize: int | None = None,
    parquet_target_row_group_size: int | None = None,
    parquet_inflation_factor: float | None = None,
    csv_target_filesize: int | None = None,
    csv_inflation_factor: float | None = None,
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
    native_parquet_writer: bool | None = None,
    use_experimental_distributed_engine: bool | None = None,
    min_cpu_per_task: float | None = None,
) -> DaftContext:
    """Globally sets various configuration parameters which control various aspects of Daft execution.

    These configuration values
    are used when a Dataframe is executed (e.g. calls to `DataFrame.write_*`, [DataFrame.collect()](https://docs.getdaft.io/en/stable/api/dataframe/#daft.DataFrame.collect) or [DataFrame.show()](https://docs.getdaft.io/en/stable/api/dataframe/#daft.DataFrame.select)).

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
        sort_merge_join_sort_with_aligned_boundaries: Whether to use a specialized algorithm for sorting both sides of a
            sort-merge join such that they have aligned boundaries. This can lead to a faster merge-join at the cost of
            more skewed sorted join inputs, increasing the risk of OOMs.
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
        native_parquet_writer: Whether to use the native parquet writer vs the pyarrow parquet writer. Defaults to `True`.
        use_experimental_distributed_engine: Whether to use the experimental distributed engine on the ray runner. Defaults to `True`.
            Note: Not all operations are currently supported, and daft will fallback to the current engine if necessary.
        min_cpu_per_task: Minimum CPU per task in the Ray runner. Defaults to 1.
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
            sort_merge_join_sort_with_aligned_boundaries=sort_merge_join_sort_with_aligned_boundaries,
            hash_join_partition_size_leniency=hash_join_partition_size_leniency,
            sample_size_for_sort=sample_size_for_sort,
            num_preview_rows=num_preview_rows,
            parquet_target_filesize=parquet_target_filesize,
            parquet_target_row_group_size=parquet_target_row_group_size,
            parquet_inflation_factor=parquet_inflation_factor,
            csv_target_filesize=csv_target_filesize,
            csv_inflation_factor=csv_inflation_factor,
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
            native_parquet_writer=native_parquet_writer,
            use_experimental_distributed_engine=use_experimental_distributed_engine,
            min_cpu_per_task=min_cpu_per_task,
        )

        ctx._ctx._daft_execution_config = new_daft_execution_config
        return ctx
