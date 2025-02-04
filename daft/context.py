from __future__ import annotations

import contextlib
import dataclasses
import logging
from typing import TYPE_CHECKING, ClassVar

from daft.daft import IOConfig, PyDaftContext, PyDaftExecutionConfig, PyDaftPlanningConfig, PyRunner
from daft.daft import set_runner_native as _set_runner_native
from daft.daft import set_runner_ray as _set_runner_ray

__all__ = [
    "DaftContext",
    "execution_config_ctx",
    "get_context",
    "planning_config_ctx",
    "set_execution_config",
    "set_planning_config",
    "set_runner_native",
    "set_runner_py",
    "set_runner_ray",
]

if TYPE_CHECKING:
    from daft.runners.runner import Runner

logger = logging.getLogger(__name__)

import threading


@dataclasses.dataclass
class DaftContext:
    """Global context for the current Daft execution environment."""

    _ctx: PyDaftContext

    # When a dataframe is executed, this config is copied into the Runner
    # which then keeps track of a per-unique-execution-ID copy of the config, using it consistently throughout the execution
    _daft_execution_config: PyDaftExecutionConfig = PyDaftExecutionConfig.from_env()

    # Non-execution calls (e.g. creation of a dataframe, logical plan building etc) directly reference values in this config
    _daft_planning_config: PyDaftPlanningConfig = PyDaftPlanningConfig.from_env()

    _runner: Runner | None = None

    _lock: ClassVar[threading.Lock] = threading.Lock()

    def _from_native(ctx: PyDaftContext) -> DaftContext:
        return DaftContext(ctx=ctx)

    def __init__(self, ctx: PyDaftContext | None = None):
        if ctx is not None:
            self._ctx = ctx
        else:
            self._ctx = PyDaftContext()

    def get_or_create_runner(self) -> PyRunner:
        return self._ctx.get_or_create_runner()

    @property
    def daft_execution_config(self) -> PyDaftExecutionConfig:
        return self._ctx.daft_execution_config

    @property
    def daft_planning_config(self) -> PyDaftPlanningConfig:
        return self._ctx.daft_planning_config


_DaftContext = DaftContext()


def get_context() -> DaftContext:
    return _DaftContext


def set_runner_ray(
    address: str | None = None,
    noop_if_initialized: bool = False,
    max_task_backlog: int | None = None,
    force_client_mode: bool = False,
) -> DaftContext:
    py_ctx = _set_runner_ray(
        address=address,
        max_task_backlog=max_task_backlog,
        force_client_mode=force_client_mode,
    )
    return DaftContext._from_native(py_ctx)


def set_runner_py(use_thread_pool: bool | None = None) -> DaftContext:
    """Set the runner for executing Daft dataframes to your local Python interpreter - this is the default behavior.

    Alternatively, users can set this behavior via an environment variable: DAFT_RUNNER=py

    Returns:
        DaftContext: Daft context after setting the Py runner
    """
    ctx = get_context()
    with ctx._lock:
        if ctx._runner is not None and ctx._runner.name not in {"py", "native"}:
            raise RuntimeError("Cannot set runner more than once")

        from daft.runners.pyrunner import PyRunner

        ctx._runner = PyRunner(use_thread_pool=use_thread_pool)
        return ctx


def set_runner_native() -> DaftContext:
    """Set the runner for executing Daft dataframes to the native runner.

    Alternatively, users can set this behavior via an environment variable: DAFT_RUNNER=native

    Returns:
        DaftContext: Daft context after setting the native runner
    """
    py_ctx = _set_runner_native()
    return DaftContext._from_native(py_ctx)


@contextlib.contextmanager
def planning_config_ctx(**kwargs):
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
        old_daft_planning_config = ctx.daft_planning_config if config is None else config
        new_daft_planning_config = old_daft_planning_config.with_config_values(
            default_io_config=default_io_config,
        )

        ctx.daft_planning_config = new_daft_planning_config
        return ctx


@contextlib.contextmanager
def execution_config_ctx(**kwargs):
    """Context manager that wraps set_execution_config to reset the config to its original setting afternwards."""
    original_config = get_context().daft_execution_config
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
    enable_native_executor: bool | None = None,
    default_morsel_size: int | None = None,
    shuffle_algorithm: str | None = None,
    pre_shuffle_merge_threshold: int | None = None,
    enable_ray_tracing: bool | None = None,
    scantask_splitting_level: int | None = None,
) -> DaftContext:
    """Globally sets various configuration parameters which control various aspects of Daft execution.

    These configuration values
    are used when a Dataframe is executed (e.g. calls to `.write_*`, `.collect()` or `.show()`).

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
        enable_native_executor: Enables the native executor, Defaults to False
        default_morsel_size: Default size of morsels used for the new local executor. Defaults to 131072 rows.
        shuffle_algorithm: The shuffle algorithm to use. Defaults to "auto", which will let Daft determine the algorithm. Options are "map_reduce" and "pre_shuffle_merge".
        pre_shuffle_merge_threshold: Memory threshold in bytes for pre-shuffle merge. Defaults to 1GB
        enable_ray_tracing: Enable tracing for Ray. Accessible in `/tmp/ray/session_latest/logs/daft` after the run completes. Defaults to False.
        scantask_splitting_level: How aggressively to split scan tasks. Setting this to `2` will use a more aggressive ScanTask splitting algorithm which might be more expensive to run but results in more even splits of partitions. Defaults to 1.
    """
    # Replace values in the DaftExecutionConfig with user-specified overrides
    ctx = get_context()
    with ctx._lock:
        old_daft_execution_config = ctx.daft_execution_config if config is None else config

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
            enable_native_executor=enable_native_executor,
            default_morsel_size=default_morsel_size,
            shuffle_algorithm=shuffle_algorithm,
            pre_shuffle_merge_threshold=pre_shuffle_merge_threshold,
            enable_ray_tracing=enable_ray_tracing,
            scantask_splitting_level=scantask_splitting_level,
        )

        ctx.daft_execution_config = new_daft_execution_config
        return ctx
