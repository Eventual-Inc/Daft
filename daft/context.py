from __future__ import annotations

import dataclasses
import logging
import os
import warnings
from typing import TYPE_CHECKING, ClassVar

from daft.daft import IOConfig, PyDaftExecutionConfig, PyDaftPlanningConfig

if TYPE_CHECKING:
    from daft.runners.runner import Runner

logger = logging.getLogger(__name__)

import threading


class _RunnerConfig:
    name = ClassVar[str]


@dataclasses.dataclass(frozen=True)
class _PyRunnerConfig(_RunnerConfig):
    name = "py"
    use_thread_pool: bool | None


@dataclasses.dataclass(frozen=True)
class _RayRunnerConfig(_RunnerConfig):
    name = "ray"
    address: str | None
    max_task_backlog: int | None


def _get_runner_config_from_env() -> _RunnerConfig:
    """Retrieves the appropriate RunnerConfig from environment variables

    To use:

    1. PyRunner: set DAFT_RUNNER=py
    2. RayRunner: set DAFT_RUNNER=ray and optionally RAY_ADDRESS=ray://...
    """
    runner_from_envvar = os.getenv("DAFT_RUNNER")
    task_backlog_env = os.getenv("DAFT_DEVELOPER_RAY_MAX_TASK_BACKLOG")
    use_thread_pool_env = os.getenv("DAFT_DEVELOPER_USE_THREAD_POOL")
    use_thread_pool = bool(int(use_thread_pool_env)) if use_thread_pool_env is not None else None

    ray_is_initialized = False
    in_ray_worker = False
    try:
        import ray

        if ray.is_initialized():
            ray_is_initialized = True
            # Check if running inside a Ray worker
            if ray._private.worker.global_worker.mode == ray.WORKER_MODE:
                in_ray_worker = True
    except ImportError:
        pass

    # Retrieve the runner from environment variables
    if runner_from_envvar and runner_from_envvar.upper() == "RAY":
        ray_address = os.getenv("DAFT_RAY_ADDRESS")
        if ray_address is not None:
            warnings.warn(
                "Detected usage of the $DAFT_RAY_ADDRESS environment variable. This will be deprecated, please use $RAY_ADDRESS instead."
            )
        else:
            ray_address = os.getenv("RAY_ADDRESS")
        return _RayRunnerConfig(
            address=ray_address,
            max_task_backlog=int(task_backlog_env) if task_backlog_env else None,
        )
    elif runner_from_envvar and runner_from_envvar.upper() == "PY":
        return _PyRunnerConfig(use_thread_pool=use_thread_pool)
    elif runner_from_envvar is not None:
        raise ValueError(f"Unsupported DAFT_RUNNER variable: {runner_from_envvar}")

    # Retrieve the runner from current initialized Ray environment, only if not running in a Ray worker
    elif ray_is_initialized and not in_ray_worker:
        return _RayRunnerConfig(
            address=None,  # No address supplied, use the existing connection
            max_task_backlog=int(task_backlog_env) if task_backlog_env else None,
        )

    # Fall back on PyRunner
    else:
        return _PyRunnerConfig(use_thread_pool=use_thread_pool)


@dataclasses.dataclass
class DaftContext:
    """Global context for the current Daft execution environment"""

    # When a dataframe is executed, this config is copied into the Runner
    # which then keeps track of a per-unique-execution-ID copy of the config, using it consistently throughout the execution
    _daft_execution_config: PyDaftExecutionConfig = PyDaftExecutionConfig.from_env()

    # Non-execution calls (e.g. creation of a dataframe, logical plan building etc) directly reference values in this config
    _daft_planning_config: PyDaftPlanningConfig = PyDaftPlanningConfig()

    _runner_config: _RunnerConfig | None = None
    _disallow_set_runner: bool = False
    _runner: Runner | None = None

    _instance: ClassVar[DaftContext | None] = None
    _lock: ClassVar[threading.Lock] = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                # Another thread could have created the instance
                # before we acquired the lock. So check that the
                # instance is still nonexistent.
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def runner(self) -> Runner:
        with self._lock:
            return self._get_runner()

    @property
    def daft_execution_config(self) -> PyDaftExecutionConfig:
        with self._lock:
            return self._daft_execution_config

    @property
    def daft_planning_config(self) -> PyDaftPlanningConfig:
        with self._lock:
            return self._daft_planning_config

    @property
    def runner_config(self) -> _RunnerConfig:
        with self._lock:
            return self._get_runner_config()

    def _get_runner_config(self) -> _RunnerConfig:
        if self._runner_config is not None:
            return self._runner_config
        self._runner_config = _get_runner_config_from_env()
        return self._runner_config

    def _get_runner(self) -> Runner:
        if self._runner is not None:
            return self._runner

        runner_config = self._get_runner_config()
        if runner_config.name == "ray":
            from daft.runners.ray_runner import RayRunner

            assert isinstance(self._runner_config, _RayRunnerConfig)
            self._runner = RayRunner(
                address=self._runner_config.address,
                max_task_backlog=self._runner_config.max_task_backlog,
            )
        elif runner_config.name == "py":
            from daft.runners.pyrunner import PyRunner

            assert isinstance(self._runner_config, _PyRunnerConfig)
            self._runner = PyRunner(use_thread_pool=self._runner_config.use_thread_pool)

        else:
            raise NotImplementedError(f"Runner config not implemented: {runner_config.name}")

        # Mark DaftContext as having the runner set, which prevents any subsequent setting of the config
        # after the runner has been initialized once
        self._disallow_set_runner = True

        return self._runner

    @property
    def is_ray_runner(self) -> bool:
        with self._lock:
            return isinstance(self._runner_config, _RayRunnerConfig)


_DaftContext = DaftContext()


def get_context() -> DaftContext:
    return _DaftContext


def set_runner_ray(
    address: str | None = None,
    noop_if_initialized: bool = False,
    max_task_backlog: int | None = None,
) -> DaftContext:
    """Set the runner for executing Daft dataframes to a Ray cluster

    Alternatively, users can set this behavior via environment variables:

    1. DAFT_RUNNER=ray
    2. Optionally, RAY_ADDRESS=ray://...

    **This function will throw an error if called multiple times in the same process.**

    Args:
        address: Address to head node of the Ray cluster. Defaults to None.
        noop_if_initialized: If set to True, only the first call to this function will have any effect in setting the Runner.
            Subsequent calls will have no effect at all. Defaults to False, which throws an error if this function is called
            more than once per process.

    Returns:
        DaftContext: Daft context after setting the Ray runner
    """

    ctx = get_context()
    with ctx._lock:
        if ctx._disallow_set_runner:
            if noop_if_initialized:
                warnings.warn(
                    "Calling daft.context.set_runner_ray(noop_if_initialized=True) multiple times has no effect beyond the first call."
                )
                return ctx
            raise RuntimeError("Cannot set runner more than once")

        ctx._runner_config = _RayRunnerConfig(
            address=address,
            max_task_backlog=max_task_backlog,
        )
        ctx._disallow_set_runner = True
        return ctx


def set_runner_py(use_thread_pool: bool | None = None) -> DaftContext:
    """Set the runner for executing Daft dataframes to your local Python interpreter - this is the default behavior.

    Alternatively, users can set this behavior via an environment variable: DAFT_RUNNER=py

    Returns:
        DaftContext: Daft context after setting the Py runner
    """
    ctx = get_context()
    with ctx._lock:
        if ctx._disallow_set_runner:
            raise RuntimeError("Cannot set runner more than once")

        ctx._runner_config = _PyRunnerConfig(use_thread_pool=use_thread_pool)
        ctx._disallow_set_runner = True
        return ctx


def set_planning_config(
    config: PyDaftPlanningConfig | None = None,
    default_io_config: IOConfig | None = None,
) -> DaftContext:
    """Globally sets various configuration parameters which control Daft plan construction behavior. These configuration values
    are used when a Dataframe is being constructed (e.g. calls to create a Dataframe, or to build on an existing Dataframe)

    Args:
        config: A PyDaftPlanningConfig object to set the config to, before applying other kwargs. Defaults to None which indicates
            that the old (current) config should be used.
        default_io_config: A default IOConfig to use in the absence of one being explicitly passed into any Expression (e.g. `.url.download()`)
            or Dataframe operation (e.g. `daft.read_parquet()`).
    """
    # Replace values in the DaftPlanningConfig with user-specified overrides
    ctx = get_context()
    with ctx._lock:
        old_daft_planning_config = ctx._daft_planning_config if config is None else config
        new_daft_planning_config = old_daft_planning_config.with_config_values(
            default_io_config=default_io_config,
        )

        ctx._daft_planning_config = new_daft_planning_config
        return ctx


def set_execution_config(
    config: PyDaftExecutionConfig | None = None,
    scan_tasks_min_size_bytes: int | None = None,
    scan_tasks_max_size_bytes: int | None = None,
    broadcast_join_size_bytes_threshold: int | None = None,
    parquet_split_row_groups_max_files: int | None = None,
    sort_merge_join_sort_with_aligned_boundaries: bool | None = None,
    sample_size_for_sort: int | None = None,
    num_preview_rows: int | None = None,
    parquet_target_filesize: int | None = None,
    parquet_target_row_group_size: int | None = None,
    parquet_inflation_factor: float | None = None,
    csv_target_filesize: int | None = None,
    csv_inflation_factor: float | None = None,
    shuffle_aggregation_default_partitions: int | None = None,
    read_sql_partition_size_bytes: int | None = None,
    enable_aqe: bool | None = None,
    enable_native_executor: bool | None = None,
) -> DaftContext:
    """Globally sets various configuration parameters which control various aspects of Daft execution. These configuration values
    are used when a Dataframe is executed (e.g. calls to `.write_*`, `.collect()` or `.show()`)

    Args:
        config: A PyDaftExecutionConfig object to set the config to, before applying other kwargs. Defaults to None which indicates
            that the old (current) config should be used.
        scan_tasks_min_size_bytes: Minimum size in bytes when merging ScanTasks when reading files from storage.
            Increasing this value will make Daft perform more merging of files into a single partition before yielding,
            which leads to bigger but fewer partitions. (Defaults to 96 MiB)
        scan_tasks_max_size_bytes: Maximum size in bytes when merging ScanTasks when reading files from storage.
            Increasing this value will increase the upper bound of the size of merged ScanTasks, which leads to bigger but
            fewer partitions. (Defaults to 384 MiB)
        broadcast_join_size_bytes_threshold: If one side of a join is smaller than this threshold, a broadcast join will be used.
            Default is 10 MiB.
        parquet_split_row_groups_max_files: Maximum number of files to read in which the row group splitting should happen. (Defaults to 10)
        sort_merge_join_sort_with_aligned_boundaries: Whether to use a specialized algorithm for sorting both sides of a
            sort-merge join such that they have aligned boundaries. This can lead to a faster merge-join at the cost of
            more skewed sorted join inputs, increasing the risk of OOMs.
        sample_size_for_sort: number of elements to sample from each partition when running sort,
            Default is 20.
        num_preview_rows: number of rows to when showing a dataframe preview,
            Default is 8.
        parquet_target_filesize: Target File Size when writing out Parquet Files. Defaults to 512MB
        parquet_target_row_group_size: Target Row Group Size when writing out Parquet Files. Defaults to 128MB
        parquet_inflation_factor: Inflation Factor of parquet files (In-Memory-Size / File-Size) ratio. Defaults to 3.0
        csv_target_filesize: Target File Size when writing out CSV Files. Defaults to 512MB
        csv_inflation_factor: Inflation Factor of CSV files (In-Memory-Size / File-Size) ratio. Defaults to 0.5
        shuffle_aggregation_default_partitions: Minimum number of partitions to create when performing aggregations. Defaults to 200, unless the number of input partitions is less than 200.
        read_sql_partition_size_bytes: Target size of partition when reading from SQL databases. Defaults to 512MB
        enable_aqe: Enables Adaptive Query Execution, Defaults to False
        enable_native_executor: Enables new local executor. Defaults to False
    """
    # Replace values in the DaftExecutionConfig with user-specified overrides
    ctx = get_context()
    with ctx._lock:
        old_daft_execution_config = ctx._daft_execution_config if config is None else config

        new_daft_execution_config = old_daft_execution_config.with_config_values(
            scan_tasks_min_size_bytes=scan_tasks_min_size_bytes,
            scan_tasks_max_size_bytes=scan_tasks_max_size_bytes,
            broadcast_join_size_bytes_threshold=broadcast_join_size_bytes_threshold,
            parquet_split_row_groups_max_files=parquet_split_row_groups_max_files,
            sort_merge_join_sort_with_aligned_boundaries=sort_merge_join_sort_with_aligned_boundaries,
            sample_size_for_sort=sample_size_for_sort,
            num_preview_rows=num_preview_rows,
            parquet_target_filesize=parquet_target_filesize,
            parquet_target_row_group_size=parquet_target_row_group_size,
            parquet_inflation_factor=parquet_inflation_factor,
            csv_target_filesize=csv_target_filesize,
            csv_inflation_factor=csv_inflation_factor,
            shuffle_aggregation_default_partitions=shuffle_aggregation_default_partitions,
            read_sql_partition_size_bytes=read_sql_partition_size_bytes,
            enable_aqe=enable_aqe,
            enable_native_executor=enable_native_executor,
        )

        ctx._daft_execution_config = new_daft_execution_config
        return ctx
