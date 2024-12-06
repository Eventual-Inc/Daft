"""This module contains utilities and wrappers that instrument tracing over our RayRunner's task scheduling + execution.

These utilities are meant to provide light wrappers on top of Ray functionality (e.g. remote functions, actors, ray.get/ray.wait)
which allow us to intercept these calls and perform the necessary actions for tracing the interaction between Daft and Ray.
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
import logging
import pathlib
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Iterator, TextIO

try:
    import ray
except ImportError:
    raise

from daft.execution.execution_step import PartitionTask
from daft.runners import ray_metrics

if TYPE_CHECKING:
    from daft import ResourceRequest
    from daft.daft import PyDaftExecutionConfig
    from daft.execution.physical_plan import MaterializedPhysicalPlan

logger = logging.getLogger(__name__)


# We add the trace by default to the latest session logs of the Ray Runner
# This lets us access our logs via the Ray dashboard when running Ray jobs
def get_log_location() -> pathlib.Path:
    return pathlib.Path("/tmp") / "ray" / "session_latest" / "logs"


def get_daft_trace_location(log_location: pathlib.Path) -> pathlib.Path:
    return log_location / "daft"


FILE_WRITE_BUFFERING_DEFAULT = 1024 * 1024  # 1MB


@contextlib.contextmanager
def ray_tracer(execution_id: str, daft_execution_config: PyDaftExecutionConfig) -> Iterator[RunnerTracer]:
    """Instantiates a RunnerTracer for the duration of the code block."""
    # Dump the RayRunner trace if we detect an active Ray session, otherwise we give up and do not write the trace
    ray_logs_location = get_log_location()
    filepath: pathlib.Path | None
    if ray_logs_location.exists() and daft_execution_config.enable_ray_tracing:
        trace_filename = (
            f"trace_RayRunner.{execution_id}.{datetime.replace(datetime.now(), microsecond=0).isoformat()[:-3]}.json"
        )
        daft_trace_location = get_daft_trace_location(ray_logs_location)
        daft_trace_location.mkdir(exist_ok=True, parents=True)
        filepath = daft_trace_location / trace_filename
    else:
        filepath = None

    if filepath is not None:
        # Get the metrics actor and block until ready
        metrics_actor = ray_metrics.get_metrics_actor(execution_id)
        metrics_actor.wait()

        with open(filepath, "w", buffering=FILE_WRITE_BUFFERING_DEFAULT) as f:
            # Yield the tracer
            runner_tracer = RunnerTracer(f, metrics_actor)

            try:
                yield runner_tracer

            # Use `finally` to cleanup and finalize the JSON even in cases where the job is killed with SIGKILL
            finally:
                runner_tracer.finalize()
    else:
        runner_tracer = RunnerTracer(None, None)
        yield runner_tracer


@dataclasses.dataclass
class TraceWriter:
    """Handles writing trace events to a JSON file in Chrome Trace Event Format."""

    file: TextIO | None
    start: float
    has_written_event: bool = False

    def write_header(self) -> None:
        """Initialize the JSON file with an opening bracket."""
        if self.file is not None:
            self.file.write("[")

    def write_metadata(self, event: dict[str, Any]) -> None:
        """Write a metadata event to the trace file.

        Args:
            event: The metadata event to write
        """
        if self.file is not None:
            if self.has_written_event:
                self.file.write(",\n")
            json.dump(event, self.file)
            self.has_written_event = True

    def write_event(self, event: dict[str, Any], ts: int | None = None) -> int:
        """Write a single trace event to the file.

        Args:
            event: The event data to write
            ts: Optional timestamp override. If None, current time will be used

        Returns:
            The timestamp that was used for the event
        """
        if self.file is not None:
            if ts is None:
                ts = int((time.time() - self.start) * 1000 * 1000)
            if self.has_written_event:
                self.file.write(",\n")
            json.dump(
                {
                    **event,
                    "ts": ts,
                },
                self.file,
            )
            self.has_written_event = True
        return ts or -1

    def write_footer(self) -> None:
        if self.file is not None:
            # Close with a closing square bracket
            self.file.write("]")


class RunnerTracer:
    # IDs and names for the visualized data
    SCHEDULER_PID = 1
    STAGES_PID = 2
    NODE_PIDS_START = 100

    # Event Phases with human-readable const names
    PHASE_METADATA = "M"
    PHASE_DURATION_BEGIN = "B"
    PHASE_DURATION_END = "E"
    PHASE_INSTANT = "i"
    PHASE_ASYNC_BEGIN = "b"
    PHASE_ASYNC_END = "e"
    PHASE_ASYNC_INSTANT = "n"
    PHASE_FLOW_START = "s"
    PHASE_FLOW_FINISH = "f"

    def __init__(self, file: TextIO | None, metrics_actor: ray_metrics.MetricsActorHandle | None):
        start = time.time()
        self._start = start
        self._stage_start_end: dict[int, tuple[int, int]] = {}
        self._task_id_to_location: dict[str, tuple[int, int]] = {}
        self._task_event_idx = 0

        self._metrics_actor = metrics_actor

        self._writer = TraceWriter(file, start)
        self._writer.write_header()

    def _write_event(self, event: dict[str, Any], ts: int | None = None) -> int:
        try:
            return self._writer.write_event(event, ts)
        except (json.JSONDecodeError, TypeError) as e:
            logger.exception("Failed to serialize event to JSON: %s", e)
            return ts or -1
        except OSError as e:
            logger.exception("Failed to write trace event to file: %s", e)
            return ts or -1

    def _write_metadata(self, metadata_event: dict[str, Any]) -> None:
        try:
            return self._writer.write_metadata(metadata_event)
        except (json.JSONDecodeError, TypeError) as e:
            logger.exception("Failed to serialize metadata to JSON: %s", e)
            return
        except OSError as e:
            logger.exception("Failed to write trace metadata to file: %s", e)
            return

    def _get_trace_timestamp(self, task_event_start: float) -> int:
        return int((task_event_start - self._start) * 1000 * 1000)

    def _flush_task_metrics(self):
        if self._metrics_actor is not None:
            task_events, new_idx = self._metrics_actor.get_task_events(self._task_event_idx)
            self._task_event_idx = new_idx

            for task_event in task_events:
                if isinstance(task_event, ray_metrics.StartTaskEvent):
                    # Record which pid/tid to associate this task_id with
                    self._task_id_to_location[task_event.task_id] = (task_event.node_idx, task_event.worker_idx)

                    start_ts = self._get_trace_timestamp(task_event.start)
                    # Write to the Async view (will nest under the task creation and dispatch)
                    self._write_event(
                        {
                            "id": task_event.task_id,
                            "category": "task",
                            "name": "task_remote_execution",
                            "ph": RunnerTracer.PHASE_ASYNC_BEGIN,
                            "pid": 1,
                            "tid": 2,
                            "args": {
                                "ray_assigned_resources": task_event.ray_assigned_resources,
                                "ray_task_id": task_event.ray_task_id,
                                "stage_id": task_event.stage_id,
                            },
                        },
                        ts=start_ts,
                    )

                    # Write to the node view (group by node ID)
                    self._write_event(
                        {
                            "name": "task_remote_execution",
                            "ph": RunnerTracer.PHASE_DURATION_BEGIN,
                            "pid": task_event.node_idx + RunnerTracer.NODE_PIDS_START,
                            "tid": task_event.worker_idx,
                            "args": {
                                "ray_assigned_resources": task_event.ray_assigned_resources,
                                "ray_task_id": task_event.ray_task_id,
                                "stage_id": task_event.stage_id,
                            },
                        },
                        ts=start_ts,
                    )
                    self._write_event(
                        {
                            "name": "stage-to-node-flow",
                            "id": task_event.stage_id,
                            "ph": RunnerTracer.PHASE_FLOW_FINISH,
                            "bp": "e",  # enclosed, since the stage "encloses" the execution
                            "pid": task_event.node_idx + RunnerTracer.NODE_PIDS_START,
                            "tid": task_event.worker_idx,
                        },
                        ts=start_ts,
                    )
                elif isinstance(task_event, ray_metrics.EndTaskEvent):
                    end_ts = self._get_trace_timestamp(task_event.end)

                    # Write to the Async view (will group by the stage ID)
                    self._write_event(
                        {
                            "id": task_event.task_id,
                            "category": "task",
                            "name": "task_remote_execution",
                            "ph": RunnerTracer.PHASE_ASYNC_END,
                            "pid": 1,
                            "tid": 2,
                        },
                        ts=end_ts,
                    )

                    # Write to the node view (group by node ID)
                    node_idx, worker_idx = self._task_id_to_location.get(task_event.task_id, (None, None))
                    if node_idx is None or worker_idx is None:
                        logger.error(
                            "Tracing received a EndTaskEvent without a corresponding StartTaskEvent that provides location information."
                        )
                    else:
                        self._write_event(
                            {
                                "name": "task_remote_execution",
                                "ph": RunnerTracer.PHASE_DURATION_END,
                                "pid": node_idx + RunnerTracer.NODE_PIDS_START,
                                "tid": worker_idx,
                            },
                            ts=end_ts,
                        )
                else:
                    raise NotImplementedError(f"Unhandled TaskEvent: {task_event}")

    def finalize(self) -> None:
        # Retrieve final task metrics
        self._flush_task_metrics()

        # Retrieve metrics from the metrics actor (blocking call)
        node_metrics = self._metrics_actor.collect_and_close() if self._metrics_actor is not None else {}

        # Write out labels for the nodes and other traced events
        nodes_to_pid_mapping = {node_id: i + RunnerTracer.NODE_PIDS_START for i, node_id in enumerate(node_metrics)}
        nodes_workers_to_tid_mapping = {
            (node_id, worker_id): (pid, tid)
            for node_id, pid in nodes_to_pid_mapping.items()
            for tid, worker_id in enumerate(node_metrics[node_id])
        }
        self._write_process_and_thread_names(
            [(pid, f"Node {node_id}") for node_id, pid in nodes_to_pid_mapping.items()],
            [(pid, tid, f"Worker {worker_id}") for (_, worker_id), (pid, tid) in nodes_workers_to_tid_mapping.items()],
        )

        # Write out stages now that everything is completed
        self._write_stages()

        # End the file with the appropriate footer
        self._writer.write_footer()

    def _write_process_and_thread_names(
        self,
        process_meta: list[tuple[int, str]],
        thread_meta: list[tuple[int, int, str]],
    ):
        """Writes metadata for the file.

        Args:
            process_meta: Pass in custom names for PIDs as a list of (pid, name).
            thread_meta: Pass in custom names for threads a a list of (pid, tid, name).
        """
        for pid, name in [
            (RunnerTracer.SCHEDULER_PID, "Scheduler"),
            (RunnerTracer.STAGES_PID, "Stages"),
        ] + process_meta:
            self._write_metadata(
                {
                    "name": "process_name",
                    "ph": RunnerTracer.PHASE_METADATA,
                    "pid": pid,
                    "args": {"name": name},
                }
            )

        for pid, tid, name in [
            (RunnerTracer.SCHEDULER_PID, 1, "_run_plan dispatch loop"),
        ] + thread_meta:
            self._write_metadata(
                {
                    "name": "thread_name",
                    "ph": RunnerTracer.PHASE_METADATA,
                    "pid": pid,
                    "tid": tid,
                    "args": {"name": name},
                }
            )

    def _write_stages(self):
        for stage_id in self._stage_start_end:
            start_ts, end_ts = self._stage_start_end[stage_id]
            self._write_event(
                {
                    "name": f"stage-{stage_id}",
                    "ph": RunnerTracer.PHASE_DURATION_BEGIN,
                    "pid": 2,
                    "tid": stage_id,
                },
                ts=start_ts,
            )

            # Add a flow view here to point to the nodes
            self._write_event(
                {
                    "name": "stage-to-node-flow",
                    "id": stage_id,
                    "ph": RunnerTracer.PHASE_FLOW_START,
                    "pid": 2,
                    "tid": stage_id,
                },
                ts=start_ts,
            )

            self._write_event(
                {
                    "name": f"stage-{stage_id}",
                    "ph": RunnerTracer.PHASE_DURATION_END,
                    "pid": 2,
                    "tid": stage_id,
                },
                ts=end_ts,
            )

    ###
    # Tracing of scheduler dispatching
    ###

    @contextlib.contextmanager
    def dispatch_wave(self, wave_num: int):
        self._write_event(
            {
                "name": f"wave-{wave_num}",
                "pid": 1,
                "tid": 1,
                "ph": RunnerTracer.PHASE_DURATION_BEGIN,
                "args": {"wave_num": wave_num},
            }
        )

        metrics = {}

        def metrics_updater(**kwargs):
            metrics.update(kwargs)

        yield metrics_updater

        self._write_event(
            {
                "name": f"wave-{wave_num}",
                "pid": 1,
                "tid": 1,
                "ph": RunnerTracer.PHASE_DURATION_END,
                "args": metrics,
            }
        )

        # On the end of every wave, perform a flush of the latest metrics from the MetricsActor
        self._flush_task_metrics()

    def count_inflight_tasks(self, count: int):
        self._write_event(
            {
                "name": "dispatch_metrics",
                "ph": "C",
                "pid": 1,
                "tid": 1,
                "args": {"num_inflight_tasks": count},
            }
        )

    @contextlib.contextmanager
    def dispatch_batching(self):
        self._write_event(
            {
                "name": "dispatch_batching",
                "pid": 1,
                "tid": 1,
                "ph": RunnerTracer.PHASE_DURATION_BEGIN,
            }
        )
        yield
        self._write_event(
            {
                "name": "dispatch_batching",
                "pid": 1,
                "tid": 1,
                "ph": RunnerTracer.PHASE_DURATION_END,
            }
        )

    @contextlib.contextmanager
    def dispatching(self):
        self._write_event(
            {
                "name": "dispatching",
                "pid": 1,
                "tid": 1,
                "ph": RunnerTracer.PHASE_DURATION_BEGIN,
            }
        )
        yield
        self._write_event(
            {
                "name": "dispatching",
                "pid": 1,
                "tid": 1,
                "ph": RunnerTracer.PHASE_DURATION_END,
            }
        )

    @contextlib.contextmanager
    def awaiting(self, waiting_for_num_results: int, wait_timeout_s: float | None):
        name = f"awaiting {waiting_for_num_results} (timeout={wait_timeout_s})"
        self._write_event(
            {
                "name": name,
                "pid": 1,
                "tid": 1,
                "ph": RunnerTracer.PHASE_DURATION_BEGIN,
                "args": {
                    "waiting_for_num_results": waiting_for_num_results,
                    "wait_timeout_s": str(wait_timeout_s),
                },
            }
        )
        yield
        self._write_event(
            {
                "name": name,
                "pid": 1,
                "tid": 1,
                "ph": RunnerTracer.PHASE_DURATION_END,
            }
        )

    @contextlib.contextmanager
    def get_next_physical_plan(self):
        self._write_event(
            {
                "name": "next(tasks)",
                "pid": 1,
                "tid": 1,
                "ph": RunnerTracer.PHASE_DURATION_BEGIN,
            }
        )

        args = {}

        def update_args(**kwargs):
            args.update(kwargs)

        yield update_args

        self._write_event(
            {
                "name": "next(tasks)",
                "pid": 1,
                "tid": 1,
                "ph": RunnerTracer.PHASE_DURATION_END,
                "args": args,
            }
        )

    def task_created(self, task_id: str, stage_id: int, resource_request: ResourceRequest, instructions: str):
        created_ts = self._write_event(
            {
                "id": task_id,
                "category": "task",
                "name": f"task_execution.stage-{stage_id}",
                "ph": RunnerTracer.PHASE_ASYNC_BEGIN,
                "args": {
                    "task_id": task_id,
                    "resource_request": {
                        "num_cpus": resource_request.num_cpus,
                        "num_gpus": resource_request.num_gpus,
                        "memory_bytes": resource_request.memory_bytes,
                    },
                    "stage_id": stage_id,
                    "instructions": instructions,
                },
                "pid": 1,
                "tid": 1,
            }
        )

        if stage_id not in self._stage_start_end:
            self._stage_start_end[stage_id] = (created_ts, created_ts)

    def task_dispatched(self, task_id: str):
        self._write_event(
            {
                "id": task_id,
                "category": "task",
                "name": "task_dispatch",
                "ph": RunnerTracer.PHASE_ASYNC_BEGIN,
                "pid": 1,
                "tid": 1,
            }
        )

    def task_received_as_ready(self, task_id: str, stage_id: int):
        self._write_event(
            {
                "id": task_id,
                "category": "task",
                "name": "task_dispatch",
                "ph": RunnerTracer.PHASE_ASYNC_END,
                "pid": 1,
                "tid": 1,
            }
        )
        new_end = self._write_event(
            {
                "id": task_id,
                "category": "task",
                "name": f"task_execution.stage-{stage_id}",
                "ph": RunnerTracer.PHASE_ASYNC_END,
                "pid": 1,
                "tid": 1,
            }
        )

        assert stage_id in self._stage_start_end
        old_start, _ = self._stage_start_end[stage_id]
        self._stage_start_end[stage_id] = (old_start, new_end)


@dataclasses.dataclass(frozen=True)
class _RayFunctionWrapper:
    """Wrapper around a Ray remote function that allows us to intercept calls and record the call for a given task ID."""

    f: ray.remote_function.RemoteFunction

    def with_tracing(self, runner_tracer: RunnerTracer, task: PartitionTask) -> _RayRunnableFunctionWrapper:
        return _RayRunnableFunctionWrapper(f=self.f, runner_tracer=runner_tracer, task=task)

    def options(self, *args, **kwargs) -> _RayFunctionWrapper:
        return dataclasses.replace(self, f=self.f.options(*args, **kwargs))


def ray_remote_traced(f: ray.remote_function.RemoteFunction):
    """Decorates a Ray Remote function to ensure that we can trace it.

    Usage:

    >>> @ray_remote_traced
    >>> @ray.remote
    >>> def f():
    >>>     ...
    >>>
    >>> f.with_tracing(tracer, task).remote(...)
    """
    return _RayFunctionWrapper(f=f)


@dataclasses.dataclass(frozen=True)
class _RayRunnableFunctionWrapper:
    """Runnable variant of RayFunctionWrapper that supports `.remote` calls."""

    f: ray.remote_function.RemoteFunction
    runner_tracer: RunnerTracer
    task: PartitionTask

    def options(self, *args, **kwargs) -> _RayRunnableFunctionWrapper:
        return dataclasses.replace(self, f=self.f.options(*args, **kwargs))

    def remote(self, *args, **kwargs):
        self.runner_tracer.task_dispatched(self.task.id())
        return self.f.remote(*args, **kwargs)


@dataclasses.dataclass(frozen=True)
class MaterializedPhysicalPlanWrapper:
    """Wrapper around MaterializedPhysicalPlan that hooks into tracing capabilities."""

    plan: MaterializedPhysicalPlan
    runner_tracer: RunnerTracer

    def __next__(self):
        with self.runner_tracer.get_next_physical_plan() as update_args:
            item = next(self.plan)

            update_args(
                next_item_type=type(item).__name__,
            )
            if isinstance(item, PartitionTask):
                instructions_description = "-".join(type(i).__name__ for i in item.instructions)
                self.runner_tracer.task_created(
                    item.id(),
                    item.stage_id,
                    item.resource_request,
                    instructions_description,
                )
                update_args(
                    partition_task_instructions=instructions_description,
                )

        return item


@contextlib.contextmanager
def collect_ray_task_metrics(execution_id: str, task_id: str, stage_id: int, execution_config: PyDaftExecutionConfig):
    """Context manager that will ping the metrics actor to record various execution metrics about a given task."""
    if execution_config.enable_ray_tracing:
        import time

        runtime_context = ray.get_runtime_context()

        metrics_actor = ray_metrics.get_metrics_actor(execution_id)
        metrics_actor.mark_task_start(
            task_id,
            time.time(),
            runtime_context.get_node_id(),
            runtime_context.get_worker_id(),
            stage_id,
            runtime_context.get_assigned_resources(),
            runtime_context.get_task_id(),
        )
        yield
        metrics_actor.mark_task_end(task_id, time.time())
    else:
        yield
