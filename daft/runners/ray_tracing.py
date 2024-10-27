"""This module contains utilities and wrappers that instrument tracing over our RayRunner's task scheduling + execution

These utilities are meant to provide light wrappers on top of Ray functionality (e.g. remote functions, actors, ray.get/ray.wait)
which allow us to intercept these calls and perform the necessary actions for tracing the interaction between Daft and Ray.
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
import os
import pathlib
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, TextIO

try:
    import ray
except ImportError:
    raise

from daft.execution.execution_step import PartitionTask
from daft.runners import ray_metrics

if TYPE_CHECKING:
    from daft import ResourceRequest
    from daft.execution.physical_plan import MaterializedPhysicalPlan


# We add the trace by default to the latest session logs of the Ray Runner
# This lets us access our logs via the Ray dashboard when running Ray jobs
DEFAULT_RAY_LOGS_LOCATION = pathlib.Path("/tmp") / "ray" / "session_latest"
DEFAULT_DAFT_TRACE_LOCATION = DEFAULT_RAY_LOGS_LOCATION / "daft"

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


@contextlib.contextmanager
def ray_tracer(execution_id: str):
    metrics_actor = ray_metrics.get_metrics_actor(execution_id)

    # Dump the RayRunner trace if we detect an active Ray session, otherwise we give up and do not write the trace
    filepath: pathlib.Path | None
    if pathlib.Path(DEFAULT_RAY_LOGS_LOCATION).exists():
        trace_filename = (
            f"trace_RayRunner.{execution_id}.{datetime.replace(datetime.now(), microsecond=0).isoformat()[:-3]}.json"
        )
        daft_trace_location = pathlib.Path(DEFAULT_DAFT_TRACE_LOCATION)
        daft_trace_location.mkdir(exist_ok=True, parents=True)
        filepath = DEFAULT_DAFT_TRACE_LOCATION / trace_filename
    else:
        filepath = None

    if filepath is not None:
        with open(filepath, "w") as f:
            # Yield the tracer
            runner_tracer = RunnerTracer(f)
            yield runner_tracer

            # Retrieve metrics from the metrics actor
            metrics = metrics_actor.collect_metrics()

            # Post-processing to obtain a map of {task_id: (node_id, worker_id)}
            task_locations = {metric.task_id: (metric.node_id, metric.worker_id) for metric in metrics}
            nodes_to_workers: dict[str, set[str]] = {}
            nodes = set()
            for node_id, worker_id in task_locations.values():
                nodes.add(node_id)
                if node_id not in nodes_to_workers:
                    nodes_to_workers[node_id] = set()
                nodes_to_workers[node_id].add(worker_id)
            nodes_to_pid_mapping = {node_id: i + NODE_PIDS_START for i, node_id in enumerate(nodes)}
            nodes_workers_to_tid_mapping = {
                node_id: {worker_id: i for i, worker_id in enumerate(worker_ids)}
                for node_id, worker_ids in nodes_to_workers.items()
            }
            parsed_task_node_locations = {
                task_id: (nodes_to_pid_mapping[node_id], nodes_workers_to_tid_mapping[node_id][worker_id])
                for task_id, (node_id, worker_id) in task_locations.items()
            }

            for metric in metrics:
                runner_tracer.write_task_metric(metric, parsed_task_node_locations.get(metric.task_id))

            runner_tracer.write_stages()
            runner_tracer._writer.write_footer(
                [(pid, f"Node {node_id}") for node_id, pid in nodes_to_pid_mapping.items()],
                [
                    (pid, nodes_workers_to_tid_mapping[node_id][worker_id], f"Worker {worker_id}")
                    for node_id, pid in nodes_to_pid_mapping.items()
                    for worker_id in nodes_workers_to_tid_mapping[node_id]
                ],
            )
    else:
        runner_tracer = RunnerTracer(None)
        yield runner_tracer


@dataclasses.dataclass
class TraceWriter:
    """Handles writing trace events to a JSON file in Chrome Trace Event Format"""

    file: TextIO | None
    start: float

    def write_header(self) -> None:
        """Initialize the JSON file with an opening bracket"""
        if self.file is not None:
            self.file.write("[")

    def write_event(self, event: dict[str, Any], ts: int | None = None) -> int:
        """Write a single trace event to the file

        Args:
            event: The event data to write
            ts: Optional timestamp override. If None, current time will be used

        Returns:
            The timestamp that was used for the event
        """
        if ts is None:
            ts = int((time.time() - self.start) * 1000 * 1000)

        if self.file is not None:
            self.file.write(
                json.dumps(
                    {
                        **event,
                        "ts": ts,
                    }
                )
            )
            self.file.write(",\n")
        return ts

    def write_footer(self, process_meta: list[tuple[int, str]], thread_meta: list[tuple[int, int, str]]) -> None:
        """Writes metadata for the file, closing out the file with a footer.

        Args:
            process_meta: Pass in custom names for PIDs as a list of (pid, name).
            thread_meta: Pass in custom names for threads a a list of (pid, tid, name).
        """
        if self.file is not None:
            for pid, name in [
                (SCHEDULER_PID, "Scheduler"),
                (STAGES_PID, "Stages"),
            ] + process_meta:
                self.file.write(
                    json.dumps(
                        {
                            "name": "process_name",
                            "ph": PHASE_METADATA,
                            "pid": pid,
                            "args": {"name": name},
                        }
                    )
                )
                self.file.write(",\n")

            for pid, tid, name in [
                (SCHEDULER_PID, 1, "_run_plan dispatch loop"),
            ] + thread_meta:
                self.file.write(
                    json.dumps(
                        {
                            "name": "thread_name",
                            "ph": PHASE_METADATA,
                            "pid": pid,
                            "tid": tid,
                            "args": {"name": name},
                        }
                    )
                )
                self.file.write(",\n")

            # Remove the trailing comma
            self.file.seek(self.file.tell() - 2, os.SEEK_SET)
            self.file.truncate()

            # Close with a closing square bracket
            self.file.write("]")


class RunnerTracer:
    def __init__(self, file: TextIO | None):
        start = time.time()
        self._start = start
        self._stage_start_end: dict[int, tuple[int, int]] = {}

        self._writer = TraceWriter(file, start)
        self._writer.write_header()

    def _write_event(self, event: dict[str, Any], ts: int | None = None) -> int:
        return self._writer.write_event(event, ts)

    def write_stages(self):
        for stage_id in self._stage_start_end:
            start_ts, end_ts = self._stage_start_end[stage_id]
            self._write_event(
                {
                    "name": f"stage-{stage_id}",
                    "ph": PHASE_DURATION_BEGIN,
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
                    "ph": PHASE_FLOW_START,
                    "pid": 2,
                    "tid": stage_id,
                },
                ts=start_ts,
            )

            self._write_event(
                {
                    "name": f"stage-{stage_id}",
                    "ph": PHASE_DURATION_END,
                    "pid": 2,
                    "tid": stage_id,
                },
                ts=end_ts,
            )

    def write_task_metric(self, metric: ray_metrics.TaskMetric, node_id_worker_id: tuple[int, int] | None):
        # Write to the Async view (will group by the stage ID)
        self._write_event(
            {
                "id": metric.task_id,
                "category": "task",
                "name": "task_remote_execution",
                "ph": PHASE_ASYNC_BEGIN,
                "pid": 1,
                "tid": metric.stage_id,
            },
            ts=int((metric.start - self._start) * 1000 * 1000),
        )
        if metric.end is not None:
            self._write_event(
                {
                    "id": metric.task_id,
                    "category": "task",
                    "name": "task_remote_execution",
                    "ph": PHASE_ASYNC_END,
                    "pid": 1,
                    "tid": metric.stage_id,
                },
                ts=int((metric.end - self._start) * 1000 * 1000),
            )

        # Write to the node/worker view
        if node_id_worker_id is not None:
            pid, tid = node_id_worker_id
            start_ts = int((metric.start - self._start) * 1000 * 1000)
            self._write_event(
                {
                    "name": "task_remote_execution",
                    "ph": PHASE_DURATION_BEGIN,
                    "pid": pid,
                    "tid": tid,
                },
                ts=start_ts,
            )
            self._write_event(
                {
                    "name": "stage-to-node-flow",
                    "id": metric.stage_id,
                    "ph": PHASE_FLOW_FINISH,
                    "bp": PHASE_ASYNC_END,  # enclosed, since the stage "encloses" the execution
                    "pid": pid,
                    "tid": tid,
                },
                ts=start_ts,
            )
            if metric.end is not None:
                self._write_event(
                    {
                        "name": "task_remote_execution",
                        "ph": PHASE_DURATION_END,
                        "pid": pid,
                        "tid": tid,
                    },
                    ts=int((metric.end - self._start) * 1000 * 1000),
                )

    @contextlib.contextmanager
    def dispatch_wave(self, wave_num: int):
        self._write_event(
            {
                "name": f"wave-{wave_num}",
                "pid": 1,
                "tid": 1,
                "ph": PHASE_DURATION_BEGIN,
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
                "ph": PHASE_DURATION_END,
                "args": metrics,
            }
        )

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

    ###
    # Tracing the dispatch batching: when the runner is retrieving enough tasks
    # from the physical plan in order to put them into a batch.
    ###

    @contextlib.contextmanager
    def dispatch_batching(self):
        self._write_event(
            {
                "name": "dispatch_batching",
                "pid": 1,
                "tid": 1,
                "ph": PHASE_DURATION_BEGIN,
            }
        )
        yield
        self._write_event(
            {
                "name": "dispatch_batching",
                "pid": 1,
                "tid": 1,
                "ph": PHASE_DURATION_END,
            }
        )

    ###
    # Tracing the dispatching of tasks
    ###

    @contextlib.contextmanager
    def dispatching(self):
        self._write_event(
            {
                "name": "dispatching",
                "pid": 1,
                "tid": 1,
                "ph": PHASE_DURATION_BEGIN,
            }
        )
        yield
        self._write_event(
            {
                "name": "dispatching",
                "pid": 1,
                "tid": 1,
                "ph": PHASE_DURATION_END,
            }
        )

    ###
    # Tracing the waiting of tasks
    ###

    @contextlib.contextmanager
    def awaiting(self, waiting_for_num_results: int, wait_timeout_s: float | None):
        name = f"awaiting {waiting_for_num_results} (timeout={wait_timeout_s})"
        self._write_event(
            {
                "name": name,
                "pid": 1,
                "tid": 1,
                "ph": PHASE_DURATION_BEGIN,
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
                "ph": PHASE_DURATION_END,
            }
        )

    ###
    # Tracing the PhysicalPlan
    ###

    @contextlib.contextmanager
    def get_next_physical_plan(self):
        self._write_event(
            {
                "name": "next(tasks)",
                "pid": 1,
                "tid": 1,
                "ph": PHASE_DURATION_BEGIN,
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
                "ph": PHASE_DURATION_END,
                "args": args,
            }
        )

    ###
    # Tracing each individual task as an Async Event
    ###

    def task_created(self, task_id: str, stage_id: int, resource_request: ResourceRequest, instructions: str):
        created_ts = self._write_event(
            {
                "id": task_id,
                "category": "task",
                "name": f"task_execution.stage-{stage_id}",
                "ph": PHASE_ASYNC_BEGIN,
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
                "ph": PHASE_ASYNC_BEGIN,
                "pid": 1,
                "tid": 1,
            }
        )

    def task_not_ready(self, task_id: str):
        self._write_event(
            {
                "id": task_id,
                "category": "task",
                "name": "task_awaited_not_ready",
                "ph": PHASE_ASYNC_INSTANT,
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
                "ph": PHASE_ASYNC_END,
                "pid": 1,
                "tid": 1,
            }
        )
        new_end = self._write_event(
            {
                "id": task_id,
                "category": "task",
                "name": f"task_execution.stage-{stage_id}",
                "ph": PHASE_ASYNC_END,
                "pid": 1,
                "tid": 1,
            }
        )

        assert stage_id in self._stage_start_end
        old_start, _ = self._stage_start_end[stage_id]
        self._stage_start_end[stage_id] = (old_start, new_end)


@dataclasses.dataclass(frozen=True)
class RayFunctionWrapper:
    """Wrapper around a Ray remote function that allows us to intercept calls and record the call for a given task ID"""

    f: ray.remote_function.RemoteFunction

    def with_tracing(self, runner_tracer: RunnerTracer, task: PartitionTask) -> RayRunnableFunctionWrapper:
        return RayRunnableFunctionWrapper(f=self.f, runner_tracer=runner_tracer, task=task)

    def options(self, *args, **kwargs) -> RayFunctionWrapper:
        return dataclasses.replace(self, f=self.f.options(*args, **kwargs))

    @classmethod
    def wrap(cls, f: ray.remote_function.RemoteFunction):
        return cls(f=f)


@dataclasses.dataclass(frozen=True)
class RayRunnableFunctionWrapper:
    """Runnable variant of RayFunctionWrapper that supports `.remote` calls"""

    f: ray.remote_function.RemoteFunction
    runner_tracer: RunnerTracer
    task: PartitionTask

    def options(self, *args, **kwargs) -> RayRunnableFunctionWrapper:
        return dataclasses.replace(self, f=self.f.options(*args, **kwargs))

    def remote(self, *args, **kwargs):
        self.runner_tracer.task_dispatched(self.task.id())
        return self.f.remote(*args, **kwargs)


@dataclasses.dataclass(frozen=True)
class MaterializedPhysicalPlanWrapper:
    """Wrapper around MaterializedPhysicalPlan that hooks into tracing capabilities"""

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
def collect_ray_task_metrics(execution_id: str, task_id: str, stage_id: int):
    """Context manager that will ping the metrics actor to record various execution metrics about a given task"""
    import time

    runtime_context = ray.get_runtime_context()

    metrics_actor = ray_metrics.get_metrics_actor(execution_id)
    metrics_actor.mark_task_start(
        task_id, time.time(), runtime_context.get_node_id(), runtime_context.get_worker_id(), stage_id
    )
    yield
    metrics_actor.mark_task_end(task_id, time.time())
