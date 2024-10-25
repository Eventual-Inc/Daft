from __future__ import annotations

import contextlib
import json
import os
import time
from typing import TYPE_CHECKING, Any, TextIO

if TYPE_CHECKING:
    from daft import ResourceRequest


@contextlib.contextmanager
def tracer(filepath: str):
    if int(os.environ.get("DAFT_RUNNER_TRACING", 0)) == 1:
        with open(filepath, "w") as f:
            # Initialize the JSON file
            f.write("[")

            # Yield the tracer
            runner_tracer = RunnerTracer(f)
            yield runner_tracer

            # Add the final touches to the file
            f.write(
                json.dumps({"name": "process_name", "ph": "M", "pid": 1, "args": {"name": "RayRunner dispatch loop"}})
            )
            f.write(",\n")
            f.write(json.dumps({"name": "process_name", "ph": "M", "pid": 2, "args": {"name": "Ray Task Execution"}}))
            f.write("\n]")
    else:
        runner_tracer = RunnerTracer(None)
        yield runner_tracer


class RunnerTracer:
    def __init__(self, file: TextIO | None):
        self._file = file
        self._start = time.time()

    def _write_event(self, event: dict[str, Any]):
        if self._file is not None:
            self._file.write(
                json.dumps(
                    {
                        **event,
                        "ts": int((time.time() - self._start) * 1000 * 1000),
                    }
                )
            )
            self._file.write(",\n")

    @contextlib.contextmanager
    def dispatch_wave(self, wave_num: int):
        self._write_event(
            {
                "name": f"wave-{wave_num}",
                "pid": 1,
                "tid": 1,
                "ph": "B",
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
                "ph": "E",
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
                "ph": "B",
            }
        )
        yield
        self._write_event(
            {
                "name": "dispatch_batching",
                "pid": 1,
                "tid": 1,
                "ph": "E",
            }
        )

    def mark_noop_task_start(self):
        """Marks the start of running a no-op task"""
        self._write_event(
            {
                "name": "no_op_task",
                "pid": 1,
                "tid": 1,
                "ph": "B",
            }
        )

    def mark_noop_task_end(self):
        """Marks the start of running a no-op task"""
        self._write_event(
            {
                "name": "no_op_task",
                "pid": 1,
                "tid": 1,
                "ph": "E",
            }
        )

    def mark_handle_none_task(self):
        """Marks when the underlying physical plan returns None"""
        self._write_event(
            {
                "name": "Physical Plan returned None, needs more progress",
                "ph": "i",
                "pid": 1,
                "tid": 1,
            }
        )

    def mark_handle_materialized_result(self):
        """Marks when the underlying physical plan returns Materialized Result"""
        self._write_event(
            {
                "name": "Physical Plan returned Materialized Result",
                "ph": "i",
                "pid": 1,
                "tid": 1,
            }
        )

    def mark_handle_task_add_to_batch(self):
        """Marks when the underlying physical plan returns a task that we need to add to the dispatch batch"""
        self._write_event(
            {
                "name": "Physical Plan returned Task to add to batch",
                "ph": "i",
                "pid": 1,
                "tid": 1,
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
                "ph": "B",
            }
        )
        yield
        self._write_event(
            {
                "name": "dispatching",
                "pid": 1,
                "tid": 1,
                "ph": "E",
            }
        )

    def mark_dispatch_task(self):
        self._write_event(
            {
                "name": "dispatch_task",
                "ph": "i",
                "pid": 1,
                "tid": 1,
            }
        )

    def mark_dispatch_actor_task(self):
        self._write_event(
            {
                "name": "dispatch_actor_task",
                "ph": "i",
                "pid": 1,
                "tid": 1,
            }
        )

    ###
    # Tracing the waiting of tasks
    ###

    @contextlib.contextmanager
    def awaiting(self):
        self._write_event(
            {
                "name": "awaiting",
                "pid": 1,
                "tid": 1,
                "ph": "B",
            }
        )
        yield
        self._write_event(
            {
                "name": "awaiting",
                "pid": 1,
                "tid": 1,
                "ph": "E",
            }
        )

    ###
    # Tracing each individual task as an Async Event
    ###

    def task_dispatched(self, task_id: str, stage_id: int, resource_request: ResourceRequest, instructions: str):
        self._write_event(
            {
                "id": task_id,
                "category": "task",
                "name": f"stage[{stage_id}]-{instructions}",
                "ph": "b",
                "args": {
                    "resource_request": {
                        "num_cpus": resource_request.num_cpus,
                        "num_gpus": resource_request.num_gpus,
                        "memory_bytes": resource_request.memory_bytes,
                    },
                    "stage_id": stage_id,
                    "instructions": instructions,
                },
                "pid": 2,
                "tid": 1,
            }
        )

    def task_not_ready(self, task_id: str):
        self._write_event(
            {
                "id": task_id,
                "category": "task",
                "name": "task_awaited_not_ready",
                "ph": "n",
                "pid": 2,
                "tid": 1,
            }
        )

    def task_ready(self, task_id: str):
        self._write_event(
            {
                "id": task_id,
                "category": "task",
                "ph": "e",
                "pid": 2,
                "tid": 1,
            }
        )
