from __future__ import annotations

import contextlib
import json
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from daft import ResourceRequest


class RunnerTracer:
    def __init__(self, filepath: str):
        self._filepath = filepath

    def __enter__(self) -> RunnerTracer:
        self._file = open(self._filepath, "w")
        self._file.write("[")
        self._start = time.time()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._file.write(
            json.dumps({"name": "process_name", "ph": "M", "pid": 1, "args": {"name": "RayRunner dispatch loop"}})
        )
        self._file.write(",\n")
        self._file.write(
            json.dumps({"name": "process_name", "ph": "M", "pid": 2, "args": {"name": "Ray Task Execution"}})
        )
        self._file.write("\n]")
        self._file.close()

    def _write_event(self, event: dict[str, Any]):
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

    # def dispatch_wave_metrics(self, metrics: dict[str, int]):
    #     """Marks a counter event for various runner counters such as num cores, max inflight tasks etc"""
    #     self._write_event({
    #         "name": "dispatch_metrics",
    #         "ph": "C",
    #         "pid": 1,
    #         "tid": 1,
    #         "args": metrics,
    #     })

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

    # def count_dispatch_batch_size(self, dispatch_batch_size: int):
    #     self._write_event({
    #         "name": "dispatch_batch_size",
    #         "ph": "C",
    #         "pid": 1,
    #         "tid": 1,
    #         "args": {"dispatch_batch_size": dispatch_batch_size},
    #     })

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

    # def count_num_ready(self, num_ready: int):
    #     self._write_event({
    #         "name": "awaiting",
    #         "ph": "C",
    #         "pid": 1,
    #         "tid": 1,
    #         "args": {"num_ready": num_ready},
    #     })

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
