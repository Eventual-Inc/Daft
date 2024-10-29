from __future__ import annotations

import dataclasses
import logging
import threading
from collections import defaultdict

logger = logging.getLogger(__name__)

try:
    import ray
except ImportError:
    logger.error(
        "Error when importing Ray. Please ensure that getdaft was installed with the Ray extras tag: getdaft[ray] (https://www.getdaft.io/projects/docs/en/latest/learn/install.html)"
    )
    raise

METRICS_ACTOR_NAME = "metrics"
METRICS_ACTOR_NAMESPACE = "daft"


@dataclasses.dataclass
class TaskMetric:
    task_id: str
    stage_id: int | None
    node_id: str
    worker_id: str
    start: float
    end: float | None


@dataclasses.dataclass(frozen=True)
class StartExecutionMetrics:
    """Holds the metrics for a given execution ID"""

    task_metrics: dict[str, TaskMetric] = dataclasses.field(default_factory=lambda: {})


@dataclasses.dataclass
class EndExecutionMetrics:
    """Holds the metrics for a given execution ID for the ending of task executions"""

    task_ends: dict[str, float] = dataclasses.field(default_factory=lambda: {})


@dataclasses.dataclass(frozen=True)
class TaskEvent:
    task_id: str


@dataclasses.dataclass(frozen=True)
class StartTaskEvent(TaskEvent):
    start: float
    stage_id: int
    node_idx: int
    worker_idx: int


@dataclasses.dataclass(frozen=True)
class EndTaskEvent(TaskEvent):
    end: float


@ray.remote(num_cpus=0)
class _MetricsActor:
    def __init__(self):
        self.task_events: dict[str, list[TaskEvent]] = defaultdict(lambda: [])
        self.execution_node_and_worker_ids: dict[str, dict[str, set[str]]] = defaultdict(
            lambda: defaultdict(lambda: set())
        )
        self.execution_node_idxs: dict[str, dict[str, int]] = defaultdict(lambda: {})
        self.execution_worker_idxs: dict[str, dict[str, int]] = defaultdict(lambda: {})

    def ready(self):
        # Discussion on how to check if an actor is ready: https://github.com/ray-project/ray/issues/14923
        return None

    def mark_task_start(
        self, execution_id: str, task_id: str, start: float, node_id: str, worker_id: str, stage_id: int
    ):
        # Update node info
        node_id_trunc, worker_id_trunc = node_id[:8], worker_id[:16]
        if node_id_trunc not in self.execution_node_and_worker_ids[execution_id]:
            self.execution_node_idxs[execution_id][node_id_trunc] = len(self.execution_node_idxs[execution_id])
        if worker_id_trunc not in self.execution_worker_idxs[execution_id]:
            self.execution_worker_idxs[execution_id][worker_id_trunc] = len(self.execution_worker_idxs[execution_id])
        self.execution_node_and_worker_ids[execution_id][node_id_trunc].add(worker_id_trunc)

        # Add a StartTaskEvent
        self.task_events[execution_id].append(
            StartTaskEvent(
                task_id=task_id,
                stage_id=stage_id,
                start=start,
                node_idx=self.execution_node_idxs[execution_id][node_id_trunc],
                worker_idx=self.execution_worker_idxs[execution_id][worker_id_trunc],
            )
        )

    def mark_task_end(self, execution_id: str, task_id: str, end: float):
        # Add an EndTaskEvent
        self.task_events[execution_id].append(EndTaskEvent(task_id=task_id, end=end))

    def drain_task_events(self, execution_id: str, idx: int) -> tuple[list[TaskEvent], int]:
        events = self.task_events[execution_id]
        return (events[idx:], len(events))

    def collect_and_close(self, execution_id: str) -> dict[str, set[str]]:
        """Collect the metrics associated with this execution, cleaning up the memory used for this execution ID"""
        # Data about the available nodes and worker IDs in those nodes
        node_data = self.execution_node_and_worker_ids[execution_id]

        # Clean up the stats for this execution
        del self.task_events[execution_id]
        del self.execution_node_idxs[execution_id]
        del self.execution_worker_idxs[execution_id]
        del self.execution_node_and_worker_ids[execution_id]

        return node_data


@dataclasses.dataclass(frozen=True)
class MetricsActorHandle:
    execution_id: str
    actor: ray.actor.ActorHandle

    def wait(self) -> None:
        """Call to block until the underlying actor is ready"""
        return ray.get(self.actor.ready.remote())

    def mark_task_start(
        self,
        task_id: str,
        start: float,
        node_id: str,
        worker_id: str,
        stage_id: int,
    ) -> None:
        self.actor.mark_task_start.remote(
            self.execution_id,
            task_id,
            start,
            node_id,
            worker_id,
            stage_id,
        )

    def mark_task_end(
        self,
        task_id: str,
        end: float,
    ) -> None:
        self.actor.mark_task_end.remote(
            self.execution_id,
            task_id,
            end,
        )

    def drain_task_events(self, idx: int) -> tuple[list[TaskEvent], int]:
        """Collect task metrics from a given logical event index

        Returns the task metrics and the new logical event index (to be used as a pagination offset token on subsequent requests)
        """
        return ray.get(self.actor.drain_task_events.remote(self.execution_id, idx))

    def collect_and_close(self) -> dict[str, set[str]]:
        """Collect node metrics and close the metrics actor for this execution"""
        return ray.get(self.actor.collect_and_close.remote(self.execution_id))


# Creating/getting an actor from multiple threads is not safe.
#
# This could be a problem because our Scheduler does multithreaded executions of plans if multiple
# plans are submitted at once.
#
# Pattern from Ray Data's _StatsActor:
# https://github.com/ray-project/ray/blob/0b1d0d8f01599796e1109060821583e270048b6e/python/ray/data/_internal/stats.py#L447-L449
_metrics_actor_lock: threading.RLock = threading.RLock()


def get_metrics_actor(execution_id: str) -> MetricsActorHandle:
    """Retrieves a handle to the Actor for a given job_id"""
    with _metrics_actor_lock:
        actor = _MetricsActor.options(  # type: ignore[attr-defined]
            name="daft_metrics_actor",
            namespace=METRICS_ACTOR_NAMESPACE,
            get_if_exists=True,
            lifetime="detached",
        ).remote()
        return MetricsActorHandle(execution_id, actor)
