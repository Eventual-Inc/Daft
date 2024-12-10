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


@dataclasses.dataclass(frozen=True)
class TaskEvent:
    task_id: str


@dataclasses.dataclass(frozen=True)
class StartTaskEvent(TaskEvent):
    """Marks the start of a task, along with available metadata."""

    # Start Unix timestamp
    start: float

    # What stage this task belongs to
    stage_id: int

    # Index of the node
    node_idx: int

    # Index of the worker within the node (not monotonically increasing)
    worker_idx: int

    # The resources that Ray assigned to this task
    ray_assigned_resources: dict
    ray_task_id: str


@dataclasses.dataclass(frozen=True)
class EndTaskEvent(TaskEvent):
    """Marks the end of a task, along with available metadata."""

    # End Unix timestamp
    end: float


class _NodeInfo:
    """Information about nodes and their workers."""

    def __init__(self):
        self.node_to_workers = {}
        self.node_idxs = {}
        self.worker_idxs = {}

    def get_node_and_worker_idx(self, node_id: str, worker_id: str) -> tuple[int, int]:
        """Returns a node and worker index for the provided IDs."""
        # Truncate to save space
        node_id = node_id[:8]
        worker_id = worker_id[:8]

        if node_id not in self.node_to_workers:
            self.node_to_workers[node_id] = []
            self.node_idxs[node_id] = len(self.node_idxs)
        node_idx = self.node_idxs[node_id]

        if worker_id not in self.worker_idxs:
            self.worker_idxs[worker_id] = len(self.worker_idxs)
            self.node_to_workers[node_id].append(worker_id)
        worker_idx = self.worker_idxs[worker_id]

        return node_idx, worker_idx

    def collect_node_info(self) -> dict[str, list[str]]:
        """Returns a dictionary of {node_id: [worker_ids...]}."""
        return self.node_to_workers.copy()


@ray.remote(num_cpus=0)
class _MetricsActor:
    def __init__(self):
        self._task_events: dict[str, list[TaskEvent]] = defaultdict(list)
        self._node_info: dict[str, _NodeInfo] = defaultdict(_NodeInfo)

    def ready(self):
        """Returns when the metrics actor is ready."""
        # Discussion on how to check if an actor is ready: https://github.com/ray-project/ray/issues/14923
        return None

    def mark_task_start(
        self,
        execution_id: str,
        task_id: str,
        start: float,
        node_id: str,
        worker_id: str,
        stage_id: int,
        ray_assigned_resources: dict,
        ray_task_id: str,
    ):
        """Records a task start event."""
        # Update node info
        node_idx, worker_idx = self._node_info[execution_id].get_node_and_worker_idx(node_id, worker_id)

        # Add a StartTaskEvent
        self._task_events[execution_id].append(
            StartTaskEvent(
                task_id=task_id,
                stage_id=stage_id,
                start=start,
                node_idx=node_idx,
                worker_idx=worker_idx,
                ray_assigned_resources=ray_assigned_resources,
                ray_task_id=ray_task_id,
            )
        )

    def mark_task_end(self, execution_id: str, task_id: str, end: float):
        # Add an EndTaskEvent
        self._task_events[execution_id].append(EndTaskEvent(task_id=task_id, end=end))

    def get_task_events(self, execution_id: str, idx: int) -> tuple[list[TaskEvent], int]:
        events = self._task_events[execution_id]
        return (events[idx:], len(events))

    def collect_and_close(self, execution_id: str) -> dict[str, list[str]]:
        """Collect the metrics associated with this execution, cleaning up the memory used for this execution ID."""
        # Data about the available nodes and worker IDs in those nodes
        node_data = self._node_info[execution_id].collect_node_info()

        # Clean up the stats for this execution
        del self._task_events[execution_id]
        del self._node_info[execution_id]

        return node_data


@dataclasses.dataclass(frozen=True)
class MetricsActorHandle:
    execution_id: str
    actor: ray.actor.ActorHandle

    def wait(self) -> None:
        """Call to block until the underlying actor is ready."""
        return ray.wait([self.actor.ready.remote()], fetch_local=False)

    def mark_task_start(
        self,
        task_id: str,
        start: float,
        node_id: str,
        worker_id: str,
        stage_id: int,
        ray_assigned_resources: dict,
        ray_task_id: str,
    ) -> None:
        self.actor.mark_task_start.remote(
            self.execution_id,
            task_id,
            start,
            node_id,
            worker_id,
            stage_id,
            ray_assigned_resources,
            ray_task_id,
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

    def get_task_events(self, idx: int) -> tuple[list[TaskEvent], int]:
        """Collect task metrics from a given logical event index.

        Returns the task metrics and the new logical event index (to be used as a pagination offset token on subsequent requests)
        """
        return ray.get(self.actor.get_task_events.remote(self.execution_id, idx))

    def collect_and_close(self) -> dict[str, set[str]]:
        """Collect node metrics and close the metrics actor for this execution."""
        return ray.get(self.actor.collect_and_close.remote(self.execution_id))


# Creating/getting an actor from multiple threads is not safe.
#
# This could be a problem because our Scheduler does multithreaded executions of plans if multiple
# plans are submitted at once.
#
# Pattern from Ray Data's _StatsActor:
# https://github.com/ray-project/ray/blob/0b1d0d8f01599796e1109060821583e270048b6e/python/ray/data/_internal/stats.py#L447-L449
_metrics_actor_lock: threading.Lock = threading.Lock()


def get_metrics_actor(execution_id: str) -> MetricsActorHandle:
    """Retrieves a handle to the Actor for a given job_id."""
    with _metrics_actor_lock:
        actor = _MetricsActor.options(  # type: ignore[attr-defined]
            name="daft_metrics_actor",
            namespace=METRICS_ACTOR_NAMESPACE,
            get_if_exists=True,
            lifetime="detached",
        ).remote()
        return MetricsActorHandle(execution_id, actor)
