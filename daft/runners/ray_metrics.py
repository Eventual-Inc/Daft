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


@ray.remote(num_cpus=0)
class _MetricsActor:
    def __init__(self):
        self.start_execution_metrics: dict[str, StartExecutionMetrics] = defaultdict(lambda: StartExecutionMetrics())
        self.end_execution_metrics: dict[str, EndExecutionMetrics] = defaultdict(lambda: EndExecutionMetrics())
        self.execution_node_and_worker_ids: dict[str, dict[str, set[str]]] = defaultdict(
            lambda: defaultdict(lambda: set())
        )

    def ready(self):
        # Discussion on how to check if an actor is ready: https://github.com/ray-project/ray/issues/14923
        return None

    def mark_task_start(
        self, execution_id: str, task_id: str, start: float, node_id: str, worker_id: str, stage_id: int
    ):
        # Update node info
        node_id_trunc, worker_id_trunc = node_id[:8], worker_id[:8]
        self.execution_node_and_worker_ids[execution_id][node_id_trunc].add(worker_id_trunc)

        # Update task info
        self.start_execution_metrics[execution_id].task_metrics[task_id] = TaskMetric(
            task_id=task_id,
            stage_id=stage_id,
            start=start,
            node_id=node_id_trunc,
            worker_id=worker_id_trunc,
            end=None,
        )

    def mark_task_end(self, execution_id: str, task_id: str, end: float):
        self.end_execution_metrics[execution_id].task_ends[task_id] = end

    def collect_metrics(self, execution_id: str) -> tuple[list[TaskMetric], dict[str, set[str]]]:
        """Collect the metrics associated with this execution, cleaning up the memory used for this execution ID"""
        # Data about the available nodes and worker IDs in those nodes
        node_data = self.execution_node_and_worker_ids[execution_id]

        # Data about task
        start_execution_metrics = self.start_execution_metrics[execution_id]
        task_metrics = list(start_execution_metrics.task_metrics.values())
        for tm in task_metrics:
            if tm.task_id in self.end_execution_metrics[execution_id].task_ends:
                tm.end = self.end_execution_metrics[execution_id].task_ends[tm.task_id]

        # Clean up the stats for this execution
        del self.start_execution_metrics[execution_id]
        del self.end_execution_metrics[execution_id]
        del self.execution_node_and_worker_ids[execution_id]

        return task_metrics, node_data


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

    def collect_metrics(self) -> tuple[list[TaskMetric], dict[str, set[str]]]:
        return ray.get(self.actor.collect_metrics.remote(self.execution_id))


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
