from __future__ import annotations

import dataclasses
import logging

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
class TaskMetric:
    task_id: str
    stage_id: int | None
    node_id: str
    worker_id: str
    start: float
    end: float | None


@dataclasses.dataclass
class ExecutionMetrics:
    """Holds the metrics for a given execution ID"""

    daft_execution_id: str
    task_start_info: dict[str, TaskMetric] = dataclasses.field(default_factory=lambda: {})
    task_ends: dict[str, float] = dataclasses.field(default_factory=lambda: {})


@ray.remote(num_cpus=0)
class _MetricsActor:
    def __init__(self):
        self.execution_metrics: dict[str, ExecutionMetrics] = {}
        self.execution_node_and_worker_ids: dict[str, dict[str, set[str]]] = {}

    def _get_or_create_execution_metrics(self, execution_id: str) -> ExecutionMetrics:
        if execution_id not in self.execution_metrics:
            self.execution_metrics[execution_id] = ExecutionMetrics(daft_execution_id=execution_id)
        return self.execution_metrics[execution_id]

    def _get_or_create_execution_node_and_worker_ids(self, execution_id: str) -> dict[str, set[str]]:
        if execution_id not in self.execution_node_and_worker_ids:
            self.execution_node_and_worker_ids[execution_id] = {}
        return self.execution_node_and_worker_ids[execution_id]

    def mark_task_start(
        self, execution_id: str, task_id: str, start: float, node_id: str, worker_id: str, stage_id: int
    ):
        # Update node info
        node_id_trunc, worker_id_trunc = node_id[:8], worker_id[:8]
        node_info = self._get_or_create_execution_node_and_worker_ids(execution_id)
        if node_id_trunc not in node_info:
            node_info[node_id_trunc] = set()
        node_info[node_id_trunc].add(worker_id_trunc)

        # Update task info
        self._get_or_create_execution_metrics(execution_id).task_start_info[task_id] = TaskMetric(
            task_id=task_id,
            stage_id=stage_id,
            start=start,
            node_id=node_id_trunc,
            worker_id=worker_id_trunc,
            end=None,
        )

    def mark_task_end(self, execution_id: str, task_id: str, end: float):
        self._get_or_create_execution_metrics(execution_id).task_ends[task_id] = end

    def collect_metrics(self, execution_id: str) -> tuple[list[TaskMetric], dict[str, set[str]]]:
        """Collect the metrics associated with this execution, cleaning up the memory used for this execution ID"""
        execution_metrics = self._get_or_create_execution_metrics(execution_id)
        data = [
            dataclasses.replace(
                execution_metrics.task_start_info[task_id], end=execution_metrics.task_ends.get(task_id)
            )
            for task_id in execution_metrics.task_start_info
        ]
        node_data = self._get_or_create_execution_node_and_worker_ids(execution_id)

        # Clean up the stats for this execution
        del self.execution_metrics[execution_id]
        del self.execution_node_and_worker_ids[execution_id]

        return data, node_data


@dataclasses.dataclass(frozen=True)
class MetricsActorHandle:
    execution_id: str
    actor: ray.actor.ActorHandle

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


def get_metrics_actor(execution_id: str) -> MetricsActorHandle:
    """Retrieves a handle to the Actor for a given job_id"""
    actor = _MetricsActor.options(  # type: ignore[attr-defined]
        name="METRICS_ACTOR_NAME",
        namespace=METRICS_ACTOR_NAMESPACE,
        get_if_exists=True,
        lifetime="detached",
    ).remote()
    return MetricsActorHandle(execution_id, actor)
