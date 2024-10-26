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
    start: float
    end: float | None


@ray.remote(num_cpus=0)
class MetricsActor:
    def __init__(self):
        self.task_starts: dict[str, float] = {}
        self.task_ends: dict[str, float] = {}

    def mark_task_start(self, task_id: str, start: float):
        self.task_starts[task_id] = start

    def mark_task_end(self, task_id: str, end: float):
        self.task_ends[task_id] = end

    def collect(self) -> list[TaskMetric]:
        return [
            TaskMetric(
                task_id=task_id,
                start=self.task_starts[task_id],
                end=self.task_ends.get(task_id),
            )
            for task_id in self.task_starts
        ]


def get_metrics_actor(job_id: str) -> ray.actor.ActorHandle:
    """Retrieves a handle to the Actor for a given job_id"""
    return MetricsActor.options(  # type: ignore[attr-defined]
        name=f"{METRICS_ACTOR_NAME}-{job_id}",
        namespace=METRICS_ACTOR_NAMESPACE,
        get_if_exists=True,
    ).remote()
