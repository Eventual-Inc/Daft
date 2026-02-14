from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from lance.optimize import Compaction, CompactionMetrics, CompactionOptions, CompactionTask, RewriteResult

if TYPE_CHECKING:
    import lance
import daft

logger = logging.getLogger(__name__)


class CompactionTaskUDF:
    """UDF to execute a batch of Lance CompactionTasks on remote workers and return execution result dictionaries."""

    def __init__(
        self,
        lance_ds: lance.LanceDataset,
    ) -> None:
        self.lance_ds = lance_ds

    def __call__(self, task: CompactionTask) -> RewriteResult:
        rewrite = task.execute(self.lance_ds)
        return rewrite


def compact_files_internal(
    lance_ds: lance.LanceDataset,
    *,
    compaction_options: dict[str, Any] | None = None,
    num_partitions: int | None = None,
    concurrency: int | None = None,
    micro_commit_batch_size: int | None = None,
) -> CompactionMetrics | None:
    """Execute Lance file compaction in distributed environment using Daft UDF style."""
    logger.info("Starting UDF-style distributed compaction")
    plan = Compaction.plan(
        lance_ds,
        CompactionOptions(
            **(compaction_options or {}),
        ),
    )
    num_tasks = plan.num_tasks()
    logger.info("Compaction plan created with %d tasks", num_tasks)

    if num_tasks == 0:
        logger.info("No compaction tasks needed")
        return None

    partition_num = None if num_partitions is None or num_partitions <= 1 else min(num_tasks, num_partitions)

    tasks = plan.tasks
    if micro_commit_batch_size is None or micro_commit_batch_size <= 0:
        micro_commit_batch_size = num_tasks
    else:
        micro_commit_batch_size = min(micro_commit_batch_size, num_tasks)

    with daft.execution_config_ctx(maintain_order=False):
        for i in range(0, num_tasks, micro_commit_batch_size):
            batch_tasks = tasks[i : i + micro_commit_batch_size]
            df = daft.from_pydict({"task": batch_tasks})
            if partition_num is not None:
                df = df.repartition(partition_num)

            WrappedRunner = daft.cls(
                CompactionTaskUDF,
                max_concurrency=concurrency,
            )
            df = df.select(WrappedRunner(lance_ds)(df["task"]).alias("rewrite"))
            results = df.to_pandas()

            metrics = Compaction.commit(lance_ds, results["rewrite"].to_list())
            logger.info("Compaction completed successfully. Metrics: %s", metrics)

    # When micro_commit_batch_size is not configured or is greater than or equal to num_tasks,we return the metrics data obtained from compact commit to lance.
    # In other cases, batch compact commit is used, and the final metrics data cannot be obtained.

    if micro_commit_batch_size == num_tasks:
        return metrics

    return None
