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
    partition_num: int | None = None,
    concurrency: int | None = None,
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

    effective_partition_num = partition_num or 1
    effective_partition_num = min(num_tasks, effective_partition_num)
    assert effective_partition_num > 0
    if effective_partition_num == 1:
        df = daft.from_pydict({"task": plan.tasks})
    else:
        df = daft.from_pydict({"task": plan.tasks}).repartition(effective_partition_num)

    WrappedRunner = daft.cls(
        CompactionTaskUDF,
        max_concurrency=concurrency,
    )
    df = df.select(WrappedRunner(lance_ds)(df["task"]).alias("rewrite"))
    results = df.to_pandas()

    metrics = Compaction.commit(lance_ds, results["rewrite"].to_list())
    logger.info("Compaction completed successfully. Metrics: %s", metrics)
    return metrics
