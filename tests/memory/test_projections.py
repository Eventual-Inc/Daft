from __future__ import annotations

import contextlib
import dataclasses
import os
import tempfile
from typing import Iterator
from unittest import mock

import memray
from memray._memray import compute_statistics

import daft
from daft.context import get_context
from daft.execution.physical_plan import MaterializedPhysicalPlan
from daft.runners.ray_runner import build_partitions


@dataclasses.dataclass
class LazyMemrayStats:
    memray_stats: memray._stats.Stats | None

    def unwrap(self) -> memray._stats.Stats:
        assert self.memray_stats is not None
        return self.memray_stats


@contextlib.contextmanager
def track_memory() -> Iterator[LazyMemrayStats]:
    tracked = LazyMemrayStats(None)
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpfile = os.path.join(tmpdir, "memray.bin")
        with memray.Tracker(tmpfile):
            yield tracked

        stats = compute_statistics(tmpfile)
        tracked.memray_stats = stats


def df_to_tasks(df: daft.DataFrame) -> MaterializedPhysicalPlan:
    cfg = get_context().daft_execution_config
    physical_plan = df._builder.to_physical_plan_scheduler(cfg)

    return physical_plan.to_partition_tasks(
        psets={
            k: v.values()
            for k, v in get_context().get_or_create_runner()._part_set_cache.get_all_partition_sets().items()
        },
        actor_pool_manager=mock.Mock(),
        results_buffer_size=None,
    )


def test_simple_project():
    df = daft.read_parquet("tests/assets/parquet-data/mvp.parquet")
    df = df.with_column("c", df["a"] + 100)

    tasks = df_to_tasks(df)
    partition_task = next(tasks)

    with track_memory() as lazy_memray_stats:
        _ = build_partitions(
            partition_task.instructions,
            partition_task.partial_metadatas,
            *partition_task.inputs,
        )

    assert partition_task.resource_request.memory_bytes is not None, "Partition Task must have resource request"
    assert (
        lazy_memray_stats.unwrap().peak_memory_allocated < partition_task.resource_request.memory_bytes
    ), "Execution must use less memory than requested"
