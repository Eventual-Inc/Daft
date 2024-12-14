from __future__ import annotations

import daft
from daft.runners.ray_runner import build_partitions
from tests.memory.utils import df_to_tasks, track_memory


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
