from __future__ import annotations

import asyncio
import unittest.mock
from unittest.mock import AsyncMock, MagicMock

from daft.runners.flotilla import RaySwordfishActorHandle, RaySwordfishTaskHandle, SwordfishTaskMetadata
from daft.runners.partitioning import PartitionMetadata


class MockObjectRef:
    def __init__(self, val=None):
        self.val = val

    def __await__(self):
        async def _get():
            return self.val

        return _get().__await__()


def get_result_partitions(result):
    """Extract partition refs from a RayTaskResult.

    PyO3 exposes unnamed enum variant fields as `_0`, `_1`, etc.
    """
    return result._0


def create_mock_handle(task_name, num_partitions, results, partition_metadatas, stats=b"stats", is_into_batches=False):
    """Helper to create a RaySwordfishTaskHandle with mocked results."""
    task_meta = SwordfishTaskMetadata(partition_metadatas=partition_metadatas, stats=stats)
    meta_ref = MockObjectRef(task_meta)

    # Pack results into MockObjectRefs
    refs = [MockObjectRef(r) for r in results]

    result_handle = MagicMock()
    result_handle.completed = AsyncMock()
    # The iterator should yield result refs then the metadata ref
    result_handle.__iter__.return_value = iter(refs + [meta_ref])

    return RaySwordfishTaskHandle(
        result_handle=result_handle,
        actor_handle=MagicMock(),
        num_partitions=num_partitions,
        is_into_batches=is_into_batches,
    )


def test_submit_task():
    actor_ref = MagicMock()
    actor_handle = RaySwordfishActorHandle(actor_ref)

    class MockPartition:
        def __init__(self, refs):
            self.object_refs = refs

    class MockTask:
        def __init__(self):
            self.name_val = "TestTask"
            self.num_partitions_val = 5
            self.is_into_batches_val = False
            self.psets_val = {
                "input1": [MockPartition(["ref1a"]), MockPartition(["ref1b"])],
                "input2": [MockPartition(["ref2"])],
            }

        def name(self):
            return self.name_val

        def num_partitions(self):
            return self.num_partitions_val

        def is_into_batches(self):
            return self.is_into_batches_val

        def psets(self):
            return self.psets_val

        def plan(self):
            return "plan"

        def config(self):
            return "config"

        def context(self):
            return {"ctx": "val"}

    task = MockTask()
    actor_ref.run_plan.options.return_value.remote.return_value = "result_handle"

    task_handle = actor_handle.submit_task(task)

    assert task_handle.num_partitions == 5
    assert task_handle.result_handle == "result_handle"

    actor_ref.run_plan.options.assert_called_once_with(name="TestTask")
    call_args = actor_ref.run_plan.options.return_value.remote.call_args
    passed_psets = call_args[0][2]
    assert set(passed_psets["input1"]) == {"ref1a", "ref1b"}
    assert passed_psets["input2"] == ["ref2"]


def test_get_result_into_batches():
    async def run():
        handle = create_mock_handle(
            task_name="OpIntoBatches",
            num_partitions=2,  # Ignored for IntoBatches
            results=["res1", "res2"],
            partition_metadatas=[
                PartitionMetadata(num_rows=10, size_bytes=100),
                PartitionMetadata(num_rows=20, size_bytes=200),
            ],
            is_into_batches=True,
        )
        result = await handle._get_result()

        assert result.success
        parts = get_result_partitions(result)
        assert len(parts) == 2
        assert parts[0].num_rows == 10
        assert parts[0].size_bytes == 100

    asyncio.run(run())


def test_get_result_packed_simple():
    async def run():
        handle = create_mock_handle(
            task_name="SomeOp",
            num_partitions=1,
            results=["res1", "res2"],
            partition_metadatas=[
                PartitionMetadata(num_rows=10, size_bytes=100),
                PartitionMetadata(num_rows=20, size_bytes=200),
            ],
        )
        result = await handle._get_result()

        assert result.success
        parts = get_result_partitions(result)
        assert len(parts) == 1
        assert parts[0].num_rows == 30  # Sum of rows
        assert parts[0].size_bytes == 300  # Sum of bytes

    asyncio.run(run())


def test_get_result_packed_distribution():
    async def run():
        handle = create_mock_handle(
            task_name="SomeOp",
            num_partitions=2,
            results=["res1", "res2", "res3"],
            partition_metadatas=[
                PartitionMetadata(num_rows=10, size_bytes=100),
                PartitionMetadata(num_rows=20, size_bytes=200),
                PartitionMetadata(num_rows=5, size_bytes=50),
            ],
        )
        result = await handle._get_result()

        assert result.success
        parts = get_result_partitions(result)
        assert len(parts) == 2
        # Part 0: res1(10,100) + res3(5,50)
        assert parts[0].num_rows == 15
        assert parts[0].size_bytes == 150
        # Part 1: res2(20,200)
        assert parts[1].num_rows == 20
        assert parts[1].size_bytes == 200

    asyncio.run(run())


def test_get_result_packed_empty_results():
    async def run():
        handle = create_mock_handle(task_name="SomeOp", num_partitions=2, results=[], partition_metadatas=[])
        result = await handle._get_result()

        assert result.success
        parts = get_result_partitions(result)
        assert len(parts) == 2
        assert parts[0].num_rows == 0
        assert parts[1].num_rows == 0

    asyncio.run(run())


def test_cancel_before_get_result():
    """Test that cancel() works before get_result() is called."""
    handle = create_mock_handle(
        task_name="SomeOp",
        num_partitions=1,
        results=["res1"],
        partition_metadatas=[PartitionMetadata(num_rows=10, size_bytes=100)],
    )

    # task should be None before get_result
    assert handle.task is None

    # cancel() should not raise even when task is None
    # ray.cancel is called on result_handle (a MagicMock, so it's a no-op)
    with unittest.mock.patch("ray.cancel") as mock_cancel:
        handle.cancel()
        mock_cancel.assert_called_once_with(handle.result_handle)


def test_cancel_after_get_result():
    """Test that cancel() cancels the running task."""

    async def run():
        handle = create_mock_handle(
            task_name="SomeOp",
            num_partitions=1,
            results=["res1"],
            partition_metadatas=[PartitionMetadata(num_rows=10, size_bytes=100)],
        )
        # Start the task via await
        task = asyncio.ensure_future(handle.get_result())
        # Yield control so the task starts
        await asyncio.sleep(0)
        assert handle.task is not None

        with unittest.mock.patch("ray.cancel"):
            handle.cancel()
            # Task is in cancelling state after cancel()
            assert handle.task.cancelling() > 0

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(run())
