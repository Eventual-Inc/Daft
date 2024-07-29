from unittest.mock import MagicMock

import pytest

from daft.execution.physical_plan import Materialize, PartitionTask
from daft.execution.rust_physical_plan_shim import scan_with_tasks


def test_single_non_buffered_plan():
    result1, result2, result3 = MagicMock(), MagicMock(), MagicMock()
    scan = scan_with_tasks([MagicMock() for i in range(3)])
    materialized = Materialize(scan, results_buffer_size=None)
    plan = iter(materialized)

    # Buffer is unbounded and grows as we iterate on the plan (when no tasks are completed)
    assert len(materialized.materializations) == 0
    task1 = next(plan)
    assert isinstance(task1, PartitionTask)
    assert len(materialized.materializations) == 1
    task2 = next(plan)
    assert isinstance(task2, PartitionTask)
    assert len(materialized.materializations) == 2
    task3 = next(plan)
    assert isinstance(task3, PartitionTask)
    assert len(materialized.materializations) == 3

    # Ran out of work, waiting on new tasks to be completed
    assert next(plan) is None

    # Manually "complete" the tasks
    task1.set_result([result1])
    task2.set_result([result2])
    task3.set_result([result3])

    # Results should be as we expect
    assert next(plan) == result1
    assert next(plan) == result2
    assert next(plan) == result3
    with pytest.raises(StopIteration):
        next(plan)


def test_single_non_buffered_plan_done_while_planning():
    result1, result2, result3 = MagicMock(), MagicMock(), MagicMock()
    scan = scan_with_tasks([MagicMock() for i in range(3)])
    materialized = Materialize(scan, results_buffer_size=None)
    plan = iter(materialized)

    # Buffer is unbounded and grows as we iterate on the plan (when no tasks are completed)
    assert len(materialized.materializations) == 0
    task1 = next(plan)
    assert isinstance(task1, PartitionTask)
    assert len(materialized.materializations) == 1
    task2 = next(plan)
    assert isinstance(task2, PartitionTask)
    assert len(materialized.materializations) == 2

    # Manually "complete" the tasks
    task1.set_result([result1])
    task2.set_result([result2])

    # On the next iteration, we should receive the result
    assert next(plan) == result1
    assert next(plan) == result2

    # Add more work to completion
    task3 = next(plan)
    assert isinstance(task3, PartitionTask)
    assert len(materialized.materializations) == 1  # only task3 on the buffer

    # Manually "complete" the last task
    task3.set_result([result3])

    # Results should be as we expect
    assert next(plan) == result3
    with pytest.raises(StopIteration):
        next(plan)


def test_single_plan_with_buffer_slow_tasks():
    result1, result2, result3 = MagicMock(), MagicMock(), MagicMock()
    scan = scan_with_tasks([MagicMock() for i in range(3)])
    materialized = Materialize(scan, results_buffer_size=2)
    plan = iter(materialized)

    # Buffer and grows to size 2 (tasks are "slow" and don't complete faster than the next plan loop call)
    assert len(materialized.materializations) == 0
    task1 = next(plan)
    assert isinstance(task1, PartitionTask)
    assert len(materialized.materializations) == 1
    task2 = next(plan)
    assert isinstance(task2, PartitionTask)
    assert len(materialized.materializations) == 2

    # Plan cannot make forward progress until task1 finishes
    assert next(plan) is None
    task2.set_result([result2])
    assert next(plan) is None
    task1.set_result([result1])

    # Plan should fill its buffer with new tasks before starting to yield results again
    task3 = next(plan)
    assert isinstance(task3, PartitionTask)
    assert next(plan) == result1
    assert next(plan) == result2

    # Finish the last task
    task3.set_result([result3])
    assert next(plan) == result3

    with pytest.raises(StopIteration):
        next(plan)


def test_single_plan_with_buffer_saturation_fast_tasks():
    result1, result2, result3 = MagicMock(), MagicMock(), MagicMock()
    scan = scan_with_tasks([MagicMock() for i in range(3)])
    materialized = Materialize(scan, results_buffer_size=2)
    plan = iter(materialized)

    # Buffer grows to size 1
    assert len(materialized.materializations) == 0
    task1 = next(plan)
    assert isinstance(task1, PartitionTask)
    assert len(materialized.materializations) == 1

    # Finish up on task 1 (task is "fast" and completes so quickly even before the next plan loop call)
    task1.set_result([result1])

    # Plan should fill its buffer completely with new tasks before starting to yield results again
    task2 = next(plan)
    assert isinstance(task2, PartitionTask)
    assert len(materialized.materializations) == 1  # task1 has been popped
    task3 = next(plan)
    assert isinstance(task3, PartitionTask)
    assert len(materialized.materializations) == 2  # task1 has been popped
    assert next(plan) == result1
    assert next(plan) is None

    # Finish the last task(s)
    task2.set_result([result2])
    task3.set_result([result3])
    assert next(plan) == result2
    assert next(plan) == result3

    with pytest.raises(StopIteration):
        next(plan)
