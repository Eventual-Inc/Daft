from __future__ import annotations

from typing import Callable

from daft.expressions import Expression
from daft.logical import logical_plan
from daft.logical.map_partition_ops import MapPartitionOp
from daft.logical.schema import ExpressionList
from daft.runners.partitioning import vPartition
from daft.runners.pyrunner import LocalLogicalPartitionOpRunner
from daft.runners.shuffle_ops import RepartitionHashOp, RepartitionRandomOp, SortOp


class Construction:
    """A Construction is an instruction stack + input partitions to run the instruction stack over.

    Instructions can be one partition -> one partition, one->many, or many->one.
    (To support this, instructions are typed as list[partition] -> list[partition].)
    """

    def __init__(self, inputs: list[vPartition]) -> None:
        # Input partitions to run over.
        self.inputs = inputs

        # Instruction stack to execute.
        self.instruction_stack: list[Callable[[list[vPartition]], list[vPartition]]] = list()

        # Where to put the materialized results.
        self._destination_array: None | list[vPartition | None] = None
        self._partno: None | int = None

    def add_instruction(self, instruction: Callable[[list[vPartition]], list[vPartition]]) -> None:
        """Add an instruction to the stack that will run for this partition."""
        self.assert_not_marked()
        self.instruction_stack.append(instruction)

    def mark_for_materialization(self, destination_array: list[vPartition | None], num_results: int = 1) -> None:
        """Mark this Construction for materialization.

        1. Prevents further instructions from being added to this Construction.
        2. Saves a reference to where the materialized results should be placed.
        """
        self.assert_not_marked()
        self._destination_array = destination_array
        self._partno = len(destination_array)
        self._destination_array += [None] * num_results

    def report_completed(self, results: list[vPartition]) -> None:
        """Give the materialized result of this Construction to the DynamicSchedule who asked for it."""

        assert self._destination_array is not None
        assert self._partno is not None

        for i, partition in enumerate(results):
            assert self._destination_array[self._partno + i] is None, self._destination_array[self._partno + i]
            self._destination_array[self._partno + i] = partition

    def is_marked_for_materialization(self) -> bool:
        return all(_ is not None for _ in (self._destination_array, self._partno))

    def assert_not_marked(self) -> None:
        assert (
            not self.is_marked_for_materialization()
        ), f"Partition already instructed to materialize into {self._destination_array}, partition index {self._partno}"


# -- Various instructions. --


def make_read_file_instruction(
    scan_node: logical_plan.Scan, index: int
) -> Callable[[list[vPartition]], list[vPartition]]:
    def instruction(inputs: list[vPartition]) -> list[vPartition]:
        assert len(inputs) == 0
        partition = LocalLogicalPartitionOpRunner()._handle_scan(
            inputs=dict(),
            scan=scan_node,
            partition_id=index,
        )
        return [partition]

    return instruction


def make_merge_instruction() -> Callable[[list[vPartition]], list[vPartition]]:
    def instruction(inputs: list[vPartition]) -> list[vPartition]:
        return [vPartition.merge_partitions(inputs, verify_partition_id=False)]

    return instruction


def make_agg_instruction(
    to_agg: list[tuple[Expression, str]], group_by: ExpressionList | None
) -> Callable[[list[vPartition]], list[vPartition]]:
    def instruction(inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        return [input.agg(to_agg, group_by)]

    return instruction


def make_write_instruction(node: logical_plan.FileWrite, index: int) -> Callable[[list[vPartition]], list[vPartition]]:
    child_id = node._children()[0].id()

    def instruction(inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        partition = LocalLogicalPartitionOpRunner()._handle_file_write(
            inputs={child_id: input},
            file_write=node,
            partition_id=index,
        )
        return [partition]

    return instruction


def make_filter_instruction(predicate: ExpressionList) -> Callable[[list[vPartition]], list[vPartition]]:
    def instruction(inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        return [input.filter(predicate)]

    return instruction


def make_project_instruction(projection: ExpressionList) -> Callable[[list[vPartition]], list[vPartition]]:
    def instruction(inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        return [input.eval_expression_list(projection)]

    return instruction


def make_map_partition_instruction(map_op: MapPartitionOp) -> Callable[[list[vPartition]], list[vPartition]]:
    def instruction(inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        return [map_op.run(input)]

    return instruction


def make_join_instruction(join: logical_plan.Join) -> Callable[[list[vPartition]], list[vPartition]]:
    def instruction(inputs: list[vPartition]) -> list[vPartition]:
        [left, right] = inputs
        result = left.join(
            right,
            left_on=join._left_on,
            right_on=join._right_on,
            output_schema=join.schema(),
            how=join._how.value,
        )
        return [result]

    return instruction


def make_local_limit_instruction(limit: int) -> Callable[[list[vPartition]], list[vPartition]]:
    def instruction(inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        return [input.head(limit)]

    return instruction


def make_map_to_samples_instruction(
    sort_by: ExpressionList, num_samples: int = 20
) -> Callable[[list[vPartition]], list[vPartition]]:
    """From logical_op_runners."""

    def instruction(inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        result = (
            input.sample(num_samples)
            .eval_expression_list(sort_by)
            .filter(ExpressionList([~e.to_column_expression().is_null() for e in sort_by]).resolve(sort_by))
        )
        return [result]

    return instruction


def make_reduce_to_quantiles_instruction(
    sort_by: ExpressionList, descending: list[bool], num_quantiles: int
) -> Callable[[list[vPartition]], list[vPartition]]:
    """From logical_op_runners."""

    def instruction(inputs: list[vPartition]) -> list[vPartition]:
        merged = vPartition.merge_partitions(inputs, verify_partition_id=False)
        merged_sorted = merged.sort(sort_by, descending=descending)
        result = merged_sorted.quantiles(num_quantiles)
        return [result]

    return instruction


def make_fanout_range_instruction(
    sort_by: ExpressionList, descending: list[bool], boundaries: vPartition, num_outputs: int
) -> Callable[[list[vPartition]], list[vPartition]]:
    def instruction(inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        partitions_with_ids = SortOp.map_fn(
            input=input,
            output_partitions=num_outputs,
            exprs=sort_by,
            boundaries=boundaries,
            descending=descending,
        )
        return [partition for _, partition in sorted(partitions_with_ids.items())]

    return instruction


def make_merge_and_sort_instruction(
    sort_by: ExpressionList, descending: list[bool]
) -> Callable[[list[vPartition]], list[vPartition]]:
    def instruction(inputs: list[vPartition]) -> list[vPartition]:
        partition = SortOp.reduce_fn(
            mapped_outputs=inputs,
            exprs=sort_by,
            descending=descending,
        )
        return [partition]

    return instruction


def make_fanout_hash_instruction(
    num_outputs: int, partition_by: ExpressionList
) -> Callable[[list[vPartition]], list[vPartition]]:
    def instruction(inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        partitions_with_ids = RepartitionHashOp.map_fn(
            input=input,
            output_partitions=num_outputs,
            exprs=partition_by,
        )
        return [partition for i, partition in sorted(partitions_with_ids.items())]

    return instruction


def make_fanout_random_instruction(num_outputs: int) -> Callable[[list[vPartition]], list[vPartition]]:
    def instruction(inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        partitions_with_ids = RepartitionRandomOp.map_fn(
            input=input,
            output_partitions=num_outputs,
        )
        return [partition for _, partition in sorted(partitions_with_ids.items())]

    return instruction
