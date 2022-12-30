from __future__ import annotations

from typing import Callable, Generic, TypeVar

from daft.expressions import Expression
from daft.logical import logical_plan
from daft.logical.map_partition_ops import MapPartitionOp
from daft.logical.schema import ExpressionList
from daft.runners.partitioning import vPartition
from daft.runners.pyrunner import LocalLogicalPartitionOpRunner
from daft.runners.shuffle_ops import RepartitionHashOp, RepartitionRandomOp, SortOp

PartitionT = TypeVar("PartitionT")


class Construction(Generic[PartitionT]):
    """A Construction is an instruction stack + input partitions to run the instruction stack over.

    Instructions can be one partition -> one partition, one->many, or many->one.
    (To support this, instructions are typed as list[partition] -> list[partition].)
    """

    def __init__(self, inputs: list[PartitionT]) -> None:
        # Input partitions to run over.
        self.inputs = inputs

        # Instruction stack to execute.
        self._instruction_stack: list[Callable[[list[vPartition]], list[vPartition]]] = list()

        # Where to put the materialized results.
        self._destination_array: None | list[PartitionT | None] = None
        self._partno: None | int = None

    def add_instruction(self, instruction: Callable[[list[vPartition]], list[vPartition]]) -> None:
        """Add an instruction to the stack that will run for this partition."""
        self.assert_not_marked()
        self._instruction_stack.append(instruction)

    def mark_for_materialization(self, destination_array: list[PartitionT | None], num_results: int = 1) -> None:
        """Mark this Construction for materialization.

        1. Prevents further instructions from being added to this Construction.
        2. Saves a reference to where the materialized results should be placed.
        """
        self.assert_not_marked()
        self._destination_array = destination_array
        self._partno = len(destination_array)
        self._destination_array += [None] * num_results

    def report_completed(self, results: list[PartitionT]) -> None:
        """Give the materialized result of this Construction to the DynamicSchedule who asked for it."""

        assert self._destination_array is not None
        assert self._partno is not None

        for i, partition in enumerate(results):
            assert self._destination_array[self._partno + i] is None, self._destination_array[self._partno + i]
            self._destination_array[self._partno + i] = partition

    def get_runnable(self) -> Callable[[list[vPartition]], list[vPartition]]:
        def runnable(partitions: list[vPartition]) -> list[vPartition]:
            for instruction in self._instruction_stack:
                partitions = instruction(partitions)
            return partitions

        return runnable

    def is_marked_for_materialization(self) -> bool:
        return all(_ is not None for _ in (self._destination_array, self._partno))

    def assert_not_marked(self) -> None:
        assert (
            not self.is_marked_for_materialization()
        ), f"Partition already instructed to materialize into {self._destination_array}, partition index {self._partno}"


class InstructionFactory:
    """Instructions for use with Construction."""

    @staticmethod
    def read_file(
        scan_node: logical_plan.TabularFilesScan, index: int
    ) -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            assert len(inputs) == 1
            [filepaths_partition] = inputs
            partition = LocalLogicalPartitionOpRunner()._handle_tabular_files_scan(
                inputs={scan_node._filepaths_child.id(): filepaths_partition},
                scan=scan_node,
                partition_id=index,
            )
            return [partition]

        return instruction

    @staticmethod
    def merge() -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            return [vPartition.merge_partitions(inputs, verify_partition_id=False)]

        return instruction

    @staticmethod
    def agg(
        to_agg: list[tuple[Expression, str]], group_by: ExpressionList | None
    ) -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            [input] = inputs
            return [input.agg(to_agg, group_by)]

        return instruction

    @staticmethod
    def write(node: logical_plan.FileWrite, index: int) -> Callable[[list[vPartition]], list[vPartition]]:
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

    @staticmethod
    def filter(predicate: ExpressionList) -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            [input] = inputs
            return [input.filter(predicate)]

        return instruction

    @staticmethod
    def project(projection: ExpressionList) -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            [input] = inputs
            return [input.eval_expression_list(projection)]

        return instruction

    @staticmethod
    def map_partition(map_op: MapPartitionOp) -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            [input] = inputs
            return [map_op.run(input)]

        return instruction

    @staticmethod
    def join(join: logical_plan.Join) -> Callable[[list[vPartition]], list[vPartition]]:
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

    @staticmethod
    def local_limit(limit: int) -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            [input] = inputs
            return [input.head(limit)]

        return instruction

    @staticmethod
    def map_to_samples(
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

    @staticmethod
    def reduce_to_quantiles(
        sort_by: ExpressionList, descending: list[bool], num_quantiles: int
    ) -> Callable[[list[vPartition]], list[vPartition]]:
        """From logical_op_runners."""

        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            merged = vPartition.merge_partitions(inputs, verify_partition_id=False)
            merged_sorted = merged.sort(sort_by, descending=descending)
            result = merged_sorted.quantiles(num_quantiles)
            return [result]

        return instruction

    @staticmethod
    def fanout_range(
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

    @staticmethod
    def merge_and_sort(
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

    @staticmethod
    def fanout_hash(num_outputs: int, partition_by: ExpressionList) -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            [input] = inputs
            partitions_with_ids = RepartitionHashOp.map_fn(
                input=input,
                output_partitions=num_outputs,
                exprs=partition_by,
            )
            return [partition for i, partition in sorted(partitions_with_ids.items())]

        return instruction

    @staticmethod
    def fanout_random(num_outputs: int) -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            [input] = inputs
            partitions_with_ids = RepartitionRandomOp.map_fn(
                input=input,
                output_partitions=num_outputs,
            )
            return [partition for _, partition in sorted(partitions_with_ids.items())]

        return instruction
