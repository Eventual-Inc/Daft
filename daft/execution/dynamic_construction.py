from __future__ import annotations

import itertools
import sys
from dataclasses import dataclass
from typing import Callable, Generic, TypeVar

if sys.version_info < (3, 8):
    from typing_extensions import Protocol
else:
    from typing import Protocol

import ray

from daft.expressions import Expression
from daft.logical import logical_plan
from daft.logical.map_partition_ops import MapPartitionOp
from daft.logical.schema import ExpressionList
from daft.runners.partitioning import PartitionMetadata, vPartition
from daft.runners.pyrunner import LocalLogicalPartitionOpRunner
from daft.runners.shuffle_ops import RepartitionHashOp, RepartitionRandomOp, SortOp

PartitionT = TypeVar("PartitionT")


@dataclass(frozen=True)
class PartitionWithInfo(Generic[PartitionT]):
    partition: PartitionT
    metadata: Callable[[PartitionT], PartitionMetadata]


class Construction(Generic[PartitionT]):
    """A Construction is an instruction stack + input partitions to run the instruction stack over.

    Instructions can be one partition -> one partition, one->many, or many->one.
    (To support this, instructions are typed as list[partition] -> list[partition].)
    """

    ID_GEN = (f"Construction_{i}" for i in itertools.count())

    def __init__(self, inputs: list[PartitionT]) -> None:
        self.id = next(self.ID_GEN)

        # Input partitions to run over.
        self.inputs = inputs

        # Instruction stack to execute.
        self.instruction_stack: list[Instruction] = list()

        # Where to put the materialized results.
        self.num_results: None | int = None
        self._dispatched: list[PartitionT] = []
        self._destination_array: None | list[PartitionWithInfo[PartitionT] | None] = None
        self._partno: None | int = None

    def add_instruction(self, instruction: Instruction) -> None:
        """Add an instruction to the stack that will run for this partition."""
        self.assert_not_marked()
        self.instruction_stack.append(instruction)

    def mark_for_materialization(
        self, destination_array: list[PartitionWithInfo[PartitionT] | None], num_results: int = 1
    ) -> None:
        """Mark this Construction for materialization.

        1. Prevents further instructions from being added to this Construction.
        2. Saves a reference to where the materialized results should be placed.
        """
        self.assert_not_marked()
        self._destination_array = destination_array
        self._partno = len(destination_array)
        self._destination_array += [None] * num_results
        self.num_results = num_results

    def report_completed(self, results: list[PartitionWithInfo[PartitionT]]) -> None:
        """Give the materialized result of this Construction to the DynamicSchedule who asked for it."""

        assert self._destination_array is not None
        assert self._partno is not None

        self._dispatched = [_.partition for _ in results]

        for i, partition_with_info in enumerate(results):
            assert self._destination_array[self._partno + i] is None, self._destination_array[self._partno + i]
            self._destination_array[self._partno + i] = partition_with_info

    def is_marked_for_materialization(self) -> bool:
        return all(_ is not None for _ in (self._destination_array, self._partno))

    def assert_not_marked(self) -> None:
        assert (
            not self.is_marked_for_materialization()
        ), f"Partition already instructed to materialize into {self._destination_array}, partition index {self._partno}"


class Instruction(Protocol):
    def run(self, inputs: list[vPartition]) -> list[vPartition]:
        ...


@dataclass(frozen=True)
class ReadFile(Instruction):
    partition_id: int
    logplan: logical_plan.TabularFilesScan

    def run(self, inputs: list[vPartition]) -> list[vPartition]:
        assert len(inputs) == 1
        [filepaths_partition] = inputs
        partition = LocalLogicalPartitionOpRunner()._handle_tabular_files_scan(
            inputs={self.logplan._filepaths_child.id(): filepaths_partition},
            scan=self.logplan,
            partition_id=self.partition_id,
        )
        return [partition]


@dataclass(frozen=True)
class WriteFile(Instruction):
    partition_id: int
    logplan: logical_plan.FileWrite

    def run(self, inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        partition = LocalLogicalPartitionOpRunner()._handle_file_write(
            inputs={self.logplan._children()[0].id(): input},
            file_write=self.logplan,
            partition_id=self.partition_id,
        )
        return [partition]


@dataclass(frozen=True)
class Filter(Instruction):
    predicate: ExpressionList

    def run(self, inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        return [input.filter(self.predicate)]


@dataclass(frozen=True)
class Project(Instruction):
    projection: ExpressionList

    def run(self, inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        return [input.eval_expression_list(self.projection)]


@dataclass(frozen=True)
class LocalLimit(Instruction):
    limit: int

    def run(self, inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        return [input.head(self.limit)]


@dataclass(frozen=True)
class MapPartition(Instruction):
    map_op: MapPartitionOp

    def run(self, inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        return [self.map_op.run(input)]


@dataclass(frozen=True)
class Sample(Instruction):
    sort_by: ExpressionList
    num_samples: int = 20

    def run(self, inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        result = (
            input.sample(self.num_samples)
            .eval_expression_list(self.sort_by)
            .filter(ExpressionList([~e.to_column_expression().is_null() for e in self.sort_by]).resolve(self.sort_by))
        )
        return [result]


@dataclass(frozen=True)
class Aggregate(Instruction):
    to_agg: list[tuple[Expression, str]]
    group_by: ExpressionList | None

    def run(self, inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        return [input.agg(self.to_agg, self.group_by)]


@dataclass(frozen=True)
class Join(Instruction):
    logplan: logical_plan.Join

    def run(self, inputs: list[vPartition]) -> list[vPartition]:
        [left, right] = inputs
        result = left.join(
            right,
            left_on=self.logplan._left_on,
            right_on=self.logplan._right_on,
            output_schema=self.logplan.schema(),
            how=self.logplan._how.value,
        )
        return [result]


class ReduceInstruction(Instruction):
    ...


@dataclass(frozen=True)
class ReduceMerge(ReduceInstruction):
    def run(self, inputs: list[vPartition]) -> list[vPartition]:
        return [vPartition.merge_partitions(inputs, verify_partition_id=False)]


@dataclass(frozen=True)
class ReduceMergeAndSort(ReduceInstruction):
    sort_by: ExpressionList
    descending: list[bool]

    def run(self, inputs: list[vPartition]) -> list[vPartition]:
        partition = SortOp.reduce_fn(
            mapped_outputs=inputs,
            exprs=self.sort_by,
            descending=self.descending,
        )
        return [partition]


@dataclass(frozen=True)
class ReduceToQuantiles(ReduceInstruction):
    num_quantiles: int
    sort_by: ExpressionList
    descending: list[bool]

    def run(self, inputs: list[vPartition]) -> list[vPartition]:
        merged = vPartition.merge_partitions(inputs, verify_partition_id=False)
        merged_sorted = merged.sort(self.sort_by, descending=self.descending)
        result = merged_sorted.quantiles(self.num_quantiles)
        return [result]


class FanoutInstruction(Instruction):
    ...


@dataclass(frozen=True)
class FanoutRandom(FanoutInstruction):
    num_outputs: int

    def run(self, inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        partitions_with_ids = RepartitionRandomOp.map_fn(
            input=input,
            output_partitions=self.num_outputs,
        )
        return [partition for _, partition in sorted(partitions_with_ids.items())]


@dataclass(frozen=True)
class FanoutHash(FanoutInstruction):
    num_outputs: int
    partition_by: ExpressionList

    def run(self, inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        partitions_with_ids = RepartitionHashOp.map_fn(
            input=input,
            output_partitions=self.num_outputs,
            exprs=self.partition_by,
        )
        return [partition for i, partition in sorted(partitions_with_ids.items())]


@dataclass(frozen=True)
class FanoutRange(FanoutInstruction, Generic[PartitionT]):
    num_outputs: int
    sort_by: ExpressionList
    descending: list[bool]
    boundaries: PartitionT

    def run(self, inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        # TODO find a generic way to do this
        vpart: vPartition
        if isinstance(self.boundaries, vPartition):
            vpart = self.boundaries
        elif isinstance(self.boundaries, ray.ObjectRef):
            vpart = ray.get(self.boundaries)
        else:
            raise RuntimeError(f"Unsupported partition type {type(self.boundaries)}")

        partitions_with_ids = SortOp.map_fn(
            input=input,
            output_partitions=self.num_outputs,
            exprs=self.sort_by,
            boundaries=vpart,
            descending=self.descending,
        )
        return [partition for _, partition in sorted(partitions_with_ids.items())]
