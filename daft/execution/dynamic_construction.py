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
ID_GEN = itertools.count()

@dataclass
class BaseConstruction(Generic[PartitionT]):
    """A Construction represents a set of partitions that are waiting to be created.

    The result partitions will be created by running some function stack over some input partitions.
    Each function takes an entire set of inputs and produces a new set of partitions to pass into the next function.

    _id: A unique identifier for this Construction.
    inputs: The partitions that will be input together into the function stack.
    instruction_stack: The functions to run over the inputs, in order. See Instruction for more details.
    """

    inputs: list[PartitionT]
    instructions: list[Instruction]

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}\n"
            f"  Inputs: {self.inputs}\n"
            f"  Instructions: {[i.__class__.__name__ for i in self.instructions]}"
        )

    def __repr__(self) -> str:
        return self.__str__()

class OpenConstruction(BaseConstruction[PartitionT]):
    """This is a Construction that can still have functions added to its function stack.

    New Constructions should be created from this class.
    """


    def __init__(self, inputs: list[PartitionT]) -> None:
        super().__init__(
            inputs=inputs,
            instructions=list(),
        )

    def add_instruction(self, instruction: Instruction) -> OpenConstruction[PartitionT]:
        """Add an instruction to this Construction's stack."""
        self.instructions.append(instruction)
        return self

    def as_materization_request(self) -> MaterializationRequest[PartitionT]:
        """Create an MaterializationRequest from this Construction.

        Returns a "frozen" version of this Construction that cannot have instructions added.
        The output of this Construction should be a single partition.
        """

        return MaterializationRequest[PartitionT](
            _id=self._id,
            inputs=self.inputs,
            instructions=self.instructions,
        )

    def as_materization_request_multi(self) -> MaterializationRequestMulti[PartitionT]:
        """Create an MaterializationRequestMulti from this Construction.

        Same as as_materization_request,
        except it denotes that the output of this Construction is a list of any number of partitions.
        """

    def copy(self) -> OpenConstruction[PartitionT]:
        return OpenConstruction[PartitionT](
            inputs=self.inputs.copy(),
            instructions=self.instructions.copy(),
        )

@dataclass
class MaterializationRequestBase(BaseConstruction[PartitionT]):
    """Common helpers for MaterializationRequest and MaterializationRequestMulti.

    _id: A unique identifier for this Construction.
    """

    _id: int = field(default_factory=lambda: next(ID_GEN))

    def id(self) -> str:
        return f"{self.__class__.__name__}_{self._id}"

    def __str__(self) -> str:
        return (
            f"{self.id()}\n"
            f"  Inputs: {self.inputs}\n"
            f"  Instructions: {[i.__class__.__name__ for i in self.instructions]}"
        )


@dataclass
class MaterializationRequest(MaterializationRequestBase[PartitionT]):
    """A Construction that is ready to execute. More instructions cannot be added.

    _id: A unique identifier for this Construction.
    result: When ready, the partition created from executing the Construction.
    """
    result: None | MaterializationResult[PartitionT] = None


@dataclass
class MaterializationRequestMulti(MaterializationRequestBase[PartitionT]):
    """A Construction that is ready to execute. More instructions cannot be added.
    This Construction will return a list of any number of partitions.

    _id: A unique identifier for this Construction.
    results: When ready, the partitions created from executing the Construction.
    """
    results: None | list[MaterializationResult[PartitionT]] = None


class MaterializationResult(Protocol[PartitionT]):
    """A wrapper class for accessing the result partition of a Construction."""

    def partition(self) -> PartitionT:
        """Get the partition of this result."""
        raise NotImplementedError

    def metadata(self) -> PartitionMetadata:
        """Get the metadata of the partition in this result."""
        raise NotImplementedError

    def cancel(self) -> None:
        """If possible, cancel execution of this Construction."""
        raise NotImplementedError



class Instruction(Protocol):
    """An instruction is a function to run over a list of partitions.

    Most instructions take one partition and return another partition.
    However, some instructions take one partition and return many partitions (fanouts),
    and others take many partitions and return one partition (reduces).
    To accomodate these, instructions are typed as list[vPartition] -> list[vPartition].
    """
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


@dataclass(frozen=True)
class FanoutInstruction(Instruction):
    num_outputs: int


@dataclass(frozen=True)
class FanoutRandom(FanoutInstruction):
    def run(self, inputs: list[vPartition]) -> list[vPartition]:
        [input] = inputs
        partitions_with_ids = RepartitionRandomOp.map_fn(
            input=input,
            output_partitions=self.num_outputs,
        )
        return [partition for _, partition in sorted(partitions_with_ids.items())]


@dataclass(frozen=True)
class FanoutHash(FanoutInstruction):
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
    sort_by: ExpressionList
    descending: list[bool]

    def run(self, inputs: list[vPartition]) -> list[vPartition]:
        [boundaries, input] = inputs

        partitions_with_ids = SortOp.map_fn(
            input=input,
            output_partitions=self.num_outputs,
            exprs=self.sort_by,
            boundaries=boundaries,
            descending=self.descending,
        )
        return [partition for _, partition in sorted(partitions_with_ids.items())]
