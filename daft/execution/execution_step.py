from __future__ import annotations

import itertools
import pathlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, Protocol

from daft.daft import FileFormat, IOConfig, JoinType, ResourceRequest, ScanTask
from daft.expressions import Expression, ExpressionsProjection, col
from daft.logical.map_partition_ops import MapPartitionOp
from daft.logical.schema import Schema
from daft.runners.partitioning import (
    Boundaries,
    MaterializedResult,
    PartialPartitionMetadata,
    PartitionMetadata,
    PartitionT,
)
from daft.table import MicroPartition, table_io

if TYPE_CHECKING:
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.table import TableProperties as IcebergTableProperties


ID_GEN = itertools.count()


@dataclass
class PartitionTask(Generic[PartitionT]):
    """A PartitionTask describes a task that will run to create a partition.

    The partition will be created by running a function pipeline (`instructions`) over some input partition(s) (`inputs`).
    Each function takes an entire set of inputs and produces a new set of partitions to pass into the next function.

    This class should not be instantiated directly. To create the appropriate PartitionTask for your use-case, use the PartitionTaskBuilder.
    """

    inputs: list[PartitionT]
    instructions: list[Instruction]
    resource_request: ResourceRequest
    num_results: int
    stage_id: int
    partial_metadatas: list[PartialPartitionMetadata]
    _id: int = field(default_factory=lambda: next(ID_GEN))

    def id(self) -> str:
        return f"{self.__class__.__name__}_{self._id}"

    def done(self) -> bool:
        """Whether the PartitionT result of this task is available."""
        raise NotImplementedError()

    def cancel(self) -> None:
        """If possible, cancel the execution of this PartitionTask."""
        raise NotImplementedError()

    def set_result(self, result: list[MaterializedResult[PartitionT]]) -> None:
        """Set the result of this Task. For use by the Task executor."""
        raise NotImplementedError

    def is_empty(self) -> bool:
        """Whether this partition task is guaranteed to result in an empty partition."""
        return len(self.partial_metadatas) > 0 and all(meta.num_rows == 0 for meta in self.partial_metadatas)

    def __str__(self) -> str:
        return (
            f"{self.id()}\n"
            f"  Inputs: <{len(self.inputs)} partitions>\n"
            f"  Resource Request: {self.resource_request}\n"
            f"  Instructions: {[i.__class__.__name__ for i in self.instructions]}"
        )

    def __repr__(self) -> str:
        return self.__str__()

    def name(self) -> str:
        return f"{'-'.join(i.__class__.__name__ for i in self.instructions)} [Stage:{self.stage_id}]"


class PartitionTaskBuilder(Generic[PartitionT]):
    """Builds a PartitionTask by adding instructions to its pipeline."""

    def __init__(
        self,
        inputs: list[PartitionT],
        partial_metadatas: list[PartialPartitionMetadata] | None,
        resource_request: ResourceRequest = ResourceRequest(),
    ) -> None:
        self.inputs = inputs
        if partial_metadatas is not None:
            self.partial_metadatas = partial_metadatas
        else:
            self.partial_metadatas = [PartialPartitionMetadata(num_rows=None, size_bytes=None) for _ in self.inputs]
        self.resource_request: ResourceRequest = resource_request
        self.instructions: list[Instruction] = list()
        self.num_results = len(inputs)

    def add_instruction(
        self,
        instruction: Instruction,
        resource_request: ResourceRequest = ResourceRequest(),
    ) -> PartitionTaskBuilder[PartitionT]:
        """Append an instruction to this PartitionTask's pipeline."""
        self.instructions.append(instruction)
        self.partial_metadatas = instruction.run_partial_metadata(self.partial_metadatas)
        self.resource_request = ResourceRequest.max_resources([self.resource_request, resource_request])
        self.num_results = instruction.num_outputs()
        return self

    def is_empty(self) -> bool:
        """Whether this partition task is guaranteed to result in an empty partition."""
        return len(self.partial_metadatas) > 0 and all(meta.num_rows == 0 for meta in self.partial_metadatas)

    def finalize_partition_task_single_output(self, stage_id: int) -> SingleOutputPartitionTask[PartitionT]:
        """Create a SingleOutputPartitionTask from this PartitionTaskBuilder.

        Returns a "frozen" version of this PartitionTask that cannot have instructions added.
        """
        resource_request_final_cpu = ResourceRequest(
            num_cpus=self.resource_request.num_cpus or 1,
            num_gpus=self.resource_request.num_gpus,
            memory_bytes=self.resource_request.memory_bytes,
        )

        assert self.num_results == 1

        return SingleOutputPartitionTask[PartitionT](
            inputs=self.inputs,
            stage_id=stage_id,
            instructions=self.instructions,
            num_results=1,
            resource_request=resource_request_final_cpu,
            partial_metadatas=self.partial_metadatas,
        )

    def finalize_partition_task_multi_output(self, stage_id: int) -> MultiOutputPartitionTask[PartitionT]:
        """Create a MultiOutputPartitionTask from this PartitionTaskBuilder.

        Same as finalize_partition_task_single_output, except the output of this PartitionTask is a list of partitions.
        This is intended for execution steps that do a fanout.
        """
        resource_request_final_cpu = ResourceRequest(
            num_cpus=self.resource_request.num_cpus or 1,
            num_gpus=self.resource_request.num_gpus,
            memory_bytes=self.resource_request.memory_bytes,
        )
        return MultiOutputPartitionTask[PartitionT](
            inputs=self.inputs,
            stage_id=stage_id,
            instructions=self.instructions,
            num_results=self.num_results,
            resource_request=resource_request_final_cpu,
            partial_metadatas=self.partial_metadatas,
        )

    def __str__(self) -> str:
        return (
            f"PartitionTaskBuilder\n"
            f"  Inputs: <{len(self.inputs)} partitions>\n"
            f"  Resource Request: {self.resource_request}\n"
            f"  Instructions: {[i.__class__.__name__ for i in self.instructions]}"
        )


@dataclass
class SingleOutputPartitionTask(PartitionTask[PartitionT]):
    """A PartitionTask that is ready to run. More instructions cannot be added."""

    # When available, the partition created from running the PartitionTask.
    _result: None | MaterializedResult[PartitionT] = None

    def set_result(self, result: list[MaterializedResult[PartitionT]]) -> None:
        assert self._result is None, f"Cannot set result twice. Result is already {self._result}"
        [partition] = result
        self._result = partition

    def done(self) -> bool:
        return self._result is not None

    def result(self) -> MaterializedResult[PartitionT]:
        assert self._result is not None, "Cannot call .result() on a PartitionTask that is not done"
        return self._result

    def cancel(self) -> None:
        # Currently only implemented for Ray tasks.
        if self.done():
            self.result().cancel()

    def partition(self) -> PartitionT:
        """Get the PartitionT resulting from running this PartitionTask."""
        return self.result().partition()

    def partition_metadata(self) -> PartitionMetadata:
        """Get the metadata of the result partition.

        (Avoids retrieving the actual partition itself if possible.)
        """
        [partial_metadata] = self.partial_metadatas
        return self.result().metadata().merge_with_partial(partial_metadata)

    def vpartition(self) -> MicroPartition:
        """Get the raw vPartition of the result."""
        return self.result().vpartition()

    def __str__(self) -> str:
        return super().__str__()

    def __repr__(self) -> str:
        return super().__str__()


@dataclass
class MultiOutputPartitionTask(PartitionTask[PartitionT]):
    """A PartitionTask that is ready to run. More instructions cannot be added.
    This PartitionTask will return a list of any number of partitions.
    """

    # When available, the partitions created from running the PartitionTask.
    _results: None | list[MaterializedResult[PartitionT]] = None

    def set_result(self, result: list[MaterializedResult[PartitionT]]) -> None:
        assert self._results is None, f"Cannot set result twice. Result is already {self._results}"
        self._results = result

    def done(self) -> bool:
        return self._results is not None

    def cancel(self) -> None:
        if self._results is not None:
            for result in self._results:
                result.cancel()

    def partitions(self) -> list[PartitionT]:
        """Get the PartitionTs resulting from running this PartitionTask."""
        assert self._results is not None
        return [result.partition() for result in self._results]

    def partition_metadatas(self) -> list[PartitionMetadata]:
        """Get the metadata of the result partitions.

        (Avoids retrieving the actual partition itself if possible.)
        """
        assert self._results is not None
        return [
            result.metadata().merge_with_partial(partial_metadata)
            for result, partial_metadata in zip(self._results, self.partial_metadatas)
        ]

    def vpartition(self, index: int) -> MicroPartition:
        """Get the raw vPartition of the result."""
        assert self._results is not None
        return self._results[index].vpartition()

    def __str__(self) -> str:
        return super().__str__()

    def __repr__(self) -> str:
        return super().__str__()


class Instruction(Protocol):
    """An instruction is a function to run over a list of partitions.

    Most instructions take one partition and return another partition.
    However, some instructions take one partition and return many partitions (fanouts),
    and others take many partitions and return one partition (reduces).
    To accommodate these, instructions are typed as list[Table] -> list[Table].
    """

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        """Run the Instruction over the input partitions.

        Note: Dispatching a descriptively named helper here will aid profiling.
        """
        ...

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        """Calculate any possible metadata about the result partition that can be derived ahead of time."""
        ...

    def num_outputs(self) -> int:
        """How many partitions will result from running this instruction."""
        ...


class SingleOutputInstruction(Instruction):
    def num_outputs(self) -> int:
        return 1


@dataclass(frozen=True)
class ScanWithTask(SingleOutputInstruction):
    scan_task: ScanTask

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._scan(inputs)

    def _scan(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        assert len(inputs) == 0
        table = MicroPartition._from_scan_task(self.scan_task)
        return [table]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        assert len(input_metadatas) == 0

        return [
            PartialPartitionMetadata(
                num_rows=self.scan_task.num_rows(),
                size_bytes=self.scan_task.size_bytes(),
            )
        ]


@dataclass(frozen=True)
class EmptyScan(SingleOutputInstruction):
    schema: Schema

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return [MicroPartition.empty(self.schema)]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        assert len(input_metadatas) == 0

        return [
            PartialPartitionMetadata(
                num_rows=0,
                size_bytes=0,
            )
        ]


@dataclass(frozen=True)
class WriteFile(SingleOutputInstruction):
    file_format: FileFormat
    schema: Schema
    root_dir: str | pathlib.Path
    compression: str | None
    partition_cols: ExpressionsProjection | None
    io_config: IOConfig | None

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._write_file(inputs)

    def _write_file(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        partition = self._handle_file_write(
            input=input,
        )
        return [partition]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        assert len(input_metadatas) == 1
        return [
            PartialPartitionMetadata(
                num_rows=None,  # we can write more than 1 file per partition
                size_bytes=None,
            )
        ]

    def _handle_file_write(self, input: MicroPartition) -> MicroPartition:
        return table_io.write_tabular(
            input,
            path=self.root_dir,
            schema=self.schema,
            file_format=self.file_format,
            compression=self.compression,
            partition_cols=self.partition_cols,
            io_config=self.io_config,
        )


@dataclass(frozen=True)
class WriteIceberg(SingleOutputInstruction):
    base_path: str
    iceberg_schema: IcebergSchema
    iceberg_properties: IcebergTableProperties
    spec_id: int
    io_config: IOConfig | None

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._write_iceberg(inputs)

    def _write_iceberg(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        partition = self._handle_file_write(
            input=input,
        )
        return [partition]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        assert len(input_metadatas) == 1
        return [
            PartialPartitionMetadata(
                num_rows=None,  # we can write more than 1 file per partition
                size_bytes=None,
            )
        ]

    def _handle_file_write(self, input: MicroPartition) -> MicroPartition:
        return table_io.write_iceberg(
            input,
            base_path=self.base_path,
            schema=self.iceberg_schema,
            properties=self.iceberg_properties,
            spec_id=self.spec_id,
            io_config=self.io_config,
        )


@dataclass(frozen=True)
class WriteDeltaLake(SingleOutputInstruction):
    base_path: str
    large_dtypes: bool
    version: int
    io_config: IOConfig | None

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._write_deltalake(inputs)

    def _write_deltalake(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        partition = self._handle_file_write(
            input=input,
        )
        return [partition]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        assert len(input_metadatas) == 1
        return [
            PartialPartitionMetadata(
                num_rows=None,  # we can write more than 1 file per partition
                size_bytes=None,
            )
        ]

    def _handle_file_write(self, input: MicroPartition) -> MicroPartition:
        return table_io.write_deltalake(
            input,
            large_dtypes=self.large_dtypes,
            base_path=self.base_path,
            version=self.version,
            io_config=self.io_config,
        )


@dataclass(frozen=True)
class WriteLance(SingleOutputInstruction):
    base_path: str
    mode: str
    io_config: IOConfig | None
    kwargs: dict | None

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._write_lance(inputs)

    def _write_lance(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        partition = self._handle_file_write(
            input=input,
        )
        return [partition]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        assert len(input_metadatas) == 1
        return [
            PartialPartitionMetadata(
                num_rows=None,  # we can write more than 1 file per partition
                size_bytes=None,
            )
        ]

    def _handle_file_write(self, input: MicroPartition) -> MicroPartition:
        return table_io.write_lance(
            input,
            base_path=self.base_path,
            mode=self.mode,
            io_config=self.io_config,
            kwargs=self.kwargs,
        )


@dataclass(frozen=True)
class Filter(SingleOutputInstruction):
    predicate: ExpressionsProjection

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._filter(inputs)

    def _filter(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        return [input.filter(self.predicate)]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        [input_meta] = input_metadatas
        return [
            PartialPartitionMetadata(
                num_rows=None,
                size_bytes=None,
                boundaries=input_meta.boundaries,
            )
        ]


@dataclass(frozen=True)
class Project(SingleOutputInstruction):
    projection: ExpressionsProjection

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._project(inputs)

    def _project(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        return [input.eval_expression_list(self.projection)]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        [input_meta] = input_metadatas
        boundaries = input_meta.boundaries
        if boundaries is not None:
            boundaries = _prune_boundaries(boundaries, self.projection)
        return [
            PartialPartitionMetadata(
                num_rows=input_meta.num_rows,
                size_bytes=None,
                boundaries=boundaries,
            )
        ]


def _prune_boundaries(boundaries: Boundaries, projection: ExpressionsProjection) -> Boundaries | None:
    """
    If projection expression is a nontrivial computation (i.e. not a direct col() reference and not an alias) on top of a boundary
    expression, then invalidate the boundary.
    """
    proj_all_names = projection.to_name_set()
    proj_names_needing_compute = proj_all_names - projection.input_mapping().keys()
    for i, e in enumerate(boundaries.sort_by):
        if e.name() in proj_names_needing_compute:
            # Found a sort expression that is no longer valid, so we invalidate that sort expression and all that follow it.
            sort_by = boundaries.sort_by[:i]
            if not sort_by:
                return None
            boundaries_ = boundaries.bounds.eval_expression_list(
                ExpressionsProjection([col(e.name()) for e in sort_by])
            )
            return Boundaries(sort_by, boundaries_)
    return boundaries


@dataclass(frozen=True)
class LocalCount(SingleOutputInstruction):
    schema: Schema

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._count(inputs)

    def _count(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        partition = MicroPartition.from_pydict({"count": [len(input)]})
        assert partition.schema() == self.schema
        return [partition]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        return [
            PartialPartitionMetadata(
                num_rows=1,
                size_bytes=104,  # An empirical value, but will likely remain small.
            )
        ]


@dataclass(frozen=True)
class LocalLimit(SingleOutputInstruction):
    limit: int

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._limit(inputs)

    def _limit(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        return [input.head(self.limit)]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        [input_meta] = input_metadatas
        return [
            PartialPartitionMetadata(
                num_rows=(min(self.limit, input_meta.num_rows) if input_meta.num_rows is not None else None),
                size_bytes=None,
                boundaries=input_meta.boundaries,
            )
        ]


@dataclass(frozen=True)
class GlobalLimit(LocalLimit):
    pass


@dataclass(frozen=True)
class MapPartition(SingleOutputInstruction):
    map_op: MapPartitionOp

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._map_partition(inputs)

    def _map_partition(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        return [self.map_op.run(input)]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        # Can't derive anything.
        return [
            PartialPartitionMetadata(
                num_rows=None,
                size_bytes=None,
            )
        ]


@dataclass(frozen=True)
class Sample(SingleOutputInstruction):
    fraction: float | None = None
    size: int | None = None
    with_replacement: bool = False
    seed: int | None = None
    sort_by: ExpressionsProjection | None = None

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._sample(inputs)

    def _sample(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        if self.sort_by:
            result = (
                input.sample(self.fraction, self.size, self.with_replacement, self.seed)
                .eval_expression_list(self.sort_by)
                .filter(ExpressionsProjection([~col(e.name()).is_null() for e in self.sort_by]))
            )
        else:
            result = input.sample(self.fraction, self.size, self.with_replacement, self.seed)
        return [result]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        # Can't derive anything due to null filter in sample.
        return [
            PartialPartitionMetadata(
                num_rows=None,
                size_bytes=None,
            )
        ]


@dataclass(frozen=True)
class MonotonicallyIncreasingId(SingleOutputInstruction):
    partition_num: int
    column_name: str

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        result = input.add_monotonically_increasing_id(self.partition_num, self.column_name)
        return [result]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        [input_meta] = input_metadatas
        return [
            PartialPartitionMetadata(
                num_rows=input_meta.num_rows,
                size_bytes=(
                    (input_meta.size_bytes + input_meta.num_rows * 8)  # 8 bytes per uint64
                    if input_meta.size_bytes is not None and input_meta.num_rows is not None
                    else None
                ),
            )
        ]


@dataclass(frozen=True)
class Aggregate(SingleOutputInstruction):
    to_agg: list[Expression]
    group_by: ExpressionsProjection | None

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._aggregate(inputs)

    def _aggregate(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        return [input.agg(self.to_agg, self.group_by)]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        # Can't derive anything.
        return [
            PartialPartitionMetadata(
                num_rows=None,
                size_bytes=None,
            )
        ]


@dataclass(frozen=True)
class Pivot(SingleOutputInstruction):
    group_by: ExpressionsProjection
    pivot_col: Expression
    value_col: Expression
    names: list[str]

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._pivot(inputs)

    def _pivot(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        return [input.pivot(self.group_by, self.pivot_col, self.value_col, self.names)]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        return [
            PartialPartitionMetadata(
                num_rows=None,
                size_bytes=None,
            )
        ]


@dataclass(frozen=True)
class Unpivot(SingleOutputInstruction):
    ids: ExpressionsProjection
    values: ExpressionsProjection
    variable_name: str
    value_name: str

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._unpivot(inputs)

    def _unpivot(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        return [input.unpivot(self.ids, self.values, self.variable_name, self.value_name)]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        [input_meta] = input_metadatas
        return [
            PartialPartitionMetadata(
                num_rows=None if input_meta.num_rows is None else input_meta.num_rows * len(self.values),
                size_bytes=None,
            )
        ]


@dataclass(frozen=True)
class HashJoin(SingleOutputInstruction):
    left_on: ExpressionsProjection
    right_on: ExpressionsProjection
    how: JoinType
    is_swapped: bool

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._hash_join(inputs)

    def _hash_join(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        # All inputs except for the last are the left side of the join, in order to support left-broadcasted joins.
        *lefts, right = inputs
        if len(lefts) > 1:
            # NOTE: MicroPartition concats don't concatenate the underlying column arrays, since MicroPartitions are chunked.
            left = MicroPartition.concat(lefts)
        else:
            left = lefts[0]
        if self.is_swapped:
            # Swap left/right back.
            # We don't need to swap left_on and right_on since those were never swapped in the first place.
            left, right = right, left
        result = left.hash_join(
            right,
            left_on=self.left_on,
            right_on=self.right_on,
            how=self.how,
        )
        return [result]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        # Can't derive anything.
        return [
            PartialPartitionMetadata(
                num_rows=None,
                size_bytes=None,
            )
        ]


@dataclass(frozen=True)
class BroadcastJoin(HashJoin): ...


@dataclass(frozen=True)
class MergeJoin(SingleOutputInstruction):
    left_on: ExpressionsProjection
    right_on: ExpressionsProjection
    how: JoinType
    preserve_left_bounds: bool

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._join(inputs)

    def _join(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        left, right = inputs
        result = left.sort_merge_join(
            right,
            left_on=self.left_on,
            right_on=self.right_on,
            how=self.how,
            is_sorted=True,
        )
        return [result]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        [left_meta, right_meta] = input_metadatas
        # If the boundaries of the left and right partitions don't intersect, then the merge-join will result in an empty partition.
        if left_meta.boundaries is None or right_meta.boundaries is None:
            is_nonempty = True
        else:
            is_nonempty = left_meta.boundaries.intersects(right_meta.boundaries)
        return [
            PartialPartitionMetadata(
                num_rows=None if is_nonempty else 0,
                size_bytes=None,
                boundaries=(left_meta.boundaries if self.preserve_left_bounds else right_meta.boundaries),
            )
        ]


class ReduceInstruction(SingleOutputInstruction): ...


@dataclass(frozen=True)
class ReduceMerge(ReduceInstruction):
    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._reduce_merge(inputs)

    def _reduce_merge(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return [MicroPartition.concat(inputs)]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        input_rows = [_.num_rows for _ in input_metadatas]
        input_sizes = [_.size_bytes for _ in input_metadatas]
        return [
            PartialPartitionMetadata(
                num_rows=(sum(input_rows) if all(_ is not None for _ in input_rows) else None),
                size_bytes=(sum(input_sizes) if all(_ is not None for _ in input_sizes) else None),
            )
        ]


@dataclass(frozen=True)
class ReduceMergeAndSort(ReduceInstruction):
    sort_by: ExpressionsProjection
    descending: list[bool]
    bounds: MicroPartition

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._reduce_merge_and_sort(inputs)

    def _reduce_merge_and_sort(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        partition = MicroPartition.concat(inputs).sort(self.sort_by, descending=self.descending)
        return [partition]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        input_rows = [_.num_rows for _ in input_metadatas]
        input_sizes = [_.size_bytes for _ in input_metadatas]
        return [
            PartialPartitionMetadata(
                num_rows=(sum(input_rows) if all(_ is not None for _ in input_rows) else None),
                size_bytes=(sum(input_sizes) if all(_ is not None for _ in input_sizes) else None),
                boundaries=Boundaries(list(self.sort_by), self.bounds),
            )
        ]


@dataclass(frozen=True)
class ReduceToQuantiles(ReduceInstruction):
    num_quantiles: int
    sort_by: ExpressionsProjection
    descending: list[bool]

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._reduce_to_quantiles(inputs)

    def _reduce_to_quantiles(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        merged = MicroPartition.concat(inputs)

        # Skip evaluation of expressions by converting to Column Expression, since evaluation was done in Sample
        merged_sorted = merged.sort(self.sort_by.to_column_expressions(), descending=self.descending)

        result = merged_sorted.quantiles(self.num_quantiles)
        return [result]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        return [
            PartialPartitionMetadata(
                num_rows=self.num_quantiles,
                size_bytes=None,
            )
        ]


@dataclass(frozen=True)
class FanoutInstruction(Instruction):
    _num_outputs: int

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        # Can't derive anything.
        return [
            PartialPartitionMetadata(
                num_rows=None,
                size_bytes=None,
            )
            for _ in range(self._num_outputs)
        ]

    def num_outputs(self) -> int:
        return self._num_outputs


@dataclass(frozen=True)
class FanoutRandom(FanoutInstruction):
    seed: int

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._fanout_random(inputs)

    def _fanout_random(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        return input.partition_by_random(num_partitions=self._num_outputs, seed=self.seed)


@dataclass(frozen=True)
class FanoutHash(FanoutInstruction):
    partition_by: ExpressionsProjection

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._fanout_hash(inputs)

    def _fanout_hash(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        return input.partition_by_hash(self.partition_by, num_partitions=self._num_outputs)


@dataclass(frozen=True)
class FanoutRange(FanoutInstruction, Generic[PartitionT]):
    sort_by: ExpressionsProjection
    descending: list[bool]

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._fanout_range(inputs)

    def _fanout_range(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [boundaries, input] = inputs
        if self._num_outputs == 1:
            return [input]

        table_boundaries = boundaries.to_table()
        partitioned_tables = input.partition_by_range(self.sort_by, table_boundaries, self.descending)

        # Pad the partitioned_tables with empty tables if fewer than self._num_outputs were returned
        # This can happen when all values are null or empty, which leads to an empty `boundaries` input
        assert len(partitioned_tables) >= 1, "Should have at least one returned table"
        schema = partitioned_tables[0].schema()
        partitioned_tables = partitioned_tables + [
            MicroPartition.empty(schema=schema) for _ in range(self._num_outputs - len(partitioned_tables))
        ]

        return partitioned_tables


@dataclass(frozen=True)
class FanoutSlices(FanoutInstruction):
    slices: list[tuple[int, int]]  # start inclusive, end exclusive

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._multislice(inputs)

    def _multislice(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        results = []

        for start, end in self.slices:
            assert start >= 0, f"start must be positive, but got {start}"
            end = min(end, len(input))

            results.append(input.slice(start, end))

        return results

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        [input_meta] = input_metadatas

        results = []
        for start, end in self.slices:
            definite_end = min(end, input_meta.num_rows) if input_meta.num_rows is not None else None
            assert start >= 0, f"start must be positive, but got {start}"

            if definite_end is not None:
                num_rows = definite_end - start
                num_rows = max(num_rows, 0)
            else:
                num_rows = None

            results.append(
                PartialPartitionMetadata(
                    num_rows=num_rows,
                    size_bytes=None,
                    boundaries=input_meta.boundaries,
                )
            )

        return results
