from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar, Generic, Protocol

from daft.context import get_context
from daft.daft import ResourceRequest
from daft.expressions import Expression, ExpressionsProjection, col
from daft.runners.partitioning import (
    Boundaries,
    EstimatedPartitionMetadata,
    ExactPartitionMetadata,
    MaterializedResult,
    PartitionT,
)
from daft.table import MicroPartition, table_io

if TYPE_CHECKING:
    import pathlib

    from pyiceberg.partitioning import PartitionSpec as IcebergPartitionSpec
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.table import TableProperties as IcebergTableProperties

    from daft.daft import FileFormat, IOConfig, JoinType, ScanTask
    from daft.logical.map_partition_ops import MapPartitionOp
    from daft.logical.schema import Schema


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
    partial_metadatas: list[EstimatedPartitionMetadata]

    # Indicates that this PartitionTask must be executed on the executor with the supplied ID
    # This is used when a specific executor (e.g. an Actor pool) must be provisioned and used for the task
    actor_pool_id: str | None

    # Indicates if the PartitionTask is "done" or not
    is_done: bool = False

    _id: int = field(default_factory=lambda: next(ID_GEN))

    def id(self) -> str:
        return f"{self.__class__.__name__}_{self._id}"

    def done(self) -> bool:
        """Whether the PartitionT result of this task is available."""
        return self.is_done

    def set_done(self):
        """Sets the PartitionTask as done."""
        assert not self.is_done, "Cannot set PartitionTask as done more than once"
        self.is_done = True

    def cancel(self) -> None:
        """If possible, cancel the execution of this PartitionTask."""
        raise NotImplementedError()

    def set_result(self, result: list[MaterializedResult[PartitionT]]) -> None:
        """Set the result of this Task. For use by the Task executor.

        NOTE: A PartitionTask may contain a `result` without being `.done()`. This is because
        results can potentially contain futures which are yet to be completed.
        """
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
        input_partition_metadatas: list[ExactPartitionMetadata],
        actor_pool_id: str | None = None,
    ) -> None:
        self.inputs = inputs
        self.instructions: list[Instruction] = list()
        self.num_results = len(inputs)
        self.actor_pool_id = actor_pool_id

        # Initial `estimated_output_partition_metadatas` is set to the inputs, since it is initially a no-op
        self.estimated_output_partition_metadatas: list[EstimatedPartitionMetadata] = [
            m.downcast_to_estimated() for m in input_partition_metadatas
        ]
        self.input_partition_exact_partition_metadatas = input_partition_metadatas

        # Initial resource request for this should for a no-op, since there are no instructions
        self.resource_request: ResourceRequest = ResourceRequest()

    def exact_partition_metadata(self) -> list[ExactPartitionMetadata] | None:
        """Returns exact partition metadata, or None if unable to provide an exact version of it"""
        if len(self.instructions) > 0:
            return None

        # If this builder is still a no-op, then the partition metadata is exact
        exact_meta: list[ExactPartitionMetadata] = []
        for m in self.estimated_output_partition_metadatas:
            assert isinstance(m, ExactPartitionMetadata)
            exact_meta.append(m)
        return exact_meta

    def add_instruction(
        self,
        instruction: Instruction,
    ) -> PartitionTaskBuilder[PartitionT]:
        """Append an instruction to this PartitionTask's pipeline.

        This triggers a re-calculation of
        """
        # Update the resource requests as well as the current estimated partition metadata at the current state of the PartitionTaskBuilder's "stack"
        estimated_output_metadata = instruction.estimate_output_metadata(self.estimated_output_partition_metadatas)
        current_instruction_resource_request = instruction.estimate_resource_request(
            self.estimated_output_partition_metadatas
        )

        self.resource_request = ResourceRequest.max_resources(
            [self.resource_request, current_instruction_resource_request]
        )
        self.estimated_output_partition_metadatas = estimated_output_metadata

        self.instructions.append(instruction)
        self.num_results = instruction.num_outputs()

        return self

    def is_empty(self) -> bool:
        """Whether this partition task is guaranteed to result in an empty partition."""
        # How can we ever know that this task is guaranteed to result in an empty partition through pure estimations?
        return False

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
            partial_metadatas=self.estimated_output_partition_metadatas,
            actor_pool_id=self.actor_pool_id,
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
            partial_metadatas=self.estimated_output_partition_metadatas,
            actor_pool_id=self.actor_pool_id,
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

    def partition_metadata(self) -> ExactPartitionMetadata:
        """Get the metadata of the result partition.

        (Avoids retrieving the actual partition itself if possible.)
        """
        [partial_metadata] = self.partial_metadatas
        return self.result().metadata().merge_with_partial(partial_metadata)

    def micropartition(self) -> MicroPartition:
        """Get the raw vPartition of the result."""
        return self.result().micropartition()

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

    def cancel(self) -> None:
        if self._results is not None:
            for result in self._results:
                result.cancel()

    def partitions(self) -> list[PartitionT]:
        """Get the PartitionTs resulting from running this PartitionTask."""
        assert self._results is not None
        return [result.partition() for result in self._results]

    def partition_metadatas(self) -> list[ExactPartitionMetadata]:
        """Get the metadata of the result partitions.

        (Avoids retrieving the actual partition itself if possible.)
        """
        assert self._results is not None
        return [
            result.metadata().merge_with_partial(partial_metadata)
            for result, partial_metadata in zip(self._results, self.partial_metadatas)
        ]

    def micropartition(self, index: int) -> MicroPartition:
        """Get the raw vPartition of the result."""
        assert self._results is not None
        return self._results[index].micropartition()

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

    def estimate_resource_request(self, input_metadatas: list[EstimatedPartitionMetadata]) -> ResourceRequest:
        """Estimates the ResourceRequest required to run this instruction based on the estimated metadata of the inputs

        A default implementation here is provided which provides reasonable fallbacks, but Instructions should override this
        to provide better estimates based on the semantics of each Instruction.
        """
        assert (
            len(input_metadatas) == 1
        ), "Default impl of estimate_resource_request only works for Instructions with 1 input"

        # Fallback to 512MB of input/output sizes if no estimation can be provided, at least giving a naive baseline
        estimated_input_data_bytes = input_metadatas[0].size_bytes
        estimated_output_data_bytes = self.estimate_output_metadata(input_metadatas=input_metadatas)[0].size_bytes
        memory_bytes = (
            estimated_input_data_bytes
            + (estimated_input_data_bytes * 2)  # Assume 2x input size required for heap memory allocs
            + estimated_output_data_bytes
        )

        return ResourceRequest(memory_bytes=memory_bytes)

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        """Estimates output metadata for this instruction."""
        ...

    def num_outputs(self) -> int:
        """How many partitions will result from running this instruction."""
        ...


class SingleOutputInstruction(Instruction):
    def num_outputs(self) -> int:
        return 1


@dataclass(frozen=True)
class ScanWithTask(SingleOutputInstruction):
    FALLBACK_PARTITION_NUM_ROWS: ClassVar[int] = 131_072
    FALLBACK_PARTITION_SIZE_BYTES: ClassVar[int] = 512 * 1024 * 1024

    scan_task: ScanTask
    resource_request: ResourceRequest

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._scan(inputs)

    def _scan(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        assert len(inputs) == 0
        table = MicroPartition._from_scan_task(self.scan_task)
        return [table]

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        # A scan is a leaf task, and shouldn't have any inputs
        assert len(input_metadatas) == 0

        cfg = get_context().daft_execution_config
        approx_num_rows = self.scan_task.approx_num_rows(cfg)
        estimated_output_metadata = [
            EstimatedPartitionMetadata(
                num_rows=int(approx_num_rows)
                if approx_num_rows is not None
                else ScanWithTask.FALLBACK_PARTITION_NUM_ROWS,
                size_bytes=self.scan_task.estimate_in_memory_size_bytes(cfg)
                or ScanWithTask.FALLBACK_PARTITION_SIZE_BYTES,
            )
        ]

        return estimated_output_metadata


@dataclass(frozen=True)
class EmptyScan(SingleOutputInstruction):
    schema: Schema

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return [MicroPartition.empty(self.schema)]

    def estimate_resource_request(self, input_metadatas: list[EstimatedPartitionMetadata]) -> ResourceRequest:
        return ResourceRequest()

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        assert len(input_metadatas) == 0

        return [
            EstimatedPartitionMetadata(
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

    def estimate_resource_request(self, input_metadatas: list[EstimatedPartitionMetadata]) -> ResourceRequest:
        assert len(input_metadatas) == 1

        estimated_data_to_write_bytes = input_metadatas[0].size_bytes

        # Assume 2x the memory of the output data is required in order to write this data
        return ResourceRequest(memory_bytes=estimated_data_to_write_bytes * 2)

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        assert len(input_metadatas) == 1
        return [
            EstimatedPartitionMetadata(
                num_rows=1,  # Assume just 1 file written per partition
                size_bytes=1000,  # Assume each filepath is <=1KB
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
    partition_spec: IcebergPartitionSpec
    io_config: IOConfig | None

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._write_iceberg(inputs)

    def _write_iceberg(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        partition = self._handle_file_write(
            input=input,
        )
        return [partition]

    def estimate_resource_request(self, input_metadatas: list[EstimatedPartitionMetadata]) -> ResourceRequest:
        assert len(input_metadatas) == 1

        estimated_data_to_write_bytes = input_metadatas[0].size_bytes

        # Assume 2x the memory of the output data is required in order to write this data
        return ResourceRequest(memory_bytes=estimated_data_to_write_bytes * 2)

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        assert len(input_metadatas) == 1
        return [
            EstimatedPartitionMetadata(
                num_rows=1,  # Assume just 1 file written per partition
                size_bytes=1000,  # Assume each filepath is <=1KB
            )
        ]

    def _handle_file_write(self, input: MicroPartition) -> MicroPartition:
        return table_io.write_iceberg(
            input,
            base_path=self.base_path,
            schema=self.iceberg_schema,
            properties=self.iceberg_properties,
            partition_spec=self.partition_spec,
            io_config=self.io_config,
        )


@dataclass(frozen=True)
class WriteDeltaLake(SingleOutputInstruction):
    base_path: str
    large_dtypes: bool
    version: int
    partition_cols: list[str] | None
    io_config: IOConfig | None

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._write_deltalake(inputs)

    def _write_deltalake(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        partition = self._handle_file_write(
            input=input,
        )
        return [partition]

    def estimate_resource_request(self, input_metadatas: list[EstimatedPartitionMetadata]) -> ResourceRequest:
        assert len(input_metadatas) == 1

        estimated_data_to_write_bytes = input_metadatas[0].size_bytes

        # Assume 2x the memory of the output data is required in order to write this data
        return ResourceRequest(memory_bytes=estimated_data_to_write_bytes * 2)

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        assert len(input_metadatas) == 1
        return [
            EstimatedPartitionMetadata(
                num_rows=1,  # Assume just 1 file written per partition
                size_bytes=1000,  # Assume each filepath is <=1KB
            )
        ]

    def _handle_file_write(self, input: MicroPartition) -> MicroPartition:
        return table_io.write_deltalake(
            input,
            large_dtypes=self.large_dtypes,
            base_path=self.base_path,
            version=self.version,
            partition_cols=self.partition_cols,
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

    def estimate_resource_request(self, input_metadatas: list[EstimatedPartitionMetadata]) -> ResourceRequest:
        assert len(input_metadatas) == 1

        estimated_data_to_write_bytes = input_metadatas[0].size_bytes

        # Assume 2x the memory of the output data is required in order to write this data
        return ResourceRequest(memory_bytes=estimated_data_to_write_bytes * 2)

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        assert len(input_metadatas) == 1
        return [
            EstimatedPartitionMetadata(
                num_rows=1,  # Assume just 1 file written per partition
                size_bytes=1000,  # Assume each filepath is <=1KB
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
    SELECTIVITY: ClassVar[float] = 0.2

    predicate: ExpressionsProjection

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._filter(inputs)

    def _filter(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        return [input.filter(self.predicate)]

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        [input_meta] = input_metadatas
        return [
            EstimatedPartitionMetadata(
                num_rows=int(input_meta.num_rows * Filter.SELECTIVITY),
                size_bytes=int(input_meta.size_bytes * Filter.SELECTIVITY),
                boundaries=input_meta.boundaries,
            )
        ]


@dataclass(frozen=True)
class Project(SingleOutputInstruction):
    # TODO: Use information about each expression to arrive at a better estimate.
    # Maybe we can just use the schema as a good heuristic as well.
    # For example, if there is a url download or image decode, we can use that to arrive at better estimates
    POST_PROJECT_INFLATION_FACTOR: ClassVar[float] = 1.5

    projection: ExpressionsProjection
    resource_request: ResourceRequest | None = None

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._project(inputs)

    def _project(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        return [input.eval_expression_list(self.projection)]

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        [input_meta] = input_metadatas
        boundaries = input_meta.boundaries
        if boundaries is not None:
            boundaries = _prune_boundaries(boundaries, self.projection)

        output_size_bytes = int(input_meta.size_bytes * Project.POST_PROJECT_INFLATION_FACTOR)

        return [
            EstimatedPartitionMetadata(
                num_rows=input_meta.num_rows,
                size_bytes=output_size_bytes,
                boundaries=boundaries,
            )
        ]


@dataclass(frozen=True)
class StatefulUDFProject(SingleOutputInstruction):
    # TODO: Use information about each expression to arrive at a better estimate.
    # Maybe we can just use the schema as a good heuristic as well.
    # For example, if there is a url download or image decode, we can use that to arrive at better estimates
    POST_PROJECT_INFLATION_FACTOR: ClassVar[float] = 1.0

    projection: ExpressionsProjection
    task_resource_request: ResourceRequest

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        raise NotImplementedError("UDFProject instruction cannot be run from outside an Actor. Please file an issue.")

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        [input_meta] = input_metadatas

        output_size_bytes = int(input_meta.size_bytes * StatefulUDFProject.POST_PROJECT_INFLATION_FACTOR)

        return [
            EstimatedPartitionMetadata(
                num_rows=input_meta.num_rows,
                size_bytes=output_size_bytes,
                boundaries=None,
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

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        return [
            EstimatedPartitionMetadata(
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

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        [input_meta] = input_metadatas

        new_num_rows = min(self.limit, input_meta.num_rows)
        selectivity = new_num_rows / input_meta.num_rows

        # Assume a selectivity of 10% of the filter if unable to use the limit to calculate selectivity
        new_size_bytes = int(selectivity * input_meta.size_bytes)

        return [
            EstimatedPartitionMetadata(
                num_rows=new_num_rows,
                size_bytes=new_size_bytes,
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

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        return self.map_op.estimate_output_metadata(input_metadatas)


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

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        assert len(input_metadatas) == 1
        [input_meta] = input_metadatas

        if self.size is not None:
            fraction = self.size / input_meta.num_rows
            return [
                EstimatedPartitionMetadata(
                    num_rows=self.size,
                    size_bytes=int(fraction * input_meta.size_bytes),
                )
            ]
        elif self.fraction is not None:
            return [
                EstimatedPartitionMetadata(
                    num_rows=int(self.fraction * input_meta.num_rows),
                    size_bytes=int(self.fraction * input_meta.size_bytes),
                )
            ]
        assert False, f"Unrecognized sampling instruction: {self}"


@dataclass(frozen=True)
class MonotonicallyIncreasingId(SingleOutputInstruction):
    partition_num: int
    column_name: str

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        result = input.add_monotonically_increasing_id(self.partition_num, self.column_name)
        return [result]

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        [input_meta] = input_metadatas

        added_column_size_bytes = input_meta.num_rows * 8  # 8 bytes per uint64
        return [
            EstimatedPartitionMetadata(
                num_rows=input_meta.num_rows,
                size_bytes=(input_meta.size_bytes + added_column_size_bytes),
            )
        ]


@dataclass(frozen=True)
class Aggregate(SingleOutputInstruction):
    GROUPBY_CARDINALITY_REDUCTION_FACTOR: ClassVar[float] = 0.2

    to_agg: list[Expression]
    group_by: ExpressionsProjection | None

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        return self._aggregate(inputs)

    def _aggregate(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        return [input.agg(self.to_agg, self.group_by)]

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        assert len(input_metadatas) == 1
        [input_meta] = input_metadatas

        if self.group_by is None:
            return [
                EstimatedPartitionMetadata(
                    num_rows=1,
                    size_bytes=input_meta.size_bytes // input_meta.num_rows,
                )
            ]

        # Assume a reduction of cardinality of about 80% in the groupby
        return [
            EstimatedPartitionMetadata(
                num_rows=int(input_meta.num_rows * Aggregate.GROUPBY_CARDINALITY_REDUCTION_FACTOR),
                size_bytes=int(
                    input_meta.size_bytes * Aggregate.GROUPBY_CARDINALITY_REDUCTION_FACTOR
                ),  # NOTE: This might be naive and doesn't take into account types
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

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        return [
            EstimatedPartitionMetadata(
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

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        [input_meta] = input_metadatas
        return [
            EstimatedPartitionMetadata(
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

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        # TODO: Incorporate this logic into ResourceRequest estimation
        # # Calculate memory request for task.
        # left_size_bytes = next_left.partition_metadata().size_bytes
        # right_size_bytes = next_right.partition_metadata().size_bytes
        # if left_size_bytes is None and right_size_bytes is None:
        #     size_bytes = None
        # elif left_size_bytes is None and right_size_bytes is not None:
        #     # Use 2x the right side as the memory request, assuming that left and right side are ~ the same size.
        #     size_bytes = 2 * right_size_bytes
        # elif right_size_bytes is None and left_size_bytes is not None:
        #     # Use 2x the left side as the memory request, assuming that left and right side are ~ the same size.
        #     size_bytes = 2 * left_size_bytes
        # elif left_size_bytes is not None and right_size_bytes is not None:
        #     size_bytes = left_size_bytes + right_size_bytes

        # Can't derive anything.
        return [
            EstimatedPartitionMetadata(
                num_rows=None,
                size_bytes=None,
            )
        ]


@dataclass(frozen=True)
class BroadcastJoin(HashJoin):
    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        # TODO: Incorporate this logic into ResourceRequest estimation
        # # Calculate memory request for task.
        # broadcaster_size_bytes_ = 0
        # broadcaster_partitions = []
        # broadcaster_partition_metadatas = []
        # null_count = 0
        # for next_broadcaster in broadcaster_parts:
        #     next_broadcaster_partition_metadata = next_broadcaster.partition_metadata()
        #     if next_broadcaster_partition_metadata is None or next_broadcaster_partition_metadata.size_bytes is None:
        #         null_count += 1
        #     else:
        #         broadcaster_size_bytes_ += next_broadcaster_partition_metadata.size_bytes
        #     broadcaster_partitions.append(next_broadcaster.partition())
        #     broadcaster_partition_metadatas.append(next_broadcaster_partition_metadata)
        # if null_count == len(broadcaster_parts):
        #     broadcaster_size_bytes = None
        # elif null_count > 0:
        #     # Impute null size estimates with mean of non-null estimates.
        #     broadcaster_size_bytes = broadcaster_size_bytes_ + math.ceil(
        #         null_count * broadcaster_size_bytes_ / (len(broadcaster_parts) - null_count)
        #     )
        # else:
        #     broadcaster_size_bytes = broadcaster_size_bytes_
        # receiver_size_bytes = receiver_part.partition_metadata().size_bytes
        # if broadcaster_size_bytes is None and receiver_size_bytes is None:
        #     size_bytes = None
        # elif broadcaster_size_bytes is None and receiver_size_bytes is not None:
        #     # Use 1.25x the receiver side as the memory request, assuming that receiver side is ~4x larger than the broadcaster side.
        #     size_bytes = int(1.25 * receiver_size_bytes)
        # elif receiver_size_bytes is None and broadcaster_size_bytes is not None:
        #     # Use 4x the broadcaster side as the memory request, assuming that receiver side is ~4x larger than the broadcaster side.
        #     size_bytes = 4 * broadcaster_size_bytes
        # elif broadcaster_size_bytes is not None and receiver_size_bytes is not None:
        #     size_bytes = broadcaster_size_bytes + receiver_size_bytes
        return [
            EstimatedPartitionMetadata(
                num_rows=None,
                size_bytes=None,
            )
        ]


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

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        # TODO: Use logic for ResourceRequest estimations
        # def _memory_bytes_for_merge(
        #     next_left: SingleOutputPartitionTask[PartitionT], next_right: SingleOutputPartitionTask[PartitionT]
        # ) -> int | None:
        #     # Calculate memory request for merge task.
        #     left_size_bytes = next_left.partition_metadata().size_bytes
        #     right_size_bytes = next_right.partition_metadata().size_bytes
        #     if left_size_bytes is None and right_size_bytes is None:
        #         size_bytes = None
        #     elif left_size_bytes is None and right_size_bytes is not None:
        #         # Use 2x the right side as the memory request, assuming that left and right side are ~ the same size.
        #         size_bytes = 2 * right_size_bytes
        #     elif right_size_bytes is None and left_size_bytes is not None:
        #         # Use 2x the left side as the memory request, assuming that left and right side are ~ the same size.
        #         size_bytes = 2 * left_size_bytes
        #     elif left_size_bytes is not None and right_size_bytes is not None:
        #         size_bytes = left_size_bytes + right_size_bytes
        #     return size_bytes

        [left_meta, right_meta] = input_metadatas
        # If the boundaries of the left and right partitions don't intersect, then the merge-join will result in an empty partition.
        if left_meta.boundaries is None or right_meta.boundaries is None:
            is_nonempty = True
        else:
            is_nonempty = left_meta.boundaries.intersects(right_meta.boundaries)
        return [
            EstimatedPartitionMetadata(
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

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        # TODO: Use this to estimate the ResourceRequest
        # def _memory_bytes_for_coalesce(input_parts: Iterable[SingleOutputPartitionTask[PartitionT]]) -> int | None:
        #     # Calculate memory request for task.
        #     size_bytes_per_task = [task.partition_metadata().size_bytes for task in input_parts]
        #     non_null_size_bytes_per_task = [size for size in size_bytes_per_task if size is not None]
        #     non_null_size_bytes = sum(non_null_size_bytes_per_task)
        #     if len(size_bytes_per_task) == len(non_null_size_bytes_per_task):
        #         # If all task size bytes are non-null, directly use the non-null size bytes sum.
        #         size_bytes = non_null_size_bytes
        #     elif non_null_size_bytes_per_task:
        #         # If some are null, calculate the non-null mean and assume that null task size bytes
        #         # have that size.
        #         mean_size = math.ceil(non_null_size_bytes / len(non_null_size_bytes_per_task))
        #         size_bytes = non_null_size_bytes + mean_size * (len(size_bytes_per_task) - len(non_null_size_bytes_per_task))
        #     else:
        #         # If all null, set to null.
        #         size_bytes = None
        #     return size_bytes

        input_rows = [_.num_rows for _ in input_metadatas]
        input_sizes = [_.size_bytes for _ in input_metadatas]
        return [
            EstimatedPartitionMetadata(
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

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        input_rows = [_.num_rows for _ in input_metadatas]
        input_sizes = [_.size_bytes for _ in input_metadatas]
        return [
            EstimatedPartitionMetadata(
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

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        return [
            EstimatedPartitionMetadata(
                num_rows=self.num_quantiles,
                size_bytes=None,
            )
        ]


@dataclass(frozen=True)
class FanoutInstruction(Instruction):
    _num_outputs: int

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
        # Can't derive anything.
        return [
            EstimatedPartitionMetadata(
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

    def estimate_output_metadata(
        self, input_metadatas: list[EstimatedPartitionMetadata]
    ) -> list[EstimatedPartitionMetadata]:
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
                EstimatedPartitionMetadata(
                    num_rows=num_rows,
                    size_bytes=None,
                    boundaries=input_meta.boundaries,
                )
            )

        return results


@dataclass(frozen=True)
class FanoutEvenSlices(FanoutInstruction):
    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        [input] = inputs
        results = []

        input_length = len(input)
        num_outputs = self.num_outputs()

        chunk_size, remainder = divmod(input_length, num_outputs)
        ptr = 0
        for output_idx in range(self.num_outputs()):
            end = ptr + chunk_size + 1 if output_idx < remainder else ptr + chunk_size
            results.append(input.slice(ptr, end))
            ptr = end
        assert ptr == input_length

        return results

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        # TODO: Derive this based on the ratios of num rows
        return [
            PartialPartitionMetadata(
                num_rows=None,
                size_bytes=None,
            )
            for _ in range(self._num_outputs)
        ]
