from __future__ import annotations

import itertools
import pathlib
import sys
from dataclasses import dataclass, field
from typing import Generic, TypeVar

if sys.version_info < (3, 8):
    from typing_extensions import Protocol
else:
    from typing import Protocol

from daft.daft import (
    CsvSourceConfig,
    FileFormat,
    FileFormatConfig,
    JoinType,
    JsonSourceConfig,
    ParquetSourceConfig,
    ResourceRequest,
    StorageConfig,
)
from daft.expressions import Expression, ExpressionsProjection, col
from daft.logical.map_partition_ops import MapPartitionOp
from daft.logical.schema import Schema
from daft.runners.partitioning import (
    PartialPartitionMetadata,
    PartitionMetadata,
    TableParseCSVOptions,
    TableReadOptions,
)
from daft.table import Table, table_io

PartitionT = TypeVar("PartitionT")
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

    def __str__(self) -> str:
        return (
            f"{self.id()}\n"
            f"  Inputs: {self.inputs}\n"
            f"  Resource Request: {self.resource_request}\n"
            f"  Instructions: {[i.__class__.__name__ for i in self.instructions]}"
        )

    def __repr__(self) -> str:
        return self.__str__()


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

    def finalize_partition_task_single_output(self) -> SingleOutputPartitionTask[PartitionT]:
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
            instructions=self.instructions,
            num_results=1,
            resource_request=resource_request_final_cpu,
        )

    def finalize_partition_task_multi_output(self) -> MultiOutputPartitionTask[PartitionT]:
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
            instructions=self.instructions,
            num_results=self.num_results,
            resource_request=resource_request_final_cpu,
        )

    def __str__(self) -> str:
        return (
            f"PartitionTaskBuilder\n"
            f"  Inputs: {self.inputs}\n"
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

    def cancel(self) -> None:
        # Currently only implemented for Ray tasks.
        if self._result is not None:
            self._result.cancel()

    def partition(self) -> PartitionT:
        """Get the PartitionT resulting from running this PartitionTask."""
        assert self._result is not None
        return self._result.partition()

    def partition_metadata(self) -> PartitionMetadata:
        """Get the metadata of the result partition.

        (Avoids retrieving the actual partition itself if possible.)
        """
        assert self._result is not None
        return self._result.metadata()

    def vpartition(self) -> Table:
        """Get the raw vPartition of the result."""
        assert self._result is not None
        return self._result.vpartition()

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
        return [result.metadata() for result in self._results]

    def vpartition(self, index: int) -> Table:
        """Get the raw vPartition of the result."""
        assert self._results is not None
        return self._results[index].vpartition()

    def __str__(self) -> str:
        return super().__str__()

    def __repr__(self) -> str:
        return super().__str__()


class MaterializedResult(Protocol[PartitionT]):
    """A protocol for accessing the result partition of a PartitionTask.

    Different Runners can fill in their own implementation here.
    """

    def partition(self) -> PartitionT:
        """Get the partition of this result."""
        ...

    def vpartition(self) -> Table:
        """Get the vPartition of this result."""
        ...

    def metadata(self) -> PartitionMetadata:
        """Get the metadata of the partition in this result."""
        ...

    def cancel(self) -> None:
        """If possible, cancel execution of this PartitionTask."""
        ...

    def _noop(self, _: PartitionT) -> None:
        """Implement this as a no-op.
        https://peps.python.org/pep-0544/#overriding-inferred-variance-of-protocol-classes
        """
        ...


class Instruction(Protocol):
    """An instruction is a function to run over a list of partitions.

    Most instructions take one partition and return another partition.
    However, some instructions take one partition and return many partitions (fanouts),
    and others take many partitions and return one partition (reduces).
    To accomodate these, instructions are typed as list[Table] -> list[Table].
    """

    def run(self, inputs: list[Table]) -> list[Table]:
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
class ReadFile(SingleOutputInstruction):
    index: int | None
    # Known number of rows.
    file_rows: int | None
    # Max number of rows to read.
    limit_rows: int | None
    schema: Schema
    storage_config: StorageConfig
    columns_to_read: list[str] | None
    file_format_config: FileFormatConfig

    def run(self, inputs: list[Table]) -> list[Table]:
        return self._read_file(inputs)

    def _read_file(self, inputs: list[Table]) -> list[Table]:
        assert len(inputs) == 1
        [filepaths_partition] = inputs
        partition = self._handle_tabular_files_scan(
            filepaths_partition=filepaths_partition,
        )
        return [partition]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        assert len(input_metadatas) == 1

        num_rows = self.file_rows
        # Only take the file read limit into account if we know how big the file is to begin with.
        if num_rows is not None and self.limit_rows is not None:
            num_rows = min(num_rows, self.limit_rows)

        return [
            PartialPartitionMetadata(
                num_rows=num_rows,
                size_bytes=None,
            )
        ]

    def _handle_tabular_files_scan(
        self,
        filepaths_partition: Table,
    ) -> Table:
        data = filepaths_partition.to_pydict()
        filepaths = data["path"]

        if self.index is not None:
            filepaths = [filepaths[self.index]]

        # Common options for reading vPartition
        read_options = TableReadOptions(
            num_rows=self.limit_rows,
            column_names=self.columns_to_read,  # read only specified columns
        )

        file_format = self.file_format_config.file_format()
        format_config = self.file_format_config.config
        if file_format == FileFormat.Csv:
            assert isinstance(format_config, CsvSourceConfig)
            table = Table.concat(
                [
                    table_io.read_csv(
                        file=fp,
                        schema=self.schema,
                        storage_config=self.storage_config,
                        csv_options=TableParseCSVOptions(
                            delimiter=format_config.delimiter,
                            header_index=0 if format_config.has_headers else None,
                        ),
                        read_options=read_options,
                    )
                    for fp in filepaths
                ]
            )
        elif file_format == FileFormat.Json:
            assert isinstance(format_config, JsonSourceConfig)
            table = Table.concat(
                [
                    table_io.read_json(
                        file=fp,
                        schema=self.schema,
                        storage_config=self.storage_config,
                        read_options=read_options,
                    )
                    for fp in filepaths
                ]
            )
        elif file_format == FileFormat.Parquet:
            assert isinstance(format_config, ParquetSourceConfig)
            table = Table.concat(
                [
                    table_io.read_parquet(
                        file=fp,
                        schema=self.schema,
                        storage_config=self.storage_config,
                        read_options=read_options,
                    )
                    for fp in filepaths
                ]
            )
        else:
            raise NotImplementedError(f"PyRunner has not implemented scan: {file_format}")

        expected_schema = (
            Schema._from_fields([self.schema[name] for name in read_options.column_names])
            if read_options.column_names is not None
            else self.schema
        )
        assert (
            table.schema() == expected_schema
        ), f"Expected table to have schema:\n{expected_schema}\n\nReceived instead:\n{table.schema()}"
        return table


@dataclass(frozen=True)
class WriteFile(SingleOutputInstruction):
    file_format: FileFormat
    schema: Schema
    root_dir: str | pathlib.Path
    compression: str | None
    partition_cols: ExpressionsProjection | None

    def run(self, inputs: list[Table]) -> list[Table]:
        return self._write_file(inputs)

    def _write_file(self, inputs: list[Table]) -> list[Table]:
        [input] = inputs
        partition = self._handle_file_write(
            input=input,
        )
        return [partition]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        assert len(input_metadatas) == 1
        return [
            PartialPartitionMetadata(
                num_rows=1,  # We currently write one file per partition.
                size_bytes=None,
            )
        ]

    def _handle_file_write(self, input: Table) -> Table:
        if self.file_format == FileFormat.Parquet:
            file_names = table_io.write_parquet(
                input,
                path=self.root_dir,
                compression=self.compression,
                partition_cols=self.partition_cols,
            )
        elif self.file_format == FileFormat.Csv:
            file_names = table_io.write_csv(
                input,
                path=self.root_dir,
                compression=self.compression,
                partition_cols=self.partition_cols,
            )
        else:
            raise ValueError(
                f"Only Parquet and CSV file formats are supported for writing, but got: {self.file_format}"
            )

        assert len(self.schema) == 1
        return Table.from_pydict(
            {
                self.schema.column_names()[0]: file_names,
            }
        )


@dataclass(frozen=True)
class Filter(SingleOutputInstruction):
    predicate: ExpressionsProjection

    def run(self, inputs: list[Table]) -> list[Table]:
        return self._filter(inputs)

    def _filter(self, inputs: list[Table]) -> list[Table]:
        [input] = inputs
        return [input.filter(self.predicate)]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        # Can't derive anything.
        return [
            PartialPartitionMetadata(
                num_rows=None,
                size_bytes=None,
            )
        ]


@dataclass(frozen=True)
class Project(SingleOutputInstruction):
    projection: ExpressionsProjection

    def run(self, inputs: list[Table]) -> list[Table]:
        return self._project(inputs)

    def _project(self, inputs: list[Table]) -> list[Table]:
        [input] = inputs
        return [input.eval_expression_list(self.projection)]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        [input_meta] = input_metadatas
        return [
            PartialPartitionMetadata(
                num_rows=input_meta.num_rows,
                size_bytes=None,
            )
        ]


@dataclass(frozen=True)
class LocalCount(SingleOutputInstruction):
    schema: Schema

    def run(self, inputs: list[Table]) -> list[Table]:
        return self._count(inputs)

    def _count(self, inputs: list[Table]) -> list[Table]:
        [input] = inputs
        partition = Table.from_pydict({"count": [len(input)]})
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

    def run(self, inputs: list[Table]) -> list[Table]:
        return self._limit(inputs)

    def _limit(self, inputs: list[Table]) -> list[Table]:
        [input] = inputs
        return [input.head(self.limit)]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        [input_meta] = input_metadatas
        return [
            PartialPartitionMetadata(
                num_rows=(min(self.limit, input_meta.num_rows) if input_meta.num_rows is not None else None),
                size_bytes=None,
            )
        ]


@dataclass(frozen=True)
class MapPartition(SingleOutputInstruction):
    map_op: MapPartitionOp

    def run(self, inputs: list[Table]) -> list[Table]:
        return self._map_partition(inputs)

    def _map_partition(self, inputs: list[Table]) -> list[Table]:
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
    sort_by: ExpressionsProjection
    num_samples: int = 20

    def run(self, inputs: list[Table]) -> list[Table]:
        return self._sample(inputs)

    def _sample(self, inputs: list[Table]) -> list[Table]:
        [input] = inputs
        result = (
            input.sample(self.num_samples)
            .eval_expression_list(self.sort_by)
            .filter(ExpressionsProjection([~col(e.name()).is_null() for e in self.sort_by]))
        )
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
class Aggregate(SingleOutputInstruction):
    to_agg: list[Expression]
    group_by: ExpressionsProjection | None

    def run(self, inputs: list[Table]) -> list[Table]:
        return self._aggregate(inputs)

    def _aggregate(self, inputs: list[Table]) -> list[Table]:
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
class Join(SingleOutputInstruction):
    left_on: ExpressionsProjection
    right_on: ExpressionsProjection
    how: JoinType

    def run(self, inputs: list[Table]) -> list[Table]:
        return self._join(inputs)

    def _join(self, inputs: list[Table]) -> list[Table]:
        [left, right] = inputs
        result = left.join(
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


class ReduceInstruction(SingleOutputInstruction):
    ...


@dataclass(frozen=True)
class ReduceMerge(ReduceInstruction):
    def run(self, inputs: list[Table]) -> list[Table]:
        return self._reduce_merge(inputs)

    def _reduce_merge(self, inputs: list[Table]) -> list[Table]:
        return [Table.concat(inputs)]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        input_rows = [_.num_rows for _ in input_metadatas]
        input_sizes = [_.size_bytes for _ in input_metadatas]
        return [
            PartialPartitionMetadata(
                num_rows=sum(input_rows) if all(_ is not None for _ in input_rows) else None,
                size_bytes=sum(input_sizes) if all(_ is not None for _ in input_sizes) else None,
            )
        ]


@dataclass(frozen=True)
class ReduceMergeAndSort(ReduceInstruction):
    sort_by: ExpressionsProjection
    descending: list[bool]

    def run(self, inputs: list[Table]) -> list[Table]:
        return self._reduce_merge_and_sort(inputs)

    def _reduce_merge_and_sort(self, inputs: list[Table]) -> list[Table]:
        partition = Table.concat(inputs).sort(self.sort_by, descending=self.descending)
        return [partition]

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        input_rows = [_.num_rows for _ in input_metadatas]
        input_sizes = [_.size_bytes for _ in input_metadatas]
        return [
            PartialPartitionMetadata(
                num_rows=sum(input_rows) if all(_ is not None for _ in input_rows) else None,
                size_bytes=sum(input_sizes) if all(_ is not None for _ in input_sizes) else None,
            )
        ]


@dataclass(frozen=True)
class ReduceToQuantiles(ReduceInstruction):
    num_quantiles: int
    sort_by: ExpressionsProjection
    descending: list[bool]

    def run(self, inputs: list[Table]) -> list[Table]:
        return self._reduce_to_quantiles(inputs)

    def _reduce_to_quantiles(self, inputs: list[Table]) -> list[Table]:
        merged = Table.concat(inputs)

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

    def run(self, inputs: list[Table]) -> list[Table]:
        return self._fanout_random(inputs)

    def _fanout_random(self, inputs: list[Table]) -> list[Table]:
        [input] = inputs
        return input.partition_by_random(num_partitions=self._num_outputs, seed=self.seed)


@dataclass(frozen=True)
class FanoutHash(FanoutInstruction):
    partition_by: ExpressionsProjection

    def run(self, inputs: list[Table]) -> list[Table]:
        return self._fanout_hash(inputs)

    def _fanout_hash(self, inputs: list[Table]) -> list[Table]:
        [input] = inputs
        return input.partition_by_hash(self.partition_by, num_partitions=self._num_outputs)


@dataclass(frozen=True)
class FanoutRange(FanoutInstruction, Generic[PartitionT]):
    sort_by: ExpressionsProjection
    descending: list[bool]

    def run(self, inputs: list[Table]) -> list[Table]:
        return self._fanout_range(inputs)

    def _fanout_range(self, inputs: list[Table]) -> list[Table]:
        [boundaries, input] = inputs
        if self._num_outputs == 1:
            return [input]
        partitioned_tables = input.partition_by_range(self.sort_by, boundaries, self.descending)

        # Pad the partitioned_tables with empty tables if fewer than self._num_outputs were returned
        # This can happen when all values are null or empty, which leads to an empty `boundaries` input
        assert len(partitioned_tables) >= 1, "Should have at least one returned table"
        schema = partitioned_tables[0].schema()
        partitioned_tables = partitioned_tables + [
            Table.empty(schema=schema) for _ in range(self._num_outputs - len(partitioned_tables))
        ]

        return partitioned_tables


@dataclass(frozen=True)
class FanoutSlices(FanoutInstruction):
    slices: list[tuple[int, int]]  # start inclusive, end exclusive

    def run(self, inputs: list[Table]) -> list[Table]:
        return self._multislice(inputs)

    def _multislice(self, inputs: list[Table]) -> list[Table]:
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
                )
            )

        return results
