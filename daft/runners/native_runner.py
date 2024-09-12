from __future__ import annotations

import contextlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Iterator

from daft.context import get_context
from daft.daft import (
    FileFormatConfig,
    FileInfos,
    IOConfig,
    NativeExecutor,
    PyLocalPhysicalPlan,
    PySinkType,
    ResourceRequest,
)
from daft.expressions.expressions import Expression, ExpressionsProjection
from daft.filesystem import glob_path_with_stats
from daft.logical.builder import LogicalPlanBuilder
from daft.logical.schema import Schema
from daft.runners import runner_io
from daft.runners.partitioning import (
    MaterializedResult,
    PartitionCacheEntry,
    PartitionMetadata,
)
from daft.runners.pyrunner import LocalPartitionSet
from daft.runners.runner import Runner
from daft.table import MicroPartition
from daft.table.table_io import write_tabular, write_tabular_from_iter

if TYPE_CHECKING:
    import pyarrow as pa


class NativeRunnerIO(runner_io.RunnerIO):
    def glob_paths_details(
        self,
        source_paths: list[str],
        file_format_config: FileFormatConfig | None = None,
        io_config: IOConfig | None = None,
    ) -> FileInfos:
        file_infos = FileInfos()
        file_format = file_format_config.file_format() if file_format_config is not None else None
        for source_path in source_paths:
            path_file_infos = glob_path_with_stats(source_path, file_format, io_config)

            if len(path_file_infos) == 0:
                raise FileNotFoundError(f"No files found at {source_path}")

            file_infos.extend(path_file_infos)

        return file_infos


class NativeRunner(Runner[MicroPartition]):
    def __init__(self) -> None:
        super().__init__()

    def runner_io(self) -> NativeRunnerIO:
        return NativeRunnerIO()

    def run(self, builder: LogicalPlanBuilder) -> PartitionCacheEntry:
        results = list(self.run_iter(builder))

        result_pset = LocalPartitionSet()
        for i, result in enumerate(results):
            result_pset.set_partition(i, result)

        pset_entry = self.put_partition_set_into_cache(result_pset)
        return pset_entry

    def run_iter(
        self,
        builder: LogicalPlanBuilder,
        results_buffer_size: int | None = None,
    ) -> Iterator[NativeMaterializedResult]:
        daft_execution_config = get_context().daft_execution_config
        builder = builder.optimize()

        local_physical_plan = PyLocalPhysicalPlan.from_logical_plan_builder(builder._builder)
        executor = NativeExecutor.from_local_physical_plan(local_physical_plan)

        sink_info = local_physical_plan.sink_info
        psets_mp = {
            part_id: [part.micropartition()._micropartition for part in parts.values()]
            for part_id, parts in self._part_set_cache.get_all_partition_sets().items()
        }
        if sink_info.sink_type == PySinkType.InMemory:
            yield from (
                NativeMaterializedResult(MicroPartition._from_pymicropartition(part))
                for part in executor.run(psets_mp, daft_execution_config, results_buffer_size)
            )
        elif sink_info.sink_type == PySinkType.FileWrite:
            schema = local_physical_plan.schema
            file_info = sink_info.file_info
            assert file_info is not None

            if file_info.partition_cols is not None:
                concated = MicroPartition.concat(
                    [
                        MicroPartition._from_pymicropartition(part)
                        for part in executor.run(psets_mp, daft_execution_config, results_buffer_size)
                    ]
                )
                write_result = write_tabular(
                    concated,
                    file_info.file_format,
                    file_info.root_dir,
                    Schema._from_pyschema(sink_info.file_schema),
                    ExpressionsProjection([Expression._from_pyexpr(pyexpr) for pyexpr in file_info.partition_cols]),
                    file_info.compression,
                    file_info.io_config,
                )
            else:

                def to_arrow_iter(
                    executor, psets_mp, daft_execution_config, results_buffer_size
                ) -> Iterable[pa.RecordBatch]:
                    for part in executor.run(psets_mp, daft_execution_config, results_buffer_size):
                        yield from MicroPartition._from_pymicropartition(part).to_arrow().to_batches()

                # print type of arrow_iter
                write_result = write_tabular_from_iter(
                    to_arrow_iter(executor, psets_mp, daft_execution_config, results_buffer_size),
                    file_info.file_format,
                    file_info.root_dir,
                    Schema._from_pyschema(schema),
                    file_info.compression,
                    file_info.io_config,
                )
                print(write_result)

            yield from [NativeMaterializedResult(write_result)]

    def run_iter_tables(
        self, builder: LogicalPlanBuilder, results_buffer_size: int | None = None
    ) -> Iterator[MicroPartition]:
        for result in self.run_iter(builder, results_buffer_size=results_buffer_size):
            yield result.partition()

    @contextlib.contextmanager
    def actor_pool_context(
        self,
        name: str,
        resource_request: ResourceRequest,
        num_actors: int,
        projection: ExpressionsProjection,
    ) -> Iterator[str]:
        raise NotImplementedError("Actor pools are not supported in the native runner")


@dataclass
class NativeMaterializedResult(MaterializedResult[MicroPartition]):
    _partition: MicroPartition
    _metadata: PartitionMetadata | None = None

    def partition(self) -> MicroPartition:
        return self._partition

    def micropartition(self) -> MicroPartition:
        return self._partition

    def metadata(self) -> PartitionMetadata:
        if self._metadata is None:
            self._metadata = PartitionMetadata.from_table(self._partition)
        return self._metadata

    def cancel(self) -> None:
        return None

    def _noop(self, _: MicroPartition) -> None:
        return None
