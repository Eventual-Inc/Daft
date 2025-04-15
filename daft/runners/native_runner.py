from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Iterator

from daft.context import get_context
from daft.daft import (
    FileFormatConfig,
    FileInfos,
    IOConfig,
    PyDaftExecutionConfig,
    set_compute_runtime_num_worker_threads,
)
from daft.daft import (
    NativeExecutor as _NativeExecutor,
)
from daft.dataframe.display import MermaidOptions
from daft.filesystem import glob_path_with_stats
from daft.recordbatch import MicroPartition
from daft.runners import runner_io
from daft.runners.partitioning import (
    LocalMaterializedResult,
    LocalPartitionSet,
    MaterializedResult,
    PartitionCacheEntry,
    PartitionSetCache,
    PartitionT,
)
from daft.runners.runner import LOCAL_PARTITION_SET_CACHE, Runner
from daft.scarf_telemetry import track_runner_on_scarf

if TYPE_CHECKING:
    from daft.logical.builder import LogicalPlanBuilder

logger = logging.getLogger(__name__)


class NativeExecutor:
    def __init__(self):
        self._executor = _NativeExecutor()

    def run(
        self,
        builder: LogicalPlanBuilder,
        psets: dict[str, list[MaterializedResult[PartitionT]]],
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: int | None,
    ) -> Iterator[LocalMaterializedResult]:
        from daft.runners.partitioning import LocalMaterializedResult

        psets_mp = {
            part_id: [part.micropartition()._micropartition for part in parts] for part_id, parts in psets.items()
        }
        return (
            LocalMaterializedResult(MicroPartition._from_pymicropartition(part))
            for part in self._executor.run(builder._builder, psets_mp, daft_execution_config, results_buffer_size)
        )

    def pretty_print(
        self,
        builder: LogicalPlanBuilder,
        daft_execution_config: PyDaftExecutionConfig,
        simple: bool = False,
        format: str = "ascii",
    ) -> str:
        """Pretty prints the current underlying logical plan."""
        if format == "ascii":
            return self._executor.repr_ascii(builder._builder, daft_execution_config, simple)
        elif format == "mermaid":
            return self._executor.repr_mermaid(builder._builder, daft_execution_config, MermaidOptions(simple))
        else:
            raise ValueError(f"Unknown format: {format}")


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
    name = "native"

    def __init__(self, num_threads: int | None = None) -> None:
        super().__init__()
        if num_threads is not None:
            set_compute_runtime_num_worker_threads(num_threads)

    def initialize_partition_set_cache(self) -> PartitionSetCache:
        return LOCAL_PARTITION_SET_CACHE

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
    ) -> Iterator[LocalMaterializedResult]:
        track_runner_on_scarf(runner=self.name)

        # NOTE: Freeze and use this same execution config for the entire execution
        daft_execution_config = get_context().daft_execution_config

        # Optimize the logical plan.
        builder = builder.optimize()
        executor = NativeExecutor()
        results_gen = executor.run(
            builder,
            {k: v.values() for k, v in self._part_set_cache.get_all_partition_sets().items()},
            daft_execution_config,
            results_buffer_size,
        )
        yield from results_gen

    def run_iter_tables(
        self, builder: LogicalPlanBuilder, results_buffer_size: int | None = None
    ) -> Iterator[MicroPartition]:
        for result in self.run_iter(builder, results_buffer_size=results_buffer_size):
            yield result.partition()
