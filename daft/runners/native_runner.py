from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Iterator

from daft.context import get_context
from daft.daft import FileFormatConfig, FileInfos, IOConfig
from daft.execution.native_executor import NativeExecutor
from daft.filesystem import glob_path_with_stats
from daft.runners import runner_io
from daft.runners.partitioning import (
    LocalMaterializedResult,
    LocalPartitionSet,
    PartitionCacheEntry,
    PartitionSetCache,
)
from daft.runners.runner import LOCAL_PARTITION_SET_CACHE, Runner
from daft.table import MicroPartition

if TYPE_CHECKING:
    from daft.logical.builder import LogicalPlanBuilder

logger = logging.getLogger(__name__)


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

    def __init__(self) -> None:
        super().__init__()

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
