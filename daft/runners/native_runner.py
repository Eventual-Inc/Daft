from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING

from daft.context import get_context
from daft.daft import (
    FileFormatConfig,
    FileInfos,
    IOConfig,
    LocalPhysicalPlan,
    PyQueryResult,
    QueryEndState,
    set_compute_runtime_num_worker_threads,
)
from daft.errors import UDFException
from daft.execution.native_executor import NativeExecutor
from daft.filesystem import glob_path_with_stats
from daft.recordbatch import MicroPartition, RecordBatch
from daft.runners import runner_io
from daft.runners.partitioning import (
    LocalMaterializedResult,
    LocalPartitionSet,
    PartitionCacheEntry,
    PartitionSetCache,
)
from daft.runners.runner import LOCAL_PARTITION_SET_CACHE, Runner
from daft.scarf_telemetry import track_runner_on_scarf

if TYPE_CHECKING:
    from collections.abc import Generator, Iterator

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
        for source_path in set(source_paths):
            try:
                path_file_infos = glob_path_with_stats(source_path, file_format, io_config)
                file_infos.merge(path_file_infos)
            except FileNotFoundError:
                logger.debug("%s is not found.", source_path)

        if len(file_infos) == 0:
            raise FileNotFoundError(f"No files found at {','.join(source_paths)}")

        return file_infos


class NativeRunner(Runner[MicroPartition]):
    name = "native"

    def __init__(self, num_threads: int | None = None) -> None:
        super().__init__()
        if num_threads is not None:
            set_compute_runtime_num_worker_threads(num_threads)

        self.native_executor = NativeExecutor()

    def initialize_partition_set_cache(self) -> PartitionSetCache:
        return LOCAL_PARTITION_SET_CACHE

    def runner_io(self) -> NativeRunnerIO:
        return NativeRunnerIO()

    def run(self, builder: LogicalPlanBuilder) -> tuple[PartitionCacheEntry, RecordBatch]:
        results_gen = self.run_iter(builder)
        result_pset = LocalPartitionSet()

        try:
            i = 0
            while True:
                result = next(results_gen)
                result_pset.set_partition(i, result)
                i += 1
        except StopIteration as e:
            metrics = e.value

        pset_entry = self.put_partition_set_into_cache(result_pset)
        return pset_entry, metrics

    def run_iter(
        self,
        builder: LogicalPlanBuilder,
        results_buffer_size: int | None = None,
    ) -> Generator[LocalMaterializedResult, None, RecordBatch]:
        track_runner_on_scarf(runner=self.name)

        # NOTE: Freeze and use this same execution config for the entire execution
        ctx = get_context()
        query_id = str(uuid.uuid4())
        output_schema = builder.schema()

        # Optimize the logical plan.
        import sys

        entrypoint = "python " + " ".join(sys.argv)

        try:
            # Try to send notifications, but don't fail the query if they fail
            from daft.daft import PyQueryMetadata

            ctx._notify_query_start(
                query_id,
                PyQueryMetadata(
                    output_schema._schema,
                    builder.repr_json(),
                    "Native (Swordfish)",
                    ray_dashboard_url=None,
                    entrypoint=entrypoint,
                ),
            )
            ctx._notify_optimization_start(query_id)
        except Exception as e:
            logger.warning("Failed to send notifications: %s", e)
            pass
        builder = builder.optimize(ctx.daft_execution_config)
        try:
            ctx._notify_optimization_end(query_id, builder.repr_json())
        except Exception as e:
            logger.warning("Failed to send optimization end notification: %s", e)
            pass

        plan, inputs = LocalPhysicalPlan.from_logical_plan_builder(builder._builder)
        results_gen = self.native_executor.run(
            plan,
            inputs,
            {
                k: [v.micropartition()._micropartition for v in v.values()]
                for k, v in self._part_set_cache.get_all_partition_sets().items()
            },
            ctx,
            {"query_id": query_id},
        )

        try:
            while True:
                result = next(results_gen)
                try:
                    ctx._notify_result_out(query_id, result.partition())
                except Exception:
                    pass
                yield result
        except StopIteration as e:
            query_result = PyQueryResult(QueryEndState.Finished, "Query finished")
            ctx._notify_query_end(query_id, query_result)
            return e.value
        except KeyboardInterrupt as e:
            query_result = PyQueryResult(QueryEndState.Canceled, "Query canceled by the user.")
            ctx._notify_query_end(query_id, query_result)
            raise e
        except UDFException as e:
            err_msg = f"UDF failed with exception: {e.original_exception}"
            query_result = PyQueryResult(QueryEndState.Failed, err_msg)
            ctx._notify_query_end(query_id, query_result)
            raise e
        except Exception as e:
            err_msg = f"General Exception raised: {e}"
            query_result = PyQueryResult(QueryEndState.Failed, err_msg)
            ctx._notify_query_end(query_id, query_result)
            raise e

    def run_iter_tables(
        self, builder: LogicalPlanBuilder, results_buffer_size: int | None = None
    ) -> Iterator[MicroPartition]:
        for result in self.run_iter(builder, results_buffer_size=results_buffer_size):
            yield result.partition()
