from __future__ import annotations

from typing import TYPE_CHECKING

from daft.daft import (
    LocalPhysicalPlan,
    PyDaftExecutionConfig,
    PyMicroPartition,
)
from daft.daft import (
    NativeExecutor as _NativeExecutor,
)
from daft.dataframe.display import MermaidOptions
from daft.event_loop import get_or_init_event_loop
from daft.recordbatch import MicroPartition

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterator

    from daft.context import DaftContext
    from daft.logical.builder import LogicalPlanBuilder
    from daft.runners.partitioning import (
        LocalMaterializedResult,
        MaterializedResult,
        PartitionT,
    )


class NativeExecutor:
    def __init__(self) -> None:
        self._executor = _NativeExecutor()

    def run(
        self,
        local_physical_plan: LocalPhysicalPlan,
        inputs: list,
        psets: dict[str, list[MaterializedResult[PartitionT]]],
        ctx: DaftContext,
        context: dict[str, str] | None,
    ) -> Iterator[LocalMaterializedResult]:
        from daft.runners.partitioning import LocalMaterializedResult

        async def stream_results() -> AsyncGenerator[PyMicroPartition | None, None]:
            # Process inputs and organize them by type
            # All inputs have input_id 0 as specified in the requirements
            scan_tasks_map = {}
            in_memory_map = {}
            glob_paths_map = {}
            
            for input_spec in inputs:
                source_id = input_spec.source_id
                
                if input_spec.input_type == "scan_task":
                    # Extract scan tasks from the input spec
                    scan_tasks = input_spec.get_scan_tasks()
                    if scan_tasks is not None:
                        scan_tasks_map[source_id] = scan_tasks
                
                elif input_spec.input_type == "in_memory":
                    # For in-memory sources, get data from psets using the cache key
                    cache_key = input_spec.get_cache_key()
                    if cache_key is not None and cache_key in psets:
                        # Get the micropartitions for this cache key
                        parts = psets[cache_key]
                        in_memory_map[source_id] = [part.micropartition()._micropartition for part in parts]
                
                elif input_spec.input_type == "glob_paths":
                    # Extract glob paths from the input spec
                    paths = input_spec.get_glob_paths()
                    if paths is not None:
                        glob_paths_map[source_id] = paths
            
            # Run plan (creating if needed) and enqueue inputs with input_id 0
            result_receiver = await self._executor.run_and_enqueue_inputs(
                local_physical_plan,
                ctx._ctx,
                0,
                context,
                scan_tasks_map if scan_tasks_map else None,
                in_memory_map if in_memory_map else None,
                glob_paths_map if glob_paths_map else None,
            )
            assert result_receiver is not None, "Result receiver should not be None for NativeExecutor"
            try:
                async for batch in result_receiver:
                    yield batch
            finally:
                # Get fingerprint to finish the plan
                fingerprint = local_physical_plan.fingerprint()
                _ = await self._executor.finish(fingerprint)

        event_loop = get_or_init_event_loop()
        async_exec = stream_results()
        try:
            while True:
                part = event_loop.run(async_exec.__anext__())
                if part is None:
                    break
                yield LocalMaterializedResult(MicroPartition._from_pymicropartition(part))
        finally:
            event_loop.run(async_exec.aclose())

    def pretty_print(
        self,
        builder: LogicalPlanBuilder,
        daft_execution_config: PyDaftExecutionConfig,
        simple: bool = False,
        format: str = "ascii",
    ) -> str:
        """Pretty prints the current underlying logical plan."""
        if format == "ascii":
            return _NativeExecutor.repr_ascii(builder._builder, daft_execution_config, simple)
        elif format == "mermaid":
            return _NativeExecutor.repr_mermaid(builder._builder, daft_execution_config, MermaidOptions(simple))
        else:
            raise ValueError(f"Unknown format: {format}")
