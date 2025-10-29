from __future__ import annotations

from typing import TYPE_CHECKING

from daft.daft import (
    LocalPhysicalPlan,
    PyDaftExecutionConfig,
    PyLocalPartitionStream,
    PyMicroPartition,
)
from daft.daft import (
    NativeExecutor as _NativeExecutor,
)
from daft.dataframe.display import MermaidOptions
from daft.event_loop import get_or_init_event_loop
from daft.recordbatch import MicroPartition

if TYPE_CHECKING:
    from collections.abc import Iterator

    from daft.context import DaftContext
    from daft.logical.builder import LogicalPlanBuilder
    from daft.runners.partitioning import (
        LocalMaterializedResult,
        MaterializedResult,
        PartitionT,
    )


class LocalPartitionStream:
    def __init__(self, stream: PyLocalPartitionStream) -> None:
        self._stream = stream

    def __aiter__(self) -> LocalPartitionStream:
        return self

    async def __anext__(self) -> PyMicroPartition:
        return await self._stream.__anext__()


class NativeExecutor:
    def __init__(self) -> None:
        self._executor = _NativeExecutor()

    def run(
        self,
        local_physical_plan: LocalPhysicalPlan,
        psets: dict[str, list[MaterializedResult[PartitionT]]],
        ctx: DaftContext,
        results_buffer_size: int | None,
        context: dict[str, str] | None,
    ) -> Iterator[LocalMaterializedResult]:
        from daft.runners.partitioning import LocalMaterializedResult

        psets_mp = {
            part_id: [part.micropartition()._micropartition for part in parts] for part_id, parts in psets.items()
        }

        async def run_executor() -> PyLocalPartitionStream:
            return self._executor.run(
                local_physical_plan,
                psets_mp,
                ctx._ctx,
                results_buffer_size,
                context,
            )

        async_iter = LocalPartitionStream(get_or_init_event_loop().run(run_executor()))
        while True:
            part = get_or_init_event_loop().run(async_iter.__anext__())
            if part is None:
                break
            yield LocalMaterializedResult(MicroPartition._from_pymicropartition(part))

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
