from __future__ import annotations

from typing import TYPE_CHECKING

from daft.daft import (
    Input,
    LocalPhysicalPlan,
    PyDaftExecutionConfig,
    PyExecutionStats,
    PyMicroPartition,
)
from daft.daft import (
    NativeExecutor as _NativeExecutor,
)
from daft.dataframe.display import MermaidOptions
from daft.event_loop import get_or_init_event_loop
from daft.recordbatch import MicroPartition
from daft.runners.partitioning import LocalMaterializedResult

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator, Mapping

    from daft.context import DaftContext
    from daft.logical.builder import LogicalPlanBuilder


class NativeExecutor:
    def __init__(self) -> None:
        self._executor = _NativeExecutor()

    def run(
        self,
        local_physical_plan: LocalPhysicalPlan,
        inputs: Mapping[int, Input | list[PyMicroPartition]],
        ctx: DaftContext,
        context: dict[str, str] | None,
    ) -> Generator[LocalMaterializedResult, None, tuple[str, PyExecutionStats]]:
        stats: PyExecutionStats | None = None
        query_plan: str | None = None

        async def stream_results() -> AsyncGenerator[PyMicroPartition | None, None]:
            result_handle = await self._executor.run(
                local_physical_plan,
                ctx._ctx,
                0,
                dict(inputs),
                context,
            )
            nonlocal query_plan
            query_plan = await result_handle.query_plan()
            nonlocal stats
            try:
                async for batch in result_handle:
                    yield batch
            finally:
                stats = await result_handle.finish()

        event_loop = get_or_init_event_loop()
        async_exec = stream_results()
        should_raise_errors_from_close = True
        try:
            while True:
                part = event_loop.run(async_exec.__anext__())
                if part is None:
                    break
                yield LocalMaterializedResult(MicroPartition._from_pymicropartition(part))
        except BaseException:
            # Preserve the original exception/GeneratorExit by not masking it with errors from async_exec.aclose().
            should_raise_errors_from_close = False
            raise
        finally:
            try:
                event_loop.run(async_exec.aclose())
            except Exception:
                if should_raise_errors_from_close:
                    raise

        assert query_plan is not None and stats is not None
        return query_plan, stats

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
