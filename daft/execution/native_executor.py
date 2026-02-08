from __future__ import annotations

from typing import TYPE_CHECKING

from daft.daft import (
    Input,
    LocalPhysicalPlan,
    PyDaftExecutionConfig,
    PyExecutionEngineFinalResult,
    PyMicroPartition,
)
from daft.daft import (
    NativeExecutor as _NativeExecutor,
)
from daft.dataframe.display import MermaidOptions
from daft.event_loop import get_or_init_event_loop
from daft.recordbatch import MicroPartition, RecordBatch

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator, Mapping

    from daft.context import DaftContext
    from daft.logical.builder import LogicalPlanBuilder
    from daft.runners.partitioning import (
        LocalMaterializedResult,
    )


class NativeExecutor:
    def __init__(self) -> None:
        self._executor = _NativeExecutor()

    def run(
        self,
        local_physical_plan: LocalPhysicalPlan,
        inputs: Mapping[int, Input | list[PyMicroPartition]],
        ctx: DaftContext,
        context: dict[str, str] | None,
    ) -> Generator[LocalMaterializedResult, None, RecordBatch]:
        from daft.runners.partitioning import (
            LocalMaterializedResult,
        )

        result: PyExecutionEngineFinalResult | None = None

        async def stream_results() -> AsyncGenerator[PyMicroPartition | None, None]:
            result_handle = await self._executor.run(
                local_physical_plan,
                ctx._ctx,
                dict(inputs),
                0,
                context,
            )
            nonlocal result
            try:
                async for batch in result_handle:
                    yield batch
            finally:
                result = await result_handle.try_finish()

        event_loop = get_or_init_event_loop()
        async_exec = stream_results()
        try:
            while True:
                part = event_loop.run(async_exec.__anext__())
                if part is None:
                    break
                yield LocalMaterializedResult(MicroPartition._from_pymicropartition(part))
        except KeyboardInterrupt as e:
            raise e
        except Exception as e:
            raise e
        else:
            event_loop.run(async_exec.aclose())
            assert result is not None
            return RecordBatch._from_pyrecordbatch(result.to_recordbatch())

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
