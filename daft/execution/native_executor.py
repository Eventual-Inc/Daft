from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from daft.daft import (
    NativeExecutor as _NativeExecutor,
)
from daft.daft import PyDaftExecutionConfig
from daft.table import MicroPartition

if TYPE_CHECKING:
    from daft.logical.builder import LogicalPlanBuilder
    from daft.runners.partitioning import (
        LocalMaterializedResult,
        MaterializedResult,
        PartitionT,
    )


class NativeExecutor:
    def __init__(self, executor: _NativeExecutor):
        self._executor = executor

    @classmethod
    def from_logical_plan_builder(cls, builder: LogicalPlanBuilder) -> NativeExecutor:
        executor = _NativeExecutor.from_logical_plan_builder(builder._builder)
        return cls(executor)

    def run(
        self,
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
            for part in self._executor.run(psets_mp, daft_execution_config, results_buffer_size)
        )

    def pretty_print(self, simple: bool = False, format: str = "ascii") -> str:
        """Pretty prints the native executor's plan."""
        from daft.dataframe.display import MermaidOptions

        if format == "ascii":
            return self._executor.repr_ascii(simple)
        elif format == "mermaid":
            return self._executor.repr_mermaid(MermaidOptions(simple))
        else:
            raise ValueError(f"Unknown format: {format}")
