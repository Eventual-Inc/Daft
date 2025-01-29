from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from daft.daft import (
    NativeExecutor as _NativeExecutor,
)
from daft.dataframe.display import MermaidOptions
from daft.table import MicroPartition

if TYPE_CHECKING:
    from daft.daft import PyDaftExecutionConfig
    from daft.logical.builder import LogicalPlanBuilder
    from daft.runners.partitioning import (
        LocalMaterializedResult,
        MaterializedResult,
        PartitionT,
    )


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
