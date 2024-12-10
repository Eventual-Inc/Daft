from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from daft.daft import (
    NativeExecutor as _NativeExecutor,
)
from daft.daft import PyDaftExecutionConfig
from daft.table import MicroPartition

if TYPE_CHECKING:
    from daft.logical.builder import LogicalPlanBuilder
    from daft.runners.partitioning import LocalMaterializedResult, PartitionSetCache


class NativeExecutor:
    def __init__(self, executor: _NativeExecutor):
        self._executor = executor

    @classmethod
    def from_logical_plan_builder(cls, builder: LogicalPlanBuilder) -> NativeExecutor:
        executor = _NativeExecutor.from_logical_plan_builder(builder._builder)
        return cls(executor)

    def run(
        self,
        pset_cache: PartitionSetCache,
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: int | None,
    ) -> Iterator[LocalMaterializedResult]:
        from daft.runners.partitioning import LocalMaterializedResult

        return (
            LocalMaterializedResult(MicroPartition._from_pymicropartition(part))
            for part in self._executor.run(pset_cache, daft_execution_config, results_buffer_size)
        )
