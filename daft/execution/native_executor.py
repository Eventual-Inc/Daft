from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from daft.daft import (
    NativeExecutor as _NativeExecutor,
)
from daft.logical.builder import LogicalPlanBuilder
from daft.runners.partitioning import (
    MaterializedResult,
    PartitionT,
)
from daft.table import MicroPartition

if TYPE_CHECKING:
    from daft.runners.pyrunner import PyMaterializedResult


class NativeExecutor:
    def __init__(self, executor: _NativeExecutor):
        self._executor = executor

    @classmethod
    def from_logical_plan_builder(cls, builder: LogicalPlanBuilder) -> NativeExecutor:
        executor = _NativeExecutor.from_logical_plan_builder(builder._builder)
        return cls(executor)

    def run(self, psets: dict[str, list[MaterializedResult[PartitionT]]]) -> Iterator[PyMaterializedResult]:
        from daft.runners.pyrunner import PyMaterializedResult

        psets_mp = {part_id: [part.vpartition()._micropartition for part in parts] for part_id, parts in psets.items()}
        return (
            PyMaterializedResult(MicroPartition._from_pymicropartition(part)) for part in self._executor.run(psets_mp)
        )
