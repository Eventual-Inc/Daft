from __future__ import annotations

from typing import TYPE_CHECKING

from daft.daft import (
    AdaptivePhysicalPlanScheduler as _AdaptivePhysicalPlanScheduler,
)
from daft.daft import PhysicalPlanScheduler as _PhysicalPlanScheduler
from daft.daft import (
    PyDaftExecutionConfig,
)
from daft.execution import physical_plan

if TYPE_CHECKING:
    from daft.logical.builder import LogicalPlanBuilder
    from daft.runners.partitioning import (
        PartitionCacheEntry,
        PartitionT,
    )


class PhysicalPlanScheduler:
    """Generates executable tasks for an underlying physical plan."""

    def __init__(self, scheduler: _PhysicalPlanScheduler):
        self._scheduler = scheduler

    @classmethod
    def from_logical_plan_builder(
        cls, builder: LogicalPlanBuilder, daft_execution_config: PyDaftExecutionConfig
    ) -> PhysicalPlanScheduler:
        scheduler = _PhysicalPlanScheduler.from_logical_plan_builder(builder._builder, daft_execution_config)
        return cls(scheduler)

    def num_partitions(self) -> int:
        return self._scheduler.num_partitions()

    def pretty_print(self, simple: bool = False, format: str = "ascii") -> str:
        """Pretty prints the current underlying physical plan."""
        from daft.dataframe.display import MermaidOptions

        if format == "ascii":
            return self._scheduler.repr_ascii(simple)
        elif format == "mermaid":
            return self._scheduler.repr_mermaid(MermaidOptions(simple))
        else:
            raise ValueError(f"Unknown format: {format}")

    def __repr__(self) -> str:
        return self._scheduler.repr_ascii(simple=False)

    def to_json_string(self) -> str:
        return self._scheduler.to_json_string()

    def to_partition_tasks(
        self,
        psets: dict[str, list[PartitionT]],
        actor_pool_manager: physical_plan.ActorPoolManager,
        results_buffer_size: int | None,
    ) -> physical_plan.MaterializedPhysicalPlan[PartitionT]:
        return iter(
            physical_plan.Materialize(
                self._scheduler.to_partition_tasks(psets, actor_pool_manager), results_buffer_size
            )
        )


class AdaptivePhysicalPlanScheduler:
    def __init__(self, scheduler: _AdaptivePhysicalPlanScheduler) -> None:
        self._scheduler = scheduler

    @classmethod
    def from_logical_plan_builder(
        cls, builder: LogicalPlanBuilder, daft_execution_config: PyDaftExecutionConfig
    ) -> AdaptivePhysicalPlanScheduler:
        scheduler = _AdaptivePhysicalPlanScheduler.from_logical_plan_builder(builder._builder, daft_execution_config)
        return cls(scheduler)

    def next(self) -> tuple[int | None, PhysicalPlanScheduler]:
        sid, pps = self._scheduler.next()
        return sid, PhysicalPlanScheduler(pps)

    def is_done(self) -> bool:
        return self._scheduler.is_done()

    def update(self, stage_id: int, cache_entry: PartitionCacheEntry) -> None:
        num_partitions = cache_entry.num_partitions()
        assert num_partitions is not None
        size_bytes = cache_entry.size_bytes()
        assert size_bytes is not None
        num_rows = cache_entry.num_rows()
        assert num_rows is not None

        self._scheduler.update(
            stage_id,
            cache_entry.key,
            cache_entry,
            num_partitions=num_partitions,
            size_bytes=size_bytes,
            num_rows=num_rows,
        )

    def update_stats(
        self, time_taken: float, size_bytes: int | None, num_rows: int | None, stage_id: int | None
    ) -> None:
        self._scheduler.update_stats(time_taken, size_bytes, num_rows, stage_id)

    def explain_analyze(self, explain_analyze_dir: str) -> None:
        self._scheduler.explain_analyze(explain_analyze_dir)
