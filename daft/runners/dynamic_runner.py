from __future__ import annotations

from daft.context import get_context
from daft.execution.dynamic_construction import (
    Construction,
    make_agg_instruction,
    make_fanout_hash_instruction,
    make_fanout_random_instruction,
    make_filter_instruction,
    make_map_partition_instruction,
    make_merge_instruction,
    make_project_instruction,
)
from daft.execution.dynamic_schedule import (
    DynamicSchedule,
    ScheduleCoalesce,
    ScheduleFanoutReduce,
    ScheduleFileRead,
    ScheduleFileWrite,
    ScheduleGlobalLimit,
    ScheduleJoin,
    ScheduleMaterialize,
    SchedulePartitionRead,
    SchedulePipelineInstruction,
    ScheduleSort,
)
from daft.internal.rule_runner import FixedPointPolicy, Once, RuleBatch, RuleRunner
from daft.logical import logical_plan
from daft.logical.logical_plan import LogicalPlan, PartitionScheme
from daft.logical.optimizer import (
    DropProjections,
    DropRepartition,
    FoldProjections,
    PruneColumns,
    PushDownClausesIntoScan,
    PushDownLimit,
    PushDownPredicates,
)
from daft.runners.partitioning import PartitionCacheEntry
from daft.runners.runner import Runner


class DynamicRunner(Runner):
    """A dynamic version of PyRunner that uses DynamicSchedule to determine execution steps."""

    def __init__(self) -> None:
        super().__init__()
        # From PyRunner
        self._optimizer = RuleRunner(
            [
                RuleBatch(
                    "SinglePassPushDowns",
                    Once,
                    [
                        DropRepartition(),
                        PushDownPredicates(),
                        PruneColumns(),
                        FoldProjections(),
                        PushDownClausesIntoScan(),
                    ],
                ),
                RuleBatch(
                    "PushDownLimitsAndRepartitions",
                    FixedPointPolicy(3),
                    [PushDownLimit(), DropRepartition(), DropProjections()],
                ),
            ]
        )

    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        # From PyRunner
        return self._optimizer.optimize(plan)

    def run(self, plan: LogicalPlan) -> PartitionCacheEntry:
        plan = self.optimize(plan)

        schedule = schedule_logical_node(plan)
        schedule = ScheduleMaterialize(schedule)

        for next_construction in schedule:
            assert next_construction is not None, "Got a None construction in singlethreaded mode"
            self._build_partitions(next_construction)

        final_result = schedule.result_partition_set()
        pset_entry = self.put_partition_set_into_cache(final_result)
        return pset_entry

    def _build_partitions(self, partspec: Construction) -> None:
        partitions = partspec.inputs

        for instruction in partspec.instruction_stack:
            partitions = instruction(partitions)

        partspec.report_completed(partitions)


def schedule_logical_node(node: LogicalPlan) -> DynamicSchedule:
    """Translates a LogicalPlan into an appropriate DynamicSchedule."""

    # -- Leaf nodes. --
    if isinstance(node, logical_plan.InMemoryScan):
        pset = get_context().runner().get_partition_set_from_cache(node._cache_entry.key).value
        assert pset is not None
        partitions = pset.values()
        return SchedulePartitionRead(partitions)

    elif isinstance(node, logical_plan.Scan):
        return ScheduleFileRead(node)

    # -- Unary nodes. --

    elif isinstance(node, logical_plan.UnaryNode):
        [child_node] = node._children()
        child_schedule = schedule_logical_node(child_node)

        if isinstance(node, logical_plan.LocalLimit):
            # Ignore LocalLimit logical nodes; the GlobalLimit handles everything
            # and will dynamically dispatch appropriate local limit instructions.
            return child_schedule

        elif isinstance(node, logical_plan.Filter):
            return SchedulePipelineInstruction(
                child_schedule=child_schedule, pipeable_instruction=make_filter_instruction(node._predicate)
            )

        elif isinstance(node, logical_plan.Projection):
            return SchedulePipelineInstruction(
                child_schedule=child_schedule, pipeable_instruction=make_project_instruction(node._projection)
            )

        elif isinstance(node, logical_plan.MapPartition):
            return SchedulePipelineInstruction(
                child_schedule=child_schedule,
                pipeable_instruction=make_map_partition_instruction(node._map_partition_op),
            )

        elif isinstance(node, logical_plan.LocalAggregate):
            return SchedulePipelineInstruction(
                child_schedule=child_schedule, pipeable_instruction=make_agg_instruction(node._agg, node._group_by)
            )

        elif isinstance(node, logical_plan.LocalDistinct):
            return SchedulePipelineInstruction(
                child_schedule=child_schedule, pipeable_instruction=make_agg_instruction([], node._group_by)
            )

        elif isinstance(node, logical_plan.FileWrite):
            return ScheduleFileWrite(child_schedule, node)

        elif isinstance(node, logical_plan.GlobalLimit):
            return ScheduleGlobalLimit(child_schedule, node)

        elif isinstance(node, logical_plan.Repartition):
            # Translate PartitionScheme to the appropriate fanout_fn
            if node._scheme == PartitionScheme.RANDOM:
                fanout_fn = make_fanout_random_instruction(num_outputs=node.num_partitions())
            elif node._scheme == PartitionScheme.HASH:
                fanout_fn = make_fanout_hash_instruction(
                    num_outputs=node.num_partitions(),
                    partition_by=node._partition_by,
                )
            else:
                raise RuntimeError(f"Unimplemented partitioning scheme {node._scheme}")

            return ScheduleFanoutReduce(
                child_schedule=child_schedule,
                num_outputs=node.num_partitions(),
                fanout_fn=fanout_fn,
                reduce_fn=make_merge_instruction(),
            )

        elif isinstance(node, logical_plan.Sort):
            return ScheduleSort(child_schedule, node)

        elif isinstance(node, logical_plan.Coalesce):
            return ScheduleCoalesce(child_schedule, node)

        else:
            raise NotImplementedError(f"Unsupported plan type {node}")

    # -- Binary nodes. --
    elif isinstance(node, logical_plan.BinaryNode):
        [left_child, right_child] = node._children()

        if isinstance(node, logical_plan.Join):
            return ScheduleJoin(
                left_source=schedule_logical_node(left_child),
                right_source=schedule_logical_node(right_child),
                join=node,
            )

        else:
            raise NotImplementedError(f"Unsupported plan type {node}")

    else:
        raise NotImplementedError(f"Unsupported plan type {node}")
