from __future__ import annotations

from typing import Generic, TypeVar

from daft.context import get_context
from daft.execution.dynamic_construction import InstructionFactory
from daft.execution.dynamic_schedule import (
    DynamicSchedule,
    ScheduleCoalesce,
    ScheduleFanoutReduce,
    ScheduleFileRead,
    ScheduleFileWrite,
    ScheduleGlobalLimit,
    ScheduleJoin,
    SchedulePartitionRead,
    SchedulePipelineInstruction,
    ScheduleSort,
)
from daft.logical import logical_plan
from daft.logical.logical_plan import LogicalPlan, PartitionScheme

PartitionT = TypeVar("PartitionT")


class DynamicScheduleFactory(Generic[PartitionT]):
    def __init__(self):
        pass

    def schedule_logical_node(self, node: LogicalPlan) -> DynamicSchedule[PartitionT]:
        """Translates a LogicalPlan into an appropriate DynamicSchedule."""

        # -- Leaf nodes. --
        if isinstance(node, logical_plan.InMemoryScan):
            pset = get_context().runner().get_partition_set_from_cache(node._cache_entry.key).value
            assert pset is not None
            partitions = pset.values()
            return SchedulePartitionRead[PartitionT](partitions)

        # -- Unary nodes. --

        elif isinstance(node, logical_plan.UnaryNode):
            [child_node] = node._children()
            child_schedule = DynamicScheduleFactory[PartitionT]().schedule_logical_node(child_node)

            if isinstance(node, logical_plan.TabularFilesScan):
                return ScheduleFileRead[PartitionT](child_schedule=child_schedule, scan_node=node)

            elif isinstance(node, logical_plan.LocalLimit):
                # Ignore LocalLimit logical nodes; the GlobalLimit handles everything
                # and will dynamically dispatch appropriate local limit instructions.
                return child_schedule

            elif isinstance(node, logical_plan.Filter):
                return SchedulePipelineInstruction[PartitionT](
                    child_schedule=child_schedule, pipeable_instruction=InstructionFactory.filter(node._predicate)
                )

            elif isinstance(node, logical_plan.Projection):
                return SchedulePipelineInstruction[PartitionT](
                    child_schedule=child_schedule, pipeable_instruction=InstructionFactory.project(node._projection)
                )

            elif isinstance(node, logical_plan.MapPartition):
                return SchedulePipelineInstruction[PartitionT](
                    child_schedule=child_schedule,
                    pipeable_instruction=InstructionFactory.map_partition(node._map_partition_op),
                )

            elif isinstance(node, logical_plan.LocalAggregate):
                return SchedulePipelineInstruction[PartitionT](
                    child_schedule=child_schedule,
                    pipeable_instruction=InstructionFactory.agg(node._agg, node._group_by),
                )

            elif isinstance(node, logical_plan.LocalDistinct):
                return SchedulePipelineInstruction[PartitionT](
                    child_schedule=child_schedule, pipeable_instruction=InstructionFactory.agg([], node._group_by)
                )

            elif isinstance(node, logical_plan.FileWrite):
                return ScheduleFileWrite[PartitionT](child_schedule, node)

            elif isinstance(node, logical_plan.GlobalLimit):
                return ScheduleGlobalLimit[PartitionT](child_schedule, node)

            elif isinstance(node, logical_plan.Repartition):
                # Translate PartitionScheme to the appropriate fanout_fn
                if node._scheme == PartitionScheme.RANDOM:
                    fanout_fn = InstructionFactory.fanout_random(num_outputs=node.num_partitions())
                elif node._scheme == PartitionScheme.HASH:
                    fanout_fn = InstructionFactory.fanout_hash(
                        num_outputs=node.num_partitions(),
                        partition_by=node._partition_by,
                    )
                else:
                    raise RuntimeError(f"Unimplemented partitioning scheme {node._scheme}")

                return ScheduleFanoutReduce[PartitionT](
                    child_schedule=child_schedule,
                    num_outputs=node.num_partitions(),
                    fanout_fn=fanout_fn,
                    reduce_fn=InstructionFactory.merge(),
                )

            elif isinstance(node, logical_plan.Sort):
                return ScheduleSort[PartitionT](child_schedule, node)

            elif isinstance(node, logical_plan.Coalesce):
                return ScheduleCoalesce[PartitionT](child_schedule, node)

            else:
                raise NotImplementedError(f"Unsupported plan type {node}")

        # -- Binary nodes. --
        elif isinstance(node, logical_plan.BinaryNode):
            [left_child, right_child] = node._children()

            if isinstance(node, logical_plan.Join):
                return ScheduleJoin(
                    left_source=self.schedule_logical_node(left_child),
                    right_source=self.schedule_logical_node(right_child),
                    join=node,
                )

            else:
                raise NotImplementedError(f"Unsupported plan type {node}")

        else:
            raise NotImplementedError(f"Unsupported plan type {node}")
