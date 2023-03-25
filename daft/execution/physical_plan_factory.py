from __future__ import annotations

from typing import TypeVar

from daft.execution import execution_step, physical_plan
from daft.logical import logical_plan
from daft.logical.logical_plan import LogicalPlan, PartitionScheme

PartitionT = TypeVar("PartitionT")


def get_materializing_physical_plan(
    node: LogicalPlan, psets: dict[str, list[PartitionT]]
) -> physical_plan.MaterializedPhysicalPlan:
    """Translates a LogicalPlan into an appropriate physical plan that materializes its final results."""

    return physical_plan.materialize(_get_physical_plan(node, psets))


def _get_physical_plan(node: LogicalPlan, psets: dict[str, list[PartitionT]]) -> physical_plan.InProgressPhysicalPlan:
    """Translates a LogicalPlan into an appropriate physical plan.

    See physical_plan.py for more details.
    """

    # -- Leaf nodes. --
    if isinstance(node, logical_plan.InMemoryScan):
        partitions = psets[node._cache_entry.key]
        return physical_plan.partition_read(_ for _ in partitions)

    # -- Unary nodes. --
    elif isinstance(node, logical_plan.UnaryNode):
        [child_node] = node._children()
        child_plan = _get_physical_plan(child_node, psets)

        if isinstance(node, logical_plan.TabularFilesScan):
            return physical_plan.file_read(child_plan=child_plan, scan_info=node)

        elif isinstance(node, logical_plan.Filter):
            return physical_plan.pipeline_instruction(
                child_plan=child_plan,
                pipeable_instruction=execution_step.Filter(node._predicate),
                resource_request=node.resource_request(),
            )

        elif isinstance(node, logical_plan.Projection):
            return physical_plan.pipeline_instruction(
                child_plan=child_plan,
                pipeable_instruction=execution_step.Project(node._projection),
                resource_request=node.resource_request(),
            )

        elif isinstance(node, logical_plan.MapPartition):
            return physical_plan.pipeline_instruction(
                child_plan=child_plan,
                pipeable_instruction=execution_step.MapPartition(node._map_partition_op),
                resource_request=node.resource_request(),
            )

        elif isinstance(node, logical_plan.LocalAggregate):
            return physical_plan.pipeline_instruction(
                child_plan=child_plan,
                pipeable_instruction=execution_step.Aggregate(to_agg=node._agg, group_by=node._group_by),
                resource_request=node.resource_request(),
            )

        elif isinstance(node, logical_plan.LocalCount):
            return physical_plan.pipeline_instruction(
                child_plan=child_plan,
                pipeable_instruction=execution_step.LocalCount(logplan=node),
                resource_request=node.resource_request(),
            )

        elif isinstance(node, logical_plan.LocalDistinct):
            return physical_plan.pipeline_instruction(
                child_plan=child_plan,
                pipeable_instruction=execution_step.Aggregate(to_agg=[], group_by=node._group_by),
                resource_request=node.resource_request(),
            )

        elif isinstance(node, logical_plan.FileWrite):
            return physical_plan.file_write(child_plan, node)

        elif isinstance(node, logical_plan.LocalLimit):
            # Note that the GlobalLimit physical plan also dynamically dispatches its own LocalLimit instructions.
            return physical_plan.local_limit(child_plan, node._num)

        elif isinstance(node, logical_plan.GlobalLimit):
            return physical_plan.global_limit(child_plan, node)

        elif isinstance(node, logical_plan.Repartition):
            # Case: simple repartition (split)
            if node._scheme == PartitionScheme.UNKNOWN:
                return physical_plan.flatten_plan(
                    physical_plan.split(
                        child_plan,
                        num_input_partitions=node._children()[0].num_partitions(),
                        num_output_partitions=node.num_partitions(),
                    )
                )

            # All other repartitions require shuffling.

            # Do the fanout.
            fanout_plan: physical_plan.InProgressPhysicalPlan
            if node._scheme == PartitionScheme.RANDOM:
                fanout_plan = physical_plan.fanout_random(child_plan, node)
            elif node._scheme == PartitionScheme.HASH:
                fanout_instruction = execution_step.FanoutHash(
                    _num_outputs=node.num_partitions(),
                    partition_by=node._partition_by,
                )
                fanout_plan = physical_plan.pipeline_instruction(
                    child_plan,
                    fanout_instruction,
                    node.resource_request(),
                )
            else:
                raise RuntimeError(f"Unimplemented partitioning scheme {node._scheme}")

            # Do the reduce.
            return physical_plan.reduce(
                fanout_plan=fanout_plan,
                num_partitions=node.num_partitions(),
                reduce_instruction=execution_step.ReduceMerge(),
            )

        elif isinstance(node, logical_plan.Sort):
            return physical_plan.sort(child_plan, node)

        elif isinstance(node, logical_plan.Coalesce):
            return physical_plan.coalesce(child_plan, node)

        else:
            raise NotImplementedError(f"Unsupported plan type {node}")

    # -- Binary nodes. --
    elif isinstance(node, logical_plan.BinaryNode):
        [left_child, right_child] = node._children()

        if isinstance(node, logical_plan.Join):
            return physical_plan.join(
                left_plan=_get_physical_plan(left_child, psets),
                right_plan=_get_physical_plan(right_child, psets),
                join=node,
            )

        else:
            raise NotImplementedError(f"Unsupported plan type {node}")

    else:
        raise NotImplementedError(f"Unsupported plan type {node}")
