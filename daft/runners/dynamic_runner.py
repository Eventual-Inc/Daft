from typing import TypeVar


from daft.internal.rule_runner import FixedPointPolicy, Once, RuleBatch, RuleRunner
from daft.logical import logical_plan
from daft.logical.logical_plan import LogicalPlan
from daft.logical.optimizer import (
    DropProjections,
    DropRepartition,
    FoldProjections,
    PruneColumns,
    PushDownClausesIntoScan,
    PushDownLimit,
    PushDownPredicates,
)
from daft.runners.partitioning import (
    PartID,
    PartitionCacheEntry,
    PartitionSet,
    vPartition,
)
from daft.runners.runner import Runner


class DynamicPyRunner(Runner):

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
        optimized_plan = self.optimize(plan)

        # TODO resource check

        scheduler = DynamicScheduler(plan)
        for next_partition_to_build in scheduler:
            partition = self._build_partition(next_partition_to_build)
            scheduler.register_partition(next_partition_to_build, partition)

        final_result = scheduler.result_partition_set()
        pset_entry = self.put_partition_set_into_cache(final_result)
        return pset_entry


    def _build_partition(self, instructions: PartitionInstructions) -> vPartition:
        """
        Postcondition:
        """

        # XXX TODO
        raise


class DynamicScheduler:
    """Dynamically generates an execution schedule for the given logical plan."""

    def __init__(self, root_plan_node: LogicalPlan) -> None:
        self._root_plan_node = root_plan_node
        # Logical plan node IDs and their completed partitions.
        # `None` denotes that the partition is under construction.
        self._partitions_by_node_id: dict[int, list[vPartition | None]] = {}
        self._is_node_ready: dict[int, bool] = {}

    def __iter__(self):
        return self

    def __next__(self):
        return self.next_computable_partition(self._root_plan_node)

    def _next_computable_partition(self, plan_node: LogicalPlan) -> PartitionInstructions | None:
        """
        Raises StopIteration if all partitions are finished for this plan node.
        Returns None if there is nothing we can do for now (i.e. must wait for other partitions to finish).
        """

        if plan_node.id() not in self._partitions_by_node_id:
            self._partitions_by_node_id[plan_node.id()] = list()
        if plan_node.id() not in self._is_node_ready:
            self._is_node_ready[plan_node.id()] = False

        # Leaf nodes.
        if isinstance(plan_node, (
            logical_plan.InMemoryScan,
            # XXX TODO
        ):
            return self._next_impl_leaf_node(plan_node)

        # XXX TODO
        raise

    def _next_impl_leaf_node(self, plan_node: LogicalPlan) -> PartitionInstructions:
        if isinstance(plan_node, logical_plan.InMemoryScan):
            # InMemoryScan is a single


    def register_completed_partition(self, instructions: PartitionInstructions, partition: vPartition) -> None:
        self._partitions_by_node_id[instructions.nid][partno] = partition

    def result_partition_set(self) -> PartitionSet[vPartition]:
        if not self._is_node_ready[self._root_plan_node]:
            raise RuntimeError("The plan has not finished executing yet.")
        result = LocalPartitionSet({})
        for i, partition in self._partitions_by_node_id[root_plan_node.id()]:
            result.set_partition(i, partition)
        return result

@dataclass(frozen=True)
class PartitionInstructions:
    # This is partition partno for plan node nid.
    nid: int
    partno: int
    # Function stack to execute.
    # This can be logical nodes for now.
    function_stack: list[LogicalPlan]
