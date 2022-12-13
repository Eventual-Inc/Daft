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

        scheduler = DynamicScheduler(plan)
        for next_partition_to_build in scheduler:
            partition = self._build_partition(next_partition_to_build)
            scheduler.register_partition(next_partition_to_build, partition)

        final_result = scheduler.result_partition_set()
        pset_entry = self.put_partition_set_into_cache(final_result)
        return pset_entry


    def _build_partition(self, partspec: PartitionInstructions) -> vPartition:
        """
        Postcondition:
        """

        partitions = partspec.inputs

        for instruction in partspec.instruction_stack:
            if isinstance(instruction, logical_plan.Filter):
                [input] = partitions
                partitions = [input.filter(instruction._predicate)]
            elif isinstance(instruction, logical_plan.InMemoryScan):




        [result] = partitions
        return result


class DynamicScheduler:
    """Dynamically generates an execution schedule for the given logical plan."""

    def __init__(self, root_plan_node: LogicalPlan) -> None:
        self._root_plan_node = root_plan_node
        # Materialized partitions by their node ID.
        # `None` denotes that the partition has been dispatched but is still under construction.
        self._materializations_by_node_id: dict[int, list[vPartition | None]] = dict()
        self._materializations_by_node_id[root_plan_node.id()] = list()

        # Number of partitions dispatched from leaf nodes.
        self._leaf_node_progress: dict[int, int] = dict()

    def __iter__(self):
        return self

    def __next__(self) -> PartitionInstructions | None:
        """
        Raises StopIteration if there are no further instructions to give for this plan node.
        Returns None if there is nothing we can do for now (i.e. must wait for other partitions to finish).
        """

        result = self._next_computable_partition(self._root_plan_node)
        if result is not None and not result.marked_for_materialization():
            result.mark_for_materialization(
                nid=self._root_plan_node.id(),
                partno=len(self._partitions_by_node_id[self._root_plan_node.id()]),
            )
            self._partitions_by_node_id[self._root_plan_node.id()].append(None)
        return result

    def _next_computable_partition(self, plan_node: LogicalPlan) -> PartitionInstructions | None:
        """
        Raises StopIteration if there are no further instructions to give for this plan node.
        Returns None if there is nothing we can do for now (i.e. must wait for other partitions to finish).
        """

        # # Initialize state tracking for this plan node if not yet seen.
        # if plan_node.id() not in self._partitions_by_node_id:
            # self._partitions_by_node_id[plan_node.id()] = list()

        # # Check if all partitions have been dispatched.
        # if len(self._partitions_by_node_id[plan_node.id()]) == plan_node.num_partitions():
            # # Check if all partitions have also been completed.
            # if all(maybe_part is not None for maybe_part in self._partitions_by_node_id[plan_node.id()]):
                # raise StopIteration
            # else:
                # return None


        # Leaf nodes.
        if isinstance(plan_node, (
            logical_plan.InMemoryScan,
            # XXX TODO
        )):
            return self._next_impl_leaf_node(plan_node)


        # Pipelineable nodes.
        elif isinstance(plan_node, (
            logical_plan.Filter,
        )):
            return self._next_impl_pipeable_node(plan_node)

        # Compulsory materialization nodes.
        # XXX TODO
        elif isinstance(plan_node, (
            # XXX TODO
        )):
            return self._next_impl_materialize_node(plan_node)




    def _next_impl_leaf_node(self, plan_node: LogicalPlan) -> PartitionInstructions:
        # Initialize state tracking for this leaf node if not yet seen.
        if plan_node.id() not in self._leaf_node_progress:
            self._leaf_node_progress[plan_node.id()] = 0

        # Check if we're done with this leaf node.
        if self._leaf_node_progress[plan_node.id()] == plan_node.num_partitions():
            raise StopIteration

        # This is the next partition to dispatch.
        next_partno = self._leaf_node_progress[plan_node.id()]

        if isinstance(plan_node, logical_plan.InMemoryScan):
            # The backing partitions are already materialized.
            # Pick out the appropriate one and initialize an instruction wrapper around it.
            pset = self.get_partition_set_from_cache(im_scan._cache_entry.key).value
            partition = pset.items()[next_partno][1]
            self._leaf_node_progress[plan_node.id()] += 1
            return PartitionInstructions([partition])

    def _next_impl_pipeable_node(self, plan_node: LogicalPlan) -> PartitionInstructions | None:
        # Just unary nodes so far.
        # TODO: binary nodes.
        [child_node] = plan_node._children()

        # Get instructions for the child node.
        child_instructions = self._next_computable_partition(child_node)
        if child_instructions is None:
            # Child node can't provide any instructions now, so we can't either.
            return None
        if child_instructions.marked_for_materialization():
            # Child node provided a materialization request, so we have to pass it through.
            return child_instructions

        # Normal case: child node provided instructions; add our instructions on top.
        child_instructions.add_instruction(plan_node)
        return child_instructions

    def _next_impl_materialize_node(self, plan_node: LogicalPlan) -> PartitionInstructions | None:
        """
        Precondition: There are undispatched partitions for this plan node.
        """
        raise # XXX TODO


    def register_completed_partition(self, instructions: PartitionInstructions, partition: vPartition) -> None:
        self._partitions_by_node_id[instructions.nid][partno] = partition

    def result_partition_set(self) -> PartitionSet[vPartition]:
        if not self._is_node_ready[self._root_plan_node]:
            raise RuntimeError("The plan has not finished executing yet.")
        result = LocalPartitionSet({})
        for i, partition in self._partitions_by_node_id[root_plan_node.id()]:
            result.set_partition(i, partition)
        return result


class PartitionInstructions:

    def __init__(self, inputs: list[vPartition]) -> None:
        # Input partitions to run over.
        self.inputs = inputs

        # Instruction stack to execute.
        # This can be logical nodes for now.
        self.instruction_stack: list[LogicalPlan] = list()

        # Materialization location: materialize as partition partno for plan node nid.
        self.nid: int | None = None
        self.partno: int | None = None

    def add_instruction(self, instruction: LogicalPlan) -> None:
        """Add an instruction to the stack that will run for this partition."""
        self.assert_not_marked()
        self.instruction_stack.append(instruction)

    def mark_for_materialization(self, nid: int, partno: int) -> None:
        """Mark this list of instructions to be materialized.

        nid: Node ID to materialize for.
        partno: The index within the plan node to materialize this partition at.

        Once marked for materialization, this object cannot be further modified.
        """
        self.assert_not_marked()
        self.nid = nid
        self.partno = partno

    def marked_for_materialization(self, nid: int, partno: int) -> bool:
        return self.nid is not None and self.partno is not None

    def assert_not_marked(self) -> None:
        assert not self.marked_for_materialization(), (
            f"Partition already instructed to materialize for node ID {self.nid}, partition index {self.partno}"
        )
