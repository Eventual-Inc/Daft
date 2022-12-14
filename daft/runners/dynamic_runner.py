from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable

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
from daft.logical.schema import ExpressionList
from daft.runners.partitioning import PartitionCacheEntry, PartitionSet, vPartition
from daft.runners.pyrunner import LocalPartitionSet
from daft.runners.runner import Runner


class DynamicRunner(Runner):
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

        scheduler = DynamicScheduler(plan, self)
        for next_partition_to_build in scheduler:
            partition = self._build_partition(next_partition_to_build)
            scheduler.register_completed_partition(next_partition_to_build, partition)

        final_result = scheduler.result_partition_set()
        pset_entry = self.put_partition_set_into_cache(final_result)
        return pset_entry

    def _build_partition(self, partspec: PartitionInstructions) -> vPartition:
        partitions = partspec.inputs

        for instruction in partspec.instruction_stack:
            partitions = instruction(partitions)

        [result] = partitions
        return result

    @staticmethod
    def instruction_filter(predicate: ExpressionList) -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            [input] = inputs
            return [input.filter(predicate)]

        return instruction

    @staticmethod
    def instruction_local_limit(limit: int) -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            [input] = inputs
            return [input.head(limit)]

        return instruction


class DynamicScheduler:
    """Dynamically generates an execution schedule for the given logical plan."""

    def __init__(self, root_plan_node: LogicalPlan, runner: DynamicRunner) -> None:
        self._root_plan_node = root_plan_node
        # Materialized partitions by their node ID.
        # `None` denotes that the partition has been dispatched but is still under construction.
        self._materializations_by_node_id: dict[int, list[vPartition | None]] = dict()
        self._materializations_by_node_id[root_plan_node.id()] = list()

        # The runner calling this scheduler, which determines the implementation of instructions.
        self._runner = runner

        # Arbitrary dictionaries to track in-progress state of logical nodes.
        # TODO: refactor elsewhere.
        self._node_states: dict[int, dict[str, Any]] = defaultdict(dict)

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
                partno=len(self._materializations_by_node_id[self._root_plan_node.id()]),
            )
            self._materializations_by_node_id[self._root_plan_node.id()].append(None)
        return result

    def _next_computable_partition(self, plan_node: LogicalPlan) -> PartitionInstructions | None:
        """
        Raises StopIteration if there are no further instructions to give for this plan node.
        Returns None if there is nothing we can do for now (i.e. must wait for other partitions to finish).
        """

        """
        # Initialize state tracking for this plan node if not yet seen.
        if plan_node.id() not in self._partitions_by_node_id:
            self._partitions_by_node_id[plan_node.id()] = list()

        # Check if all partitions have been dispatched.
        if len(self._partitions_by_node_id[plan_node.id()]) == plan_node.num_partitions():
            # Check if all partitions have also been completed.
            if all(maybe_part is not None for maybe_part in self._partitions_by_node_id[plan_node.id()]):
                raise StopIteration
            else:
                return None
        """

        # Leaf nodes.
        if isinstance(
            plan_node,
            (
                logical_plan.InMemoryScan,
                # XXX TODO
            ),
        ):
            return self._next_impl_leaf_node(plan_node)

        # Pipelineable nodes.
        elif isinstance(
            plan_node,
            (
                logical_plan.Filter,
                logical_plan.LocalLimit,
            ),
        ):
            return self._next_impl_pipeable_node(plan_node)

        # Nodes that might need dependencies materialized.
        elif isinstance(plan_node, (logical_plan.GlobalLimit,)):
            return self._next_impl_matdeps_node(plan_node)

        raise

    def _next_impl_leaf_node(self, plan_node: LogicalPlan) -> PartitionInstructions:
        # Initialize state tracking for this leaf node if not yet seen.
        node_state = self._node_states[plan_node.id()]
        if "num_dispatched" not in node_state:
            node_state["num_dispatched"] = 0

        # Check if we're done with this leaf node.
        if node_state["num_dispatched"] == plan_node.num_partitions():
            raise StopIteration

        # This is the next partition to dispatch.
        partno = node_state["num_dispatched"]

        if isinstance(plan_node, logical_plan.InMemoryScan):
            # The backing partitions are already materialized.
            # Pick out the appropriate one and initialize an empty instruction wrapper around it.
            pset = self._runner.get_partition_set_from_cache(plan_node._cache_entry.key).value
            assert pset is not None
            partition = pset.items()[partno][1]

            result = PartitionInstructions([partition])

            node_state["num_dispatched"] += 1
            return result

        raise

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
        if isinstance(plan_node, logical_plan.Filter):
            instructions = self._runner.instruction_filter(plan_node._predicate)
            child_instructions.add_instruction(instructions)
        elif isinstance(plan_node, logical_plan.LocalLimit):
            # In DynamicRunner, the local limit computation will actually be dispatched by the global limit node,
            # since global limit is tracking the global row progresss and can fine-tune the local limit for each paritition.
            # This original "LocalLimit" node will thus just be treated as a no-op here.
            pass
        else:
            raise

        return child_instructions

    def _next_impl_matdeps_node(self, plan_node: LogicalPlan) -> PartitionInstructions | None:
        # Just unary nodes so far.
        # TODO: binary nodes.
        [child_node] = plan_node._children()
        node_state = self._node_states[plan_node.id()]

        if child_node.id() not in self._materializations_by_node_id:
            self._materializations_by_node_id[child_node.id()] = list()

        if isinstance(plan_node, logical_plan.GlobalLimit):
            # Instantiate node state if not yet seen.
            if not node_state:
                node_state["total_rows"] = plan_node._num
                node_state["rows_so_far"] = 0
                node_state["continue_from_partition"] = 0

            # Evaluate progress so far.
            # Are we done with the limit?
            remaining_global_limit = node_state["total_rows"] - node_state["rows_so_far"]
            assert remaining_global_limit >= 0, f"{node_state}"
            if remaining_global_limit == 0:

                # The current implementation of LogicalPlan still requires
                # a mandated number of partitions to be returned.
                # Return empty partitions until we have returned enough.

                if node_state["continue_from_partition"] < plan_node.num_partitions():
                    empty_partition = vPartition.from_pydict(
                        data=defaultdict(list),
                        schema=plan_node.schema(),
                        partition_id=node_state["continue_from_partition"],
                    )
                    next_instructions = PartitionInstructions([empty_partition])

                    node_state["continue_from_partition"] += 1
                    return next_instructions

                else:
                    raise StopIteration

            # We're not done; check to see if we can return a global limit partition.
            # Is the next local limit partition materialized?
            # If so, update global limit progress, and return the local limit.
            dependencies = self._materializations_by_node_id[child_node.id()]
            if node_state["continue_from_partition"] < len(dependencies):
                next_partition = dependencies[node_state["continue_from_partition"]]
                if next_partition is not None:
                    next_limit = min(remaining_global_limit, len(next_partition))
                    node_state["rows_so_far"] += next_limit

                    next_instructions = PartitionInstructions([next_partition])
                    if next_limit < len(next_partition):
                        next_instructions.add_instruction(self._runner.instruction_local_limit(next_limit))

                    node_state["continue_from_partition"] += 1
                    return next_instructions

            # We cannot return a global limit partition,
            # so return instructions to materialize the next local limit partition.

            child_instructions = self._next_computable_partition(child_node)
            if child_instructions is None or child_instructions.marked_for_materialization():
                return child_instructions

            child_instructions.add_instruction(self._runner.instruction_local_limit(remaining_global_limit))
            return child_instructions

        raise  # XXX TODO

    def register_completed_partition(self, instructions: PartitionInstructions, partition: vPartition) -> None:
        # for mypy
        assert instructions.nid is not None
        assert instructions.partno is not None
        self._materializations_by_node_id[instructions.nid][instructions.partno] = partition

    def result_partition_set(self) -> PartitionSet[vPartition]:
        partitions = self._materializations_by_node_id[self._root_plan_node.id()]

        # Ensure that the plan has finished executing.
        try:
            unexpected_instructions = self._next_computable_partition(self._root_plan_node)
            assert False, f"The plan has not finished executing yet, got {unexpected_instructions}"
        except StopIteration:
            pass

        # Ensure that all partitions have finished materializing.
        finished_partitions = [p for p in partitions if p is not None]
        assert len(finished_partitions) == len(
            partitions
        ), f"Not all partitions have finished materializing yet in results: {partitions}"

        # Return the result partition set.
        result = LocalPartitionSet({})
        for i, partition in enumerate(finished_partitions):
            result.set_partition(i, partition)

        return result


class PartitionInstructions:
    def __init__(self, inputs: list[vPartition]) -> None:
        # Input partitions to run over.
        self.inputs = inputs

        # Instruction stack to execute.
        self.instruction_stack: list[Callable[[list[vPartition]], list[vPartition]]] = list()

        # Materialization location: materialize as partition partno for plan node nid.
        self.nid: int | None = None
        self.partno: int | None = None

    def add_instruction(self, instruction: Callable[[list[vPartition]], list[vPartition]]) -> None:
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

    def marked_for_materialization(self) -> bool:
        return self.nid is not None and self.partno is not None

    def assert_not_marked(self) -> None:
        assert (
            not self.marked_for_materialization()
        ), f"Partition already instructed to materialize for node ID {self.nid}, partition index {self.partno}"
