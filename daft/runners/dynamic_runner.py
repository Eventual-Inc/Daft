from __future__ import annotations

from abc import abstractmethod
from collections import defaultdict
from typing import Callable

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
from daft.runners.shuffle_ops import RepartitionHashOp, RepartitionRandomOp


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

        schedule = DynamicSchedule.handle_logical_node(plan)
        schedule = ExecuteMaterialize(schedule)

        for next_construction in schedule:
            self._build_partitions(next_construction)

        final_result = schedule.result_partition_set()
        pset_entry = self.put_partition_set_into_cache(final_result)
        return pset_entry

    def _build_partitions(self, partspec: ConstructionInstructions) -> None:
        partitions = partspec.inputs

        for instruction in partspec.instruction_stack:
            partitions = instruction(partitions)

        partspec.report_completed(partitions)

    @staticmethod
    def instruction_fanout_hash(
        num_outputs: int, partition_by: ExpressionList
    ) -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            [input] = inputs
            partitions_with_ids = RepartitionHashOp.map_fn(
                input=input,
                output_partitions=num_outputs,
                exprs=partition_by,
            )
            return [partition for i, partition in sorted(partitions_with_ids.items())]

        return instruction

    @staticmethod
    def instruction_fanout_random(num_outputs: int) -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            [input] = inputs
            partitions_with_ids = RepartitionRandomOp.map_fn(
                input=input,
                output_partitions=num_outputs,
            )
            return [partition for i, partition in sorted(partitions_with_ids.items())]

        return instruction

    @staticmethod
    def instruction_merge() -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            return [vPartition.merge_partitions(inputs)]

        return instruction


class DynamicSchedule:
    """DynamicSchedule just-in-time computes the sequence of execution steps necessary to generate a target.

    The target is typically a given LogicalPlan.
    The sequence of execution steps is exposed as an iterator (__next__).
    """

    def __iter__(self):
        return self

    @abstractmethod
    def __next__(self) -> ConstructionInstructions | None:
        """
        Raises StopIteration if there are no further instructions to give for this schedule.
        Returns None if there is nothing we can do for now (i.e. must wait for other partitions to finish).
        """
        raise NotImplementedError()

    @staticmethod
    def handle_logical_node(node: LogicalPlan) -> DynamicSchedule:
        if isinstance(node, logical_plan.InMemoryScan):
            return HandlerInMemoryScan(node)
        elif isinstance(node, logical_plan.Filter):
            return HandlerFilter(node)
        elif isinstance(node, logical_plan.GlobalLimit):
            return HandlerGlobalLimit(node)
        else:
            raise RuntimeError(f"Unsupported plan type {node}")


class RequiresMaterialization(DynamicSchedule):
    def __init__(self) -> None:
        super().__init__()
        self._materializing_dependencies: list[vPartition | None] = list()

    def register_completed_dependencies(self, partitions: list[vPartition], partno: int) -> None:
        for i, partition in enumerate(partitions):
            assert self._materializing_dependencies[partno + i] is None, self._materializing_dependencies[partno + i]
            self._materializing_dependencies[partno + i] = partition

    def _prepare_to_materialize(self, instructions: ConstructionInstructions, num_results: int = 1) -> None:
        instructions.mark_for_materialization(self, len(self._materializing_dependencies))
        self._materializing_dependencies += [None] * num_results


class HandlerInMemoryScan(DynamicSchedule):
    def __init__(self, im_scan: logical_plan.InMemoryScan) -> None:
        super().__init__()
        self._im_scan = im_scan
        self._num_dispatched = 0

    def __next__(self) -> ConstructionInstructions | None:
        # Check if we're done with this leaf node.
        if self._num_dispatched == self._im_scan.num_partitions():
            raise StopIteration

        # The backing partitions are already materialized.
        # Pick out the appropriate one and initialize an empty instruction wrapper around it.
        from daft.context import get_context

        pset = get_context().runner().get_partition_set_from_cache(self._im_scan._cache_entry.key).value
        assert pset is not None
        partition = pset.items()[self._num_dispatched][1]

        new_construct = ConstructionInstructions([partition])

        self._num_dispatched += 1
        return new_construct


class HandlerFilter(DynamicSchedule):
    def __init__(self, filter_node: logical_plan.Filter) -> None:
        super().__init__()
        self._filter_node = filter_node
        [child_node] = filter_node._children()
        self.source = self.handle_logical_node(child_node)

    def __next__(self) -> ConstructionInstructions | None:
        construct = next(self.source)
        if construct is None or construct.marked_for_materialization():
            return construct

        construct.add_instruction(self._filter(self._filter_node._predicate))

        return construct

    @staticmethod
    def _filter(predicate: ExpressionList) -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            [input] = inputs
            return [input.filter(predicate)]

        return instruction


class HandlerGlobalLimit(RequiresMaterialization):
    def __init__(self, global_limit: logical_plan.GlobalLimit) -> None:
        super().__init__()
        self._global_limit = global_limit
        self._remaining_limit = self._global_limit._num
        self._continue_from_partition = 0
        [child_node] = self._global_limit._children()
        if isinstance(child_node, logical_plan.LocalLimit):
            # Ignore LocalLimit logical nodes; the GlobalLimit handles everything
            # and will dynamically dispatch appropriate local limit instructions.
            [child_node] = child_node._children()
        self._source = DynamicSchedule.handle_logical_node(child_node)

    def __next__(self) -> ConstructionInstructions | None:
        # Evaluate progress so far.
        # Are we done with the limit?
        if self._remaining_limit == 0:

            # We are done with the limit,
            # but the current implementation of LogicalPlan still requires
            # a mandated number of partitions to be returned.
            # Return empty partitions until we have returned enough.
            if self._continue_from_partition < self._global_limit.num_partitions():
                empty_partition = vPartition.from_pydict(
                    data=defaultdict(list),
                    schema=self._global_limit.schema(),
                    partition_id=self._continue_from_partition,
                )
                new_construct = ConstructionInstructions([empty_partition])

                self._continue_from_partition += 1
                return new_construct

            else:
                raise StopIteration

        # We're not done; check to see if we can return a global limit partition.
        # Is the next local limit partition materialized?
        # If so, update global limit progress, and return the local limit.
        if self._continue_from_partition < len(self._materializing_dependencies):
            next_partition = self._materializing_dependencies[self._continue_from_partition]
            if next_partition is not None:
                next_limit = min(self._remaining_limit, len(next_partition))
                self._remaining_limit -= next_limit

                new_construct = ConstructionInstructions([next_partition])
                if next_limit < len(next_partition):
                    new_construct.add_instruction(self._local_limit(next_limit))

                self._continue_from_partition += 1
                return new_construct

        # We cannot return a global limit partition,
        # so return instructions to materialize the next local limit partition.
        try:
            construct = next(self._source)
        except StopIteration:
            construct = None

        if construct is None or construct.marked_for_materialization():
            return construct

        construct.add_instruction(self._local_limit(self._remaining_limit))
        self._prepare_to_materialize(construct)
        return construct

    @staticmethod
    def _local_limit(limit: int) -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            [input] = inputs
            return [input.head(limit)]

        return instruction


class ExecuteMaterialize(RequiresMaterialization):
    """Materializes its dependencies and does nothing else."""

    def __init__(self, source: DynamicSchedule) -> None:
        super().__init__()
        self.source = source

    def __next__(self) -> ConstructionInstructions | None:
        construct = next(self.source)
        if construct is not None and not construct.marked_for_materialization():
            self._prepare_to_materialize(construct)
        return construct

    def result_partition_set(self) -> PartitionSet[vPartition]:
        # Ensure that the plan has finished executing.
        try:
            unexpected_construct = next(self.source)
            assert False, f"The plan has not finished executing yet, got {unexpected_construct}"
        except StopIteration:
            pass

        # Ensure that all partitions have finished materializing.
        finished_partitions = [p for p in self._materializing_dependencies if p is not None]
        assert len(finished_partitions) == len(
            self._materializing_dependencies
        ), f"Not all partitions have finished materializing yet in results: {self._materializing_dependencies}"

        # Return the result partition set.
        result = LocalPartitionSet({})
        for i, partition in enumerate(finished_partitions):
            result.set_partition(i, partition)

        return result


'''
class DynamicScheduler:
    """Dynamically generates an execution schedule for the given logical plan."""

    def _next_computable_partition(self, plan_node: LogicalPlan) -> ConstructionInstructions | None:
        """
        Raises StopIteration if there are no further instructions to give for this plan node.
        Returns None if there is nothing we can do for now (i.e. must wait for other partitions to finish).
        """

        # Pipelineable nodes.
        elif isinstance(
            plan_node,
            (
                logical_plan.LocalLimit,
            ),
        ):
            return self._next_impl_pipeable_node(plan_node)

        # Nodes that might need dependencies materialized.
        elif isinstance(plan_node, logical_plan.GlobalLimit):
            return self._next_impl_global_limit(plan_node)
        elif isinstance(plan_node, logical_plan.Repartition):
            return self._next_impl_repartition(plan_node)
        elif isinstance(plan_node, logical_plan.Sort):
            return self._next_impl_sort(plan_node)

        else:
            raise RuntimeError(f"Unsupported plan type {plan_node}")

    def _next_impl_pipeable_node(self, plan_node: LogicalPlan) -> ConstructionInstructions | None:
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
            raise NotImplementedError(f"Plan node {plan_node} is not supported yet.")

        return child_instructions

    def _next_impl_sort(self, plan_node: logical_plan.Sort) -> ConstructionInstructions | None:
        #

    def _next_impl_repartition(self, plan_node: logical_plan.Repartition) -> ConstructionInstructions | None:
        [child_node] = plan_node._children()
        node_state = self._node_states[plan_node.id()]

        if plan_node.id() not in self._materializations_by_node_id:
            self._materializations_by_node_id[plan_node.id()] = list()

        # Instantiate node state if not yet seen.
        if not node_state:
            node_state["reduces_emitted"] = 0
        dependencies = self._materializations_by_node_id[plan_node.id()]

        # Have we emitted all reduce partitions?
        num_reduces = plan_node.num_partitions()
        if num_reduces == node_state["reduces_emitted"]:
            raise StopIteration

        # We haven't emitted all reduce partitions yet and need to emit more.
        # To do this, we must first ensure all shufflemaps are dispatched.
        # See if we can dispatch any more shufflemaps.
        try:
            child_instructions = self._next_computable_partition(child_node)
            if child_instructions is None or child_instructions.marked_for_materialization():
                return child_instructions

            # Choose the correct instruction to dispatch based on shuffle op.
            if plan_node._scheme == PartitionScheme.RANDOM:
                child_instructions.add_instruction(self._runner.instruction_fanout_random(num_outputs=num_reduces))
            elif plan_node._scheme == PartitionScheme.HASH:
                child_instructions.add_instruction(
                    self._runner.instruction_fanout_hash(num_outputs=num_reduces, partition_by=plan_node._partition_by)
                )
            else:
                raise RuntimeError(f"Unrecognized partitioning scheme {plan_node._scheme}")

            self._prepare_to_materialize(
                instructions=child_instructions,
                nid=plan_node.id(),
                partno=len(dependencies),
                num_results=num_reduces,
            )

            return child_instructions

        except StopIteration:
            # No more shufflemaps to dispatch; continue on.
            pass

        # All shufflemaps have been dispatched; see if we can dispatch the next reduce.
        # The kth reduce's dependencies are the kth element of every batch of shufflemap outputs.
        # Here, k := node_state["reduces_emitted"] and batchlength := num_reduces
        maybe_reduce_dependencies = [
            partition for i, partition in enumerate(dependencies) if i % num_reduces == node_state["reduces_emitted"]
        ]
        reduce_dependencies = [x for x in maybe_reduce_dependencies if x is not None]
        if len(reduce_dependencies) != len(maybe_reduce_dependencies):
            # Need to wait for some shufflemaps to complete.
            return None

        next_construction = ConstructionInstructions(reduce_dependencies)
        next_construction.add_instruction(self._runner.instruction_merge())
        node_state["reduces_emitted"] += 1
        return next_construction

    def _next_impl_global_limit(self, plan_node: logical_plan.GlobalLimit) -> ConstructionInstructions | None:
        # Just unary nodes so far.
        # TODO: binary nodes.
        [child_node] = plan_node._children()
        node_state = self._node_states[plan_node.id()]

        if plan_node.id() not in self._materializations_by_node_id:
            self._materializations_by_node_id[plan_node.id()] = list()

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

            # We are done with the limit,
            # but the current implementation of LogicalPlan still requires
            # a mandated number of partitions to be returned.
            # Return empty partitions until we have returned enough.
            if node_state["continue_from_partition"] < plan_node.num_partitions():
                empty_partition = vPartition.from_pydict(
                    data=defaultdict(list),
                    schema=plan_node.schema(),
                    partition_id=node_state["continue_from_partition"],
                )
                next_instructions = ConstructionInstructions([empty_partition])

                node_state["continue_from_partition"] += 1
                return next_instructions

            else:
                raise StopIteration

        # We're not done; check to see if we can return a global limit partition.
        # Is the next local limit partition materialized?
        # If so, update global limit progress, and return the local limit.
        dependencies = self._materializations_by_node_id[plan_node.id()]
        if node_state["continue_from_partition"] < len(dependencies):
            next_partition = dependencies[node_state["continue_from_partition"]]
            if next_partition is not None:
                next_limit = min(remaining_global_limit, len(next_partition))
                node_state["rows_so_far"] += next_limit

                next_instructions = ConstructionInstructions([next_partition])
                if next_limit < len(next_partition):
                    next_instructions.add_instruction(self._runner.instruction_local_limit(next_limit))

                node_state["continue_from_partition"] += 1
                print(f"rows_so_far: {node_state['rows_so_far']}, limit: {next_limit}")
                return next_instructions

        # We cannot return a global limit partition,
        # so return instructions to materialize the next local limit partition.

        try:
            child_instructions = self._next_computable_partition(child_node)
        except StopIteration:
            child_instructions = None

        if child_instructions is None or child_instructions.marked_for_materialization():
            return child_instructions

        child_instructions.add_instruction(self._runner.instruction_local_limit(remaining_global_limit))
        self._prepare_to_materialize(
            instructions=child_instructions,
            nid=plan_node.id(),
            partno=len(dependencies),
        )
        return child_instructions

    def _prepare_to_materialize(
        self,
        instructions: ConstructionInstructions,
        nid: int,
        partno: int,
        num_results: int = 1,
    ) -> None:
        for i in range(num_results):
            self._materializations_by_node_id[nid].append(None)

        instructions.mark_for_materialization(nid, partno)

    def register_completed_partitions(self, instructions: ConstructionInstructions, partitions: list[vPartition]) -> None:
        # for mypy
        assert instructions.nid is not None
        assert instructions.partno is not None
        for i, partition in enumerate(partitions):
            self._materializations_by_node_id[instructions.nid][instructions.partno + i] = partition

    def result_partition_set(self) -> PartitionSet[vPartition]:
        partitions = self._materializations_by_node_id[self.FINAL_RESULT]

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
'''


class ConstructionInstructions:
    def __init__(self, inputs: list[vPartition]) -> None:
        # Input partitions to run over.
        self.inputs = inputs

        # Instruction stack to execute.
        self.instruction_stack: list[Callable[[list[vPartition]], list[vPartition]]] = list()

        # Materialization location: materialize as partition partno for the given execution node.
        self._dynamic_schedule: RequiresMaterialization | None = None
        self._partno: int | None = None

    def add_instruction(self, instruction: Callable[[list[vPartition]], list[vPartition]]) -> None:
        """Add an instruction to the stack that will run for this partition."""
        self.assert_not_marked()
        self.instruction_stack.append(instruction)

    def mark_for_materialization(self, dynamic_schedule: RequiresMaterialization, partno: int) -> None:
        """Mark this list of instructions to be materialized.

        nid: Node ID to materialize for.
        partno: The index within the plan node to materialize this partition at.

        Once marked for materialization, this object cannot be further modified.
        """
        self.assert_not_marked()
        self._dynamic_schedule = dynamic_schedule
        self._partno = partno

    def marked_for_materialization(self) -> bool:
        return self._dynamic_schedule is not None and self._partno is not None

    def assert_not_marked(self) -> None:
        assert (
            not self.marked_for_materialization()
        ), f"Partition already instructed to materialize for node ID {self._dynamic_schedule}, partition index {self._partno}"

    def report_completed(self, results: list[vPartition]) -> None:
        assert self._dynamic_schedule is not None
        assert self._partno is not None
        self._dynamic_schedule.register_completed_dependencies(results, self._partno)
