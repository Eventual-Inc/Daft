from __future__ import annotations

from abc import abstractmethod
from collections import defaultdict
from typing import Callable

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
            return InMemoryScanHandler(node)
        elif isinstance(node, logical_plan.Filter):
            return FilterHandler(node)
        elif isinstance(node, logical_plan.GlobalLimit):
            return GlobalLimitHandler(node)
        elif isinstance(node, logical_plan.Repartition):
            return RepartitionHandler(node)
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


class InMemoryScanHandler(DynamicSchedule):
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


class FilterHandler(DynamicSchedule):
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


class GlobalLimitHandler(RequiresMaterialization):
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


class RepartitionHandler(RequiresMaterialization):
    def __init__(self, repartition: logical_plan.Repartition) -> None:
        super().__init__()

        self._reduces_emitted = 0

        self._repartition = repartition

        [child_node] = repartition._children()
        self._source = DynamicSchedule.handle_logical_node(child_node)

    def __next__(self) -> ConstructionInstructions | None:
        # Have we emitted all reduce partitions?
        if self._reduces_emitted == self._repartition.num_partitions():
            raise StopIteration

        # We haven't emitted all reduce partitions yet and need to emit more.
        # To do this, we must first ensure all shufflemaps are dispatched.
        # See if we can dispatch any more shufflemaps.
        try:
            construct = next(self._source)
            if construct is None or construct.marked_for_materialization():
                return construct

            # Choose the correct instruction to dispatch based on shuffle op.
            if self._repartition._scheme == PartitionScheme.RANDOM:
                construct.add_instruction(self._fanout_random(num_outputs=self._repartition.num_partitions()))
            elif self._repartition._scheme == PartitionScheme.HASH:
                construct.add_instruction(
                    self._fanout_hash(
                        num_outputs=self._repartition.num_partitions(),
                        partition_by=self._repartition._partition_by,
                    )
                )
            else:
                raise RuntimeError(f"Unimplemented partitioning scheme {self._repartition._scheme}")

            self._prepare_to_materialize(construct, num_results=self._repartition.num_partitions())

            return construct

        except StopIteration:
            # No more shufflemaps to dispatch; continue on.
            pass

        # All shufflemaps have been dispatched; see if we can dispatch the next reduce.
        # The kth reduce's dependencies are the kth element of every batch of shufflemap outputs.
        # Here, k := reduces_emitted and batchlength := repartition.num_partitions()
        maybe_reduce_dependencies = [
            partition
            for i, partition in enumerate(self._materializing_dependencies)
            if i % self._repartition.num_partitions() == self._reduces_emitted
        ]
        reduce_dependencies = [x for x in maybe_reduce_dependencies if x is not None]
        if len(reduce_dependencies) != len(maybe_reduce_dependencies):
            # Need to wait for some shufflemaps to complete.
            return None

        new_construct = ConstructionInstructions(reduce_dependencies)
        new_construct.add_instruction(self._merge())
        self._reduces_emitted += 1
        return new_construct

    @staticmethod
    def _fanout_hash(num_outputs: int, partition_by: ExpressionList) -> Callable[[list[vPartition]], list[vPartition]]:
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
    def _fanout_random(num_outputs: int) -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            [input] = inputs
            partitions_with_ids = RepartitionRandomOp.map_fn(
                input=input,
                output_partitions=num_outputs,
            )
            return [partition for i, partition in sorted(partitions_with_ids.items())]

        return instruction

    @staticmethod
    def _merge() -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            return [vPartition.merge_partitions(inputs)]

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
