from __future__ import annotations

from abc import abstractmethod
from collections import defaultdict
from typing import Callable, Iterator

from daft.context import get_context
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
from daft.runners.shuffle_ops import RepartitionHashOp, RepartitionRandomOp, SortOp


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
            assert next_construction is not None
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

    DEPENDENCIES = "dependencies"

    def __init__(self) -> None:
        # Materialized partitions that this dynamic schedule needs to do its job.
        # `None` denotes a partition whose materialization is in-progress.
        self._materializations: dict[str, list[vPartition | None]] = defaultdict(list)

    def __iter__(self) -> Iterator[ConstructionInstructions | None]:
        return self

    @abstractmethod
    def __next__(self) -> ConstructionInstructions | None:
        """
        Raises StopIteration if there are no further instructions to give for this schedule.
        Returns None if there is nothing we can do for now (i.e. must wait for other partitions to finish).
        """
        raise NotImplementedError()

    def register_completed_materialization(self, partitions: list[vPartition], label: str, partno: int) -> None:
        for i, partition in enumerate(partitions):
            assert self._materializations[label][partno + i] is None, self._materializations[label][partno + i]
            self._materializations[label][partno + i] = partition

    def _prepare_to_materialize(
        self, instructions: ConstructionInstructions, label: str = DEPENDENCIES, num_results: int = 1
    ) -> None:
        instructions.mark_for_materialization(self, label, len(self._materializations[label]))
        self._materializations[label] += [None] * num_results

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
        elif isinstance(node, logical_plan.Sort):
            return SortHandler(node)
        else:
            raise RuntimeError(f"Unsupported plan type {node}")


class InMemoryScanHandler(DynamicSchedule):
    def __init__(self, im_scan: logical_plan.InMemoryScan) -> None:
        super().__init__()
        # The backing partitions are already materialized; put it into a FromPartitions.
        pset = get_context().runner().get_partition_set_from_cache(im_scan._cache_entry.key).value
        assert pset is not None
        partitions = pset.values()
        self._from_partitions = FromPartitions(partitions)

    def __next__(self) -> ConstructionInstructions | None:
        return next(self._from_partitions)


class FromPartitions(DynamicSchedule):
    def __init__(self, partitions: list[vPartition]) -> None:
        super().__init__()
        self._partitions = partitions
        self._num_dispatched = 0

    def __next__(self) -> ConstructionInstructions | None:
        if self._num_dispatched == len(self._partitions):
            raise StopIteration

        partition = self._partitions[self._num_dispatched]
        self._num_dispatched += 1

        new_construct = ConstructionInstructions([partition])
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


class GlobalLimitHandler(DynamicSchedule):
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

        dependencies = self._materializations[self.DEPENDENCIES]

        # Evaluate progress so far.
        # Are we done with the limit?
        if self._remaining_limit == 0:

            # We are done with the limit,
            # but the current implementation of LogicalPlan still requires
            # a mandated number of partitions to be returned.
            # Return empty partitions until we have returned enough.
            if not dependencies:
                # We need at least one materialized partition to get the correct schema to return.
                pass

            elif self._continue_from_partition < self._global_limit.num_partitions():
                # We need at least one materialized partition to get the correct schema to return.
                if dependencies[0] is None:
                    return None

                new_construct = ConstructionInstructions([dependencies[0]])
                new_construct.add_instruction(self._local_limit(0))

                self._continue_from_partition += 1
                return new_construct

            else:
                raise StopIteration

        # We're not done; check to see if we can return a global limit partition.
        # Is the next local limit partition materialized?
        # If so, update global limit progress, and return the local limit.
        if self._continue_from_partition < len(dependencies):
            next_partition = dependencies[self._continue_from_partition]
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


class SortHandler(DynamicSchedule):

    SAMPLES = "samples"
    BOUNDARIES = "boundaries"

    def __init__(self, sort: logical_plan.Sort) -> None:
        super().__init__()
        self._sort = sort

        [child_node] = sort._children()
        self._source = DynamicSchedule.handle_logical_node(child_node)

        # The final step of the sort.
        self._fanout_reduce: ExecuteFanoutReduce | None = None

    def __next__(self) -> ConstructionInstructions | None:

        # First, materialize the child node.
        try:
            construct = next(self._source)
            if construct is None or construct.marked_for_materialization():
                return construct

            self._prepare_to_materialize(construct)

            return construct

        except StopIteration:
            # Child node fully materialized.
            pass

        # Sample partitions to get sort boundaries.
        source_partitions = self._materializations[self.DEPENDENCIES]
        sample_partitions = self._materializations[self.SAMPLES]
        if len(sample_partitions) < len(source_partitions):
            source = source_partitions[len(sample_partitions)]
            if source is None:
                return None
            construct_sample = ConstructionInstructions([source])
            construct_sample.add_instruction(self._map_to_samples(self._sort._sort_by))
            self._prepare_to_materialize(construct_sample, label=self.SAMPLES)
            return construct_sample

        # Sample partitions are done, reduce to get quantiles.
        if not len(self._materializations[self.BOUNDARIES]):
            finished_samples = [_ for _ in sample_partitions if _ is not None]
            if len(finished_samples) != len(sample_partitions):
                return None

            construct_boundaries = ConstructionInstructions(finished_samples)
            construct_boundaries.add_instruction(
                self._reduce_to_quantiles(
                    sort_by=self._sort._sort_by,
                    descending=self._sort._descending,
                    num_quantiles=self._sort.num_partitions(),
                )
            )
            self._prepare_to_materialize(construct_boundaries, label=self.BOUNDARIES)
            return construct_boundaries

        [boundaries_partition] = self._materializations[self.BOUNDARIES]
        if boundaries_partition is None:
            return None

        # Boundaries are ready; execute fanout-reduce.
        finished_dependencies = [_ for _ in source_partitions if _ is not None]  # for mypy; these are all done
        assert len(finished_dependencies) == len(source_partitions)

        if self._fanout_reduce is None:
            from_partitions = FromPartitions(finished_dependencies)
            fanout_fn = self._fanout_range(
                sort_by=self._sort._sort_by,
                descending=self._sort._descending,
                boundaries=boundaries_partition,
                num_outputs=self._sort.num_partitions(),
            )
            reduce_fn = self._merge_and_sort(
                sort_by=self._sort._sort_by,
                descending=self._sort._descending,
            )
            self._fanout_reduce = ExecuteFanoutReduce(
                source=from_partitions,
                num_outputs=self._sort.num_partitions(),
                fanout_fn=fanout_fn,
                reduce_fn=reduce_fn,
            )

        return next(self._fanout_reduce)

    @staticmethod
    def _map_to_samples(
        sort_by: ExpressionList, num_samples: int = 20
    ) -> Callable[[list[vPartition]], list[vPartition]]:
        """From logical_op_runners."""

        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            [input] = inputs
            result = (
                input.sample(num_samples)
                .eval_expression_list(sort_by)
                .filter(ExpressionList([~e.to_column_expression().is_null() for e in sort_by]).resolve(sort_by))
            )
            return [result]

        return instruction

    @staticmethod
    def _reduce_to_quantiles(
        sort_by: ExpressionList, descending: list[bool], num_quantiles: int
    ) -> Callable[[list[vPartition]], list[vPartition]]:
        """From logical_op_runners."""

        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            merged = vPartition.merge_partitions(inputs, verify_partition_id=False)
            merged_sorted = merged.sort(sort_by, descending=descending)
            result = merged_sorted.quantiles(num_quantiles)
            return [result]

        return instruction

    @staticmethod
    def _fanout_range(
        sort_by: ExpressionList, descending: list[bool], boundaries: vPartition, num_outputs: int
    ) -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            [input] = inputs
            partitions_with_ids = SortOp.map_fn(
                input=input,
                output_partitions=num_outputs,
                exprs=sort_by,
                boundaries=boundaries,
                descending=descending,
            )
            return [partition for _, partition in sorted(partitions_with_ids.items())]

        return instruction

    @staticmethod
    def _merge_and_sort(
        sort_by: ExpressionList, descending: list[bool]
    ) -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            partition = SortOp.reduce_fn(
                mapped_outputs=inputs,
                exprs=sort_by,
                descending=descending,
            )
            return [partition]

        return instruction


class RepartitionHandler(DynamicSchedule):
    def __init__(self, repartition: logical_plan.Repartition) -> None:
        super().__init__()

        [child_node] = repartition._children()
        self._child_source = DynamicSchedule.handle_logical_node(child_node)

        # Translate PartitionScheme to the appropriate fanout_fn
        if repartition._scheme == PartitionScheme.RANDOM:
            fanout_fn = self._fanout_random(num_outputs=repartition.num_partitions())
        elif repartition._scheme == PartitionScheme.HASH:
            fanout_fn = self._fanout_hash(
                num_outputs=repartition.num_partitions(),
                partition_by=repartition._partition_by,
            )
        else:
            raise RuntimeError(f"Unimplemented partitioning scheme {repartition._scheme}")

        self._fanout_reduce = ExecuteFanoutReduce(
            source=self._child_source,
            num_outputs=repartition.num_partitions(),
            fanout_fn=fanout_fn,
            reduce_fn=self._merge(),
        )

    def __next__(self) -> ConstructionInstructions | None:
        return next(self._fanout_reduce)

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
            return [partition for _, partition in sorted(partitions_with_ids.items())]

        return instruction

    @staticmethod
    def _merge() -> Callable[[list[vPartition]], list[vPartition]]:
        def instruction(inputs: list[vPartition]) -> list[vPartition]:
            return [vPartition.merge_partitions(inputs)]

        return instruction


class ExecuteFanoutReduce(DynamicSchedule):
    def __init__(
        self,
        source: DynamicSchedule,
        num_outputs: int,
        fanout_fn: Callable[[list[vPartition]], list[vPartition]],
        reduce_fn: Callable[[list[vPartition]], list[vPartition]],
    ) -> None:
        super().__init__()
        self._source = source
        self._num_outputs = num_outputs
        self._fanout_fn = fanout_fn
        self._reduce_fn = reduce_fn

        self._reduces_emitted = 0

    def __next__(self) -> ConstructionInstructions | None:
        # Dispatch shufflemaps.
        try:
            construct = next(self._source)
            if construct is None or construct.marked_for_materialization():
                return construct

            construct.add_instruction(self._fanout_fn)

            self._prepare_to_materialize(construct, num_results=self._num_outputs)
            return construct

        except StopIteration:
            # No more shufflemaps to dispatch, continue to reduce.
            pass

        # All shufflemaps have been dispatched; see if we can dispatch the next ("kth") reduce.
        # The kth reduce's dependencies are the kth element of every batch of shufflemap outputs.
        # Here, k := reduces_emitted and batchlength := num_outputs
        if self._reduces_emitted == self._num_outputs:
            raise StopIteration

        kth_reduce_dependencies = [
            partition
            for i, partition in enumerate(self._materializations[self.DEPENDENCIES])
            if i % self._num_outputs == self._reduces_emitted
        ]
        finished_reduce_dependencies = [_ for _ in kth_reduce_dependencies if _ is not None]
        if len(finished_reduce_dependencies) != len(kth_reduce_dependencies):
            # Need to wait for some shufflemaps to complete.
            return None

        construct_reduce = ConstructionInstructions(finished_reduce_dependencies)
        construct_reduce.add_instruction(self._reduce_fn)
        self._reduces_emitted += 1
        return construct_reduce


class ExecuteMaterialize(DynamicSchedule):
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
        dependencies = self._materializations[self.DEPENDENCIES]
        finished_partitions = [p for p in dependencies if p is not None]
        assert len(finished_partitions) == len(
            dependencies
        ), f"Not all partitions have finished materializing yet in results: {dependencies}"

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
        self._dynamic_schedule: DynamicSchedule | None = None
        self._label: str | None = None
        self._partno: int | None = None

    def add_instruction(self, instruction: Callable[[list[vPartition]], list[vPartition]]) -> None:
        """Add an instruction to the stack that will run for this partition."""
        self.assert_not_marked()
        self.instruction_stack.append(instruction)

    def mark_for_materialization(self, dynamic_schedule: DynamicSchedule, label: str, partno: int) -> None:
        """Mark this list of instructions to be materialized.

        nid: Node ID to materialize for.
        partno: The index within the plan node to materialize this partition at.

        Once marked for materialization, this object cannot be further modified.
        """
        self.assert_not_marked()
        self._dynamic_schedule = dynamic_schedule
        self._label = label
        self._partno = partno

    def marked_for_materialization(self) -> bool:
        return all(_ is not None for _ in (self._dynamic_schedule, self._label, self._partno))

    def assert_not_marked(self) -> None:
        assert (
            not self.marked_for_materialization()
        ), f"Partition already instructed to materialize for node ID {self._dynamic_schedule}, label {self._label}, partition index {self._partno}"

    def report_completed(self, results: list[vPartition]) -> None:
        assert self._dynamic_schedule is not None
        assert self._label is not None
        assert self._partno is not None
        self._dynamic_schedule.register_completed_materialization(results, self._label, self._partno)
