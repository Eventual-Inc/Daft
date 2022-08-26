from __future__ import annotations

import copyreg
import io
import multiprocessing
import multiprocessing as mp
import pickle
from dataclasses import dataclass
from typing import Callable, ClassVar, Dict, List, Optional, Tuple, Type

import pandas as pd

from daft.execution.execution_plan import ExecutionPlan
from daft.execution.logical_op_runners import (
    LogicalGlobalOpRunner,
    LogicalPartitionOpRunner,
)
from daft.internal.rule_runner import FixedPointPolicy, Once, RuleBatch, RuleRunner
from daft.logical.logical_plan import LogicalPlan
from daft.logical.optimizer import (
    DropRepartition,
    FoldProjections,
    PushDownLimit,
    PushDownPredicates,
)
from daft.logical.schema import ExpressionList
from daft.runners.partitioning import PartID, PartitionManager, PartitionSet, vPartition
from daft.runners.runner import Runner
from daft.runners.shuffle_ops import (
    CoalesceOp,
    RepartitionHashOp,
    RepartitionRandomOp,
    ShuffleOp,
    Shuffler,
    SortOp,
)


class ForkingPickler5(pickle.Pickler):
    """Pickler subclass used by multiprocessing."""

    _extra_reducers = {}
    _copyreg_dispatch_table = copyreg.dispatch_table

    def __init__(self, *args):
        super().__init__(*args)
        self.dispatch_table = self._copyreg_dispatch_table.copy()
        self.dispatch_table.update(self._extra_reducers)

    @classmethod
    def register(cls, type, reduce):
        """Register a reduce function for a type."""
        cls._extra_reducers[type] = reduce

    @classmethod
    def dumps(cls, obj, protocol=5):
        buf = io.BytesIO()
        cls(buf, protocol).dump(obj)
        return buf.getbuffer()

    loads = pickle.loads


register = ForkingPickler5.register


def dump(obj, file, protocol=5):
    """Replacement for pickle.dump() using ForkingPickler."""
    ForkingPickler5(file, protocol).dump(obj)


from multiprocessing.reduction import AbstractReducer

_POOL = None


def get_pool():
    global _POOL
    if _POOL is None:
        _POOL = multiprocessing.Pool(10)
    return _POOL


class Pickle5Reducer(AbstractReducer):
    ForkingPickler = ForkingPickler5
    register = ForkingPickler5.register
    dump = staticmethod(dump)


mp.get_context().reducer = Pickle5Reducer


@dataclass
class LocalPartitionSet(PartitionSet[vPartition]):
    _partitions: Dict[PartID, vPartition]

    def to_pandas(self, schema: Optional[ExpressionList] = None) -> "pd.DataFrame":
        partition_ids = sorted(list(self._partitions.keys()))
        assert partition_ids[0] == 0
        assert partition_ids[-1] + 1 == len(partition_ids)
        part_dfs = [self._partitions[pid].to_pandas(schema=schema) for pid in partition_ids]
        return pd.concat([pdf for pdf in part_dfs if not pdf.empty], ignore_index=True)

    def get_partition(self, idx: PartID) -> vPartition:
        return self._partitions[idx]

    def set_partition(self, idx: PartID, part: vPartition) -> None:
        self._partitions[idx] = part

    def delete_partition(self, idx: PartID) -> None:
        del self._partitions[idx]

    def has_partition(self, idx: PartID) -> bool:
        return idx in self._partitions

    def __len__(self) -> int:
        return sum(self.len_of_partitions())

    def len_of_partitions(self) -> List[int]:
        partition_ids = sorted(list(self._partitions.keys()))
        return [len(self._partitions[pid]) for pid in partition_ids]

    def num_partitions(self) -> int:
        return len(self._partitions)


class PyRunnerSimpleShuffler(Shuffler):
    def run(self, input: PartitionSet, num_target_partitions: int) -> PartitionSet:
        map_args = self._map_args if self._map_args is not None else {}
        reduce_args = self._reduce_args if self._reduce_args is not None else {}

        source_partitions = input.num_partitions()
        map_results = [
            self.map_fn(input=input.get_partition(i), output_partitions=num_target_partitions, **map_args)
            for i in range(source_partitions)
        ]
        reduced_results = []
        for t in range(num_target_partitions):
            reduced_part = self.reduce_fn(
                [map_results[i][t] for i in range(source_partitions) if t in map_results[i]], **reduce_args
            )
            reduced_results.append(reduced_part)

        return LocalPartitionSet({i: part for i, part in enumerate(reduced_results)})


from functools import partial

import tqdm

DISABLE_PBAR = 0


class PyRunnerMPSimpleShuffler(Shuffler):
    def _map_wrapper(self, partition, num_target_partitions, **map_args):
        return partition.partition_id, self.map_fn(partition, output_partitions=num_target_partitions, **map_args)

    def run(self, input: PartitionSet, num_target_partitions: int) -> PartitionSet:
        map_args = self._map_args if self._map_args is not None else {}
        reduce_args = self._reduce_args if self._reduce_args is not None else {}
        source_partitions = input.num_partitions()
        if source_partitions > 1:
            to_map = [input.get_partition(i) for i in range(source_partitions) if len(input.get_partition(i)) > 0]
            map_results = list(
                tqdm.tqdm(
                    get_pool().imap_unordered(
                        partial(self._map_wrapper, num_target_partitions=num_target_partitions, **map_args),
                        to_map,
                    ),
                    total=source_partitions,
                    desc=f"{self.__class__.__name__}:Map",
                    disable=DISABLE_PBAR,
                )
            )
            to_fill = [dict() for _ in range(source_partitions)]
            for i, part in map_results:
                to_fill[i] = part
            map_results = to_fill
        else:
            map_results = [
                partial(self.map_fn, output_partitions=num_target_partitions, **map_args)(input.get_partition(0))
            ]

        reduced_results = list(
            tqdm.tqdm(
                get_pool().imap_unordered(
                    partial(self.reduce_fn, **reduce_args),
                    [
                        [map_results[i][t] for i in range(source_partitions) if t in map_results[i]]
                        for t in range(num_target_partitions)
                    ],
                ),
                total=num_target_partitions,
                desc=f"{self.__class__.__name__}:Reduce",
                disable=DISABLE_PBAR,
            )
        )
        return LocalPartitionSet({part.partition_id: part for part in reduced_results})


class PyRunnerRepartitionRandom(PyRunnerMPSimpleShuffler, RepartitionRandomOp):
    ...


class PyRunnerRepartitionHash(PyRunnerMPSimpleShuffler, RepartitionHashOp):
    ...


class PyRunnerCoalesceOp(PyRunnerMPSimpleShuffler, CoalesceOp):
    ...


class PyRunnerSortOp(PyRunnerMPSimpleShuffler, SortOp):
    ...


class LocalLogicalPartitionOpRunner(LogicalPartitionOpRunner):
    def _run_node_list_single_wrapper(
        self, input_values: Tuple[Dict[int, PartitionSet], int], nodes: List[LogicalPlan]
    ) -> vPartition:
        inputs, part_idx = input_values
        return part_idx, self.run_node_list_single_partition(inputs=inputs, nodes=nodes, partition_id=part_idx)

    def run_node_list(
        self, inputs: Dict[int, PartitionSet], nodes: List[LogicalPlan], num_partitions: int
    ) -> PartitionSet:
        result = LocalPartitionSet({})
        print(nodes)
        result_partitions = list(
            tqdm.tqdm(
                get_pool().imap_unordered(
                    partial(self._run_node_list_single_wrapper, nodes=nodes),
                    [({nid: inputs[nid].get_partition(i) for nid in inputs}, i) for i in range(num_partitions)],
                ),
                total=num_partitions,
                desc=f"{self.__class__.__name__}",
                disable=DISABLE_PBAR,
            )
        )
        result_partitions.sort(key=lambda x: x[0])
        for idx, part in result_partitions:
            result.set_partition(idx, part)
        return result


class LocalLogicalGlobalOpRunner(LogicalGlobalOpRunner):
    shuffle_ops: ClassVar[Dict[Type[ShuffleOp], Type[Shuffler]]] = {
        RepartitionRandomOp: PyRunnerRepartitionRandom,
        RepartitionHashOp: PyRunnerRepartitionHash,
        CoalesceOp: PyRunnerCoalesceOp,
        SortOp: PyRunnerSortOp,
    }

    def map_partitions(self, pset: PartitionSet, func: Callable[[vPartition], vPartition], **kwargs) -> PartitionSet:
        return LocalPartitionSet({i: func(pset.get_partition(i), **kwargs) for i in range(pset.num_partitions())})

    def reduce_partitions(
        self, pset: PartitionSet, func: Callable[[List[vPartition]], vPartition], **kwargs
    ) -> vPartition:
        data = [pset.get_partition(i) for i in range(pset.num_partitions())]
        return func(data, **kwargs)


class PyRunner(Runner):
    def __init__(self) -> None:

        self._part_manager = PartitionManager(lambda: LocalPartitionSet({}))
        self._part_op_runner = LocalLogicalPartitionOpRunner()
        self._global_op_runner = LocalLogicalGlobalOpRunner()
        self._optimizer = RuleRunner(
            [
                RuleBatch(
                    "SinglePassPushDowns",
                    Once,
                    [PushDownPredicates(), FoldProjections(), DropRepartition()],
                ),
                RuleBatch(
                    "PushDownLimits",
                    FixedPointPolicy(3),
                    [PushDownLimit()],
                ),
            ]
        )

    def run(self, plan: LogicalPlan) -> PartitionSet:
        plan = self._optimizer.optimize(plan)
        # plan.to_dot_file()
        exec_plan = ExecutionPlan.plan_from_logical(plan)
        result_partition_set: PartitionSet
        for exec_op in exec_plan.execution_ops:

            data_deps = exec_op.data_deps
            input_partition_set = {nid: self._part_manager.get_partition_set(nid) for nid in data_deps}

            if exec_op.is_global_op:
                input_partition_set = {nid: self._part_manager.get_partition_set(nid) for nid in data_deps}
                result_partition_set = self._global_op_runner.run_node_list(input_partition_set, exec_op.logical_ops)
            else:
                result_partition_set = self._part_op_runner.run_node_list(
                    input_partition_set, exec_op.logical_ops, exec_op.num_partitions
                )

            for child_id in data_deps:
                self._part_manager.rm(child_id)

            self._part_manager.put_partition_set(exec_op.logical_ops[-1].id(), result_partition_set)

        last = exec_plan.execution_ops[-1].logical_ops[-1]
        return self._part_manager.get_partition_set(last.id())
