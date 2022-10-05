from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, ClassVar, Dict, List, Optional, Type, cast

import ray
from loguru import logger

from daft.execution.execution_plan import ExecutionPlan
from daft.execution.logical_op_runners import (
    LogicalGlobalOpRunner,
    LogicalPartitionOpRunner,
    ReduceType,
)
from daft.internal.rule_runner import FixedPointPolicy, Once, RuleBatch, RuleRunner
from daft.logical.logical_plan import LogicalPlan
from daft.logical.optimizer import (
    DropRepartition,
    FoldProjections,
    PruneColumns,
    PushDownClausesIntoScan,
    PushDownLimit,
    PushDownPredicates,
)
from daft.resource_request import ResourceRequest
from daft.runners.partitioning import PartID, PartitionManager, PartitionSet, vPartition
from daft.runners.profiler import profiler
from daft.runners.runner import Runner
from daft.runners.shuffle_ops import (
    CoalesceOp,
    RepartitionHashOp,
    RepartitionRandomOp,
    ShuffleOp,
    Shuffler,
    SortOp,
)


@dataclass
class RayPartitionSet(PartitionSet[ray.ObjectRef]):
    _partitions: Dict[PartID, ray.ObjectRef]

    def _get_all_vpartitions(self) -> List[vPartition]:
        partition_ids = sorted(list(self._partitions.keys()))
        assert partition_ids[0] == 0
        assert partition_ids[-1] + 1 == len(partition_ids)
        return cast(List[vPartition], ray.get([self._partitions[pid] for pid in partition_ids]))

    def get_partition(self, idx: PartID) -> ray.ObjectRef:
        return self._partitions[idx]

    def set_partition(self, idx: PartID, part: ray.ObjectRef) -> None:
        self._partitions[idx] = part

    def delete_partition(self, idx: PartID) -> None:
        del self._partitions[idx]

    def has_partition(self, idx: PartID) -> bool:
        return idx in self._partitions

    def __len__(self) -> int:
        return sum(self.len_of_partitions())

    def len_of_partitions(self) -> List[int]:
        partition_ids = sorted(list(self._partitions.keys()))

        @ray.remote
        def remote_len(p: vPartition) -> int:
            return len(p)

        result: List[int] = ray.get([remote_len.remote(self._partitions[pid]) for pid in partition_ids])
        return result

    def num_partitions(self) -> int:
        return len(self._partitions)


class RayRunnerSimpleShuffler(Shuffler):
    def run(self, input: PartitionSet, num_target_partitions: int) -> PartitionSet:
        map_args = self._map_args if self._map_args is not None else {}
        reduce_args = self._reduce_args if self._reduce_args is not None else {}
        ray_expr_eval_task_options = _get_ray_task_options(self._expr_eval_resource_request)

        source_partitions = input.num_partitions()

        @ray.remote(scheduling_strategy="SPREAD")
        def reduce_wrapper(*to_reduce: vPartition):
            return self.reduce_fn(list(to_reduce), **reduce_args)

        @ray.remote(num_returns=num_target_partitions)
        def map_wrapper(input: vPartition):
            output_dict = self.map_fn(input=input, output_partitions=num_target_partitions, **map_args)
            output_list: List[Optional[vPartition]] = [None for _ in range(num_target_partitions)]
            for part_id, part in output_dict.items():
                output_list[part_id] = part

            if num_target_partitions == 1:
                return part
            else:
                return output_list

        map_results = [
            map_wrapper.options(**ray_expr_eval_task_options).remote(input=input.get_partition(i))
            for i in range(source_partitions)
        ]

        if num_target_partitions == 1:
            ray.wait(map_results)
        else:
            ray.wait([ref for block in map_results for ref in block])

        reduced_results = []
        for t in range(num_target_partitions):
            if num_target_partitions == 1:
                map_subset = map_results
            else:
                map_subset = [map_results[i][t] for i in range(source_partitions)]
            # NOTE: not all reduce ops actually require ray_expr_eval_task_options. This is an area for
            # potential improvement for repartitioning operations which only require the task options for mapping
            reduced_part = reduce_wrapper.options(**ray_expr_eval_task_options).remote(*map_subset)
            reduced_results.append(reduced_part)

        return RayPartitionSet({i: part for i, part in enumerate(reduced_results)})


class RayRunnerRepartitionRandom(RayRunnerSimpleShuffler, RepartitionRandomOp):
    ...


class RayRunnerRepartitionHash(RayRunnerSimpleShuffler, RepartitionHashOp):
    ...


class RayRunnerCoalesceOp(RayRunnerSimpleShuffler, CoalesceOp):
    ...


class RayRunnerSortOp(RayRunnerSimpleShuffler, SortOp):
    ...


@ray.remote
def _ray_partition_single_part_runner(
    *input_parts: vPartition,
    op_runner: RayLogicalPartitionOpRunner,
    input_node_ids: List[int],
    nodes: List[LogicalPlan],
    partition_id: int,
) -> vPartition:
    input_partitions = {id: val for id, val in zip(input_node_ids, input_parts)}
    return op_runner.run_node_list_single_partition(input_partitions, nodes=nodes, partition_id=partition_id)


def _get_ray_task_options(resource_request: ResourceRequest) -> Dict[str, Any]:
    options = {}
    if resource_request.num_cpus is not None:
        options["num_cpus"] = resource_request.num_cpus
    if resource_request.num_gpus is not None:
        options["num_gpus"] = resource_request.num_gpus
    return options


class RayLogicalPartitionOpRunner(LogicalPartitionOpRunner):
    def run_node_list(
        self,
        inputs: Dict[int, PartitionSet],
        nodes: List[LogicalPlan],
        num_partitions: int,
        resource_request: ResourceRequest,
    ) -> PartitionSet:
        single_part_runner = _ray_partition_single_part_runner.options(**_get_ray_task_options(resource_request))
        node_ids = list(inputs.keys())
        results = []
        for i in range(num_partitions):
            input_partitions = [inputs[nid].get_partition(i) for nid in node_ids]
            result_partition = single_part_runner.remote(
                *input_partitions, input_node_ids=node_ids, op_runner=self, nodes=nodes, partition_id=i
            )
            results.append(result_partition)
        return RayPartitionSet({i: part for i, part in enumerate(results)})


class RayLogicalGlobalOpRunner(LogicalGlobalOpRunner):
    shuffle_ops: ClassVar[Dict[Type[ShuffleOp], Type[Shuffler]]] = {
        RepartitionRandomOp: RayRunnerRepartitionRandom,
        RepartitionHashOp: RayRunnerRepartitionHash,
        CoalesceOp: RayRunnerCoalesceOp,
        SortOp: RayRunnerSortOp,
    }

    def map_partitions(
        self, pset: PartitionSet, func: Callable[[vPartition], vPartition], resource_request: ResourceRequest
    ) -> PartitionSet:
        remote_func = ray.remote(func).options(**_get_ray_task_options(resource_request))
        return RayPartitionSet({i: remote_func.remote(pset.get_partition(i)) for i in range(pset.num_partitions())})

    def reduce_partitions(self, pset: PartitionSet, func: Callable[[List[vPartition]], ReduceType]) -> ReduceType:
        # @ray.remote
        # def remote_func(*parts: vPartition) -> ReduceType:
        #     return func(list(parts))

        data = [pset.get_partition(i) for i in range(pset.num_partitions())]
        result: ReduceType = func(ray.get(data))
        return result


class RayRunner(Runner):
    def __init__(self, address: Optional[str]) -> None:
        if ray.is_initialized():
            logger.warning(f"Ray has already been initialized, Daft will reuse the existing Ray context")
        else:
            ray.init(address=address)
        self._part_manager = PartitionManager(lambda: RayPartitionSet({}))
        self._part_op_runner = RayLogicalPartitionOpRunner()
        self._global_op_runner = RayLogicalGlobalOpRunner()
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
                    [PushDownLimit(), DropRepartition()],
                ),
            ]
        )

    def run(self, plan: LogicalPlan) -> PartitionSet:
        plan = self._optimizer.optimize(plan)
        exec_plan = ExecutionPlan.plan_from_logical(plan)
        result_partition_set: PartitionSet
        with profiler("profile.json"):
            for exec_op in exec_plan.execution_ops:
                data_deps = exec_op.data_deps
                input_partition_set = {nid: self._part_manager.get_partition_set(nid) for nid in data_deps}

                if exec_op.is_global_op:
                    input_partition_set = {nid: self._part_manager.get_partition_set(nid) for nid in data_deps}
                    result_partition_set = self._global_op_runner.run_node_list(
                        input_partition_set, exec_op.logical_ops
                    )
                else:
                    result_partition_set = self._part_op_runner.run_node_list(
                        input_partition_set, exec_op.logical_ops, exec_op.num_partitions, exec_op.resource_request()
                    )
                del input_partition_set
                for child_id in data_deps:
                    self._part_manager.rm(child_id)
                self._part_manager.put_partition_set(exec_op.logical_ops[-1].id(), result_partition_set)
                del result_partition_set
            last_id = exec_plan.execution_ops[-1].logical_ops[-1].id()
            last_pset = self._part_manager.get_partition_set(last_id)
            self._part_manager.clear()
            return last_pset
