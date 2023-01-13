from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Iterator

import pyarrow as pa
import ray
from loguru import logger

from daft.execution import physical_plan_factory
from daft.execution.execution_plan import ExecutionPlan
from daft.execution.execution_step import (
    Instruction,
    MaterializationRequest,
    MaterializationRequestBase,
    MaterializationRequestMulti,
    MaterializationResult,
)
from daft.execution.logical_op_runners import (
    LogicalGlobalOpRunner,
    LogicalPartitionOpRunner,
    ReduceType,
)
from daft.expressions import ColumnExpression
from daft.filesystem import glob_path
from daft.internal.rule_runner import FixedPointPolicy, Once, RuleBatch, RuleRunner
from daft.logical import logical_plan
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
from daft.resource_request import ResourceRequest
from daft.runners.blocks import ArrowDataBlock, zip_blocks_as_py
from daft.runners.partitioning import (
    PartID,
    PartitionCacheEntry,
    PartitionMetadata,
    PartitionSet,
    PartitionSetFactory,
    vPartition,
)
from daft.runners.profiler import profiler
from daft.runners.pyrunner import LocalPartitionSet
from daft.runners.runner import Runner
from daft.runners.shuffle_ops import (
    CoalesceOp,
    RepartitionHashOp,
    RepartitionRandomOp,
    ShuffleOp,
    Shuffler,
    SortOp,
)
from daft.types import ExpressionType

if TYPE_CHECKING:
    from ray.data.block import Block as RayDatasetBlock
    from ray.data.dataset import Dataset as RayDataset

_RAY_FROM_ARROW_REFS_AVAILABLE = True
try:
    from ray.data import from_arrow_refs
except ImportError:
    _RAY_FROM_ARROW_REFS_AVAILABLE = False


@ray.remote
def _glob_path_into_vpartitions(path: str, schema: ExpressionList) -> list[tuple[PartID, vPartition]]:
    assert len(schema) == 1
    filepath_expr = list(schema)[0]
    filepaths = glob_path(path)

    # Hardcoded to 1 filepath per partition
    partition_refs = []
    for i, filepath in enumerate(filepaths):
        partition = vPartition.from_pydict({filepath_expr.name(): [filepath]}, schema=schema, partition_id=i)
        partition_ref = ray.put(partition)
        partition_refs.append((i, partition_ref))

    return partition_refs


@ray.remote
def _make_ray_block_from_vpartition(partition: vPartition) -> RayDatasetBlock:
    daft_blocks = {tile.column_name: tile.block for _, tile in partition.columns.items()}

    all_arrow = all(isinstance(daft_block, ArrowDataBlock) for daft_block in daft_blocks.values())
    if all_arrow:
        return pa.Table.from_pydict({colname: daft_block.data for colname, daft_block in daft_blocks.items()})

    colnames = list(daft_blocks.keys())
    blocks = list(daft_blocks.values())
    return [dict(zip(colnames, row_tuple)) for row_tuple in zip_blocks_as_py(*blocks)]


@dataclass
class RayPartitionSet(PartitionSet[ray.ObjectRef]):
    _partitions: dict[PartID, ray.ObjectRef]

    def items(self) -> list[tuple[PartID, ray.ObjectRef]]:
        return sorted(self._partitions.items())

    def _get_merged_vpartition(self) -> vPartition:
        ids_and_partitions = self.items()
        assert ids_and_partitions[0][0] == 0
        assert ids_and_partitions[-1][0] + 1 == len(ids_and_partitions)
        all_partitions = ray.get([part for id, part in ids_and_partitions])
        return vPartition.merge_partitions(all_partitions, verify_partition_id=False)

    def to_ray_dataset(self) -> RayDataset:
        if not _RAY_FROM_ARROW_REFS_AVAILABLE:
            raise ImportError(
                "Unable to import `ray.data.from_arrow_refs`. Please ensure that you have a compatible version of Ray >= 1.10 installed."
            )

        blocks = [_make_ray_block_from_vpartition.remote(self._partitions[k]) for k in self._partitions.keys()]
        # NOTE: although the Ray method is called `from_arrow_refs`, this method works also when the blocks are List[T] types
        # instead of Arrow tables as the codepath for Dataset creation is the same.
        return from_arrow_refs(blocks)

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

    def len_of_partitions(self) -> list[int]:
        partition_ids = sorted(list(self._partitions.keys()))

        @ray.remote
        def remote_len(p: vPartition) -> int:
            return len(p)

        result: list[int] = ray.get([remote_len.remote(self._partitions[pid]) for pid in partition_ids])
        return result

    def num_partitions(self) -> int:
        return len(self._partitions)

    def wait(self) -> None:
        ray.wait([o for o in self._partitions.values()])


class RayPartitionSetFactory(PartitionSetFactory[ray.ObjectRef]):
    def glob_filepaths(
        self,
        source_path: str,
    ) -> tuple[RayPartitionSet, ExpressionList]:
        schema = ExpressionList([ColumnExpression(self.FILEPATH_COLUMN_NAME, ExpressionType.string())]).resolve()
        partition_refs = ray.get(_glob_path_into_vpartitions.remote(source_path, schema))
        return RayPartitionSet({part_id: part for part_id, part in partition_refs}), schema


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
            output_list: list[vPartition | None] = [None for _ in range(num_target_partitions)]
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
    input_node_ids: list[int],
    nodes: list[logical_plan.LogicalPlan],
    partition_id: int,
) -> vPartition:
    input_partitions = {id: val for id, val in zip(input_node_ids, input_parts)}
    return op_runner.run_node_list_single_partition(input_partitions, nodes=nodes, partition_id=partition_id)


def _get_ray_task_options(resource_request: ResourceRequest) -> dict[str, Any]:
    options = {}
    # FYI: Ray's default resource behaviour is documented here:
    # https://docs.ray.io/en/latest/ray-core/tasks/resources.html
    if resource_request.num_cpus is not None:
        options["num_cpus"] = resource_request.num_cpus
    if resource_request.num_gpus is not None:
        options["num_gpus"] = resource_request.num_gpus
    if resource_request.memory_bytes is not None:
        options["memory"] = resource_request.memory_bytes
    return options


class RayLogicalPartitionOpRunner(LogicalPartitionOpRunner):
    def run_node_list(
        self,
        inputs: dict[int, PartitionSet],
        nodes: list[logical_plan.LogicalPlan],
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
    shuffle_ops: ClassVar[dict[type[ShuffleOp], type[Shuffler]]] = {
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

    def reduce_partitions(self, pset: PartitionSet, func: Callable[[list[vPartition]], ReduceType]) -> ReduceType:
        data = [pset.get_partition(i) for i in range(pset.num_partitions())]
        result: ReduceType = func(ray.get(data))
        return result


class RayRunner(Runner):
    def __init__(self, address: str | None) -> None:
        super().__init__()
        if ray.is_initialized():
            logger.warning(f"Ray has already been initialized, Daft will reuse the existing Ray context")
        else:
            ray.init(address=address)
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
                    [PushDownLimit(), DropRepartition(), DropProjections()],
                ),
            ]
        )

    def put_partition_set_into_cache(self, pset: PartitionSet) -> PartitionCacheEntry:
        if isinstance(pset, LocalPartitionSet):
            pset = RayPartitionSet({pid: ray.put(val) for pid, val in pset._partitions.items()})

        return self._part_set_cache.put_partition_set(pset=pset)

    def optimize(self, plan: logical_plan.LogicalPlan) -> logical_plan.LogicalPlan:
        return self._optimizer.optimize(plan)

    def partition_set_factory(self) -> PartitionSetFactory:
        return RayPartitionSetFactory()

    def run(self, plan: logical_plan.LogicalPlan) -> PartitionCacheEntry:
        plan = self.optimize(plan)
        exec_plan = ExecutionPlan.plan_from_logical(plan)
        result_partition_set: PartitionSet
        partition_intermediate_results: dict[int, PartitionSet] = {}
        with profiler("profile_RayRunner.run_{datetime.now().isoformat()}.json"):
            for exec_op in exec_plan.execution_ops:

                data_deps = exec_op.data_deps
                input_partition_set = {nid: partition_intermediate_results[nid] for nid in data_deps}

                if exec_op.is_global_op:
                    input_partition_set = {nid: partition_intermediate_results[nid] for nid in data_deps}
                    result_partition_set = self._global_op_runner.run_node_list(
                        input_partition_set, exec_op.logical_ops
                    )
                else:
                    result_partition_set = self._part_op_runner.run_node_list(
                        input_partition_set, exec_op.logical_ops, exec_op.num_partitions, exec_op.resource_request()
                    )

                for child_id in data_deps:
                    del partition_intermediate_results[child_id]

                partition_intermediate_results[exec_op.logical_ops[-1].id()] = result_partition_set

            last = exec_plan.execution_ops[-1].logical_ops[-1]
            final_result = partition_intermediate_results[last.id()]
            pset_entry = self._part_set_cache.put_partition_set(final_result)
            return pset_entry


@ray.remote
def build_partitions(instruction_stack: list[Instruction], *inputs: vPartition) -> vPartition | list[vPartition]:
    partitions = list(inputs)
    for instruction in instruction_stack:
        partitions = instruction.run(partitions)

    return partitions if len(partitions) > 1 else partitions[0]


@ray.remote
def get_meta(partition: vPartition) -> PartitionMetadata:
    return partition.metadata()


class DynamicRayRunner(RayRunner):
    def run(self, plan: logical_plan.LogicalPlan) -> PartitionCacheEntry:

        plan = self.optimize(plan)

        phys_plan: Iterator[
            None | MaterializationRequestBase[vPartition]
        ] = physical_plan_factory.get_materializing_physical_plan(plan)

        # Note: For autoscaling clusters, we will probably want to query this dynamically.
        # Keep in mind this call takes about 0.3ms.
        cores = int(ray.cluster_resources()["CPU"])
        constructions_to_dispatch = []
        inflight_constructions: dict[str, MaterializationRequestBase[ray.ObjectRef]] = dict()
        inflight_ref_to_construction: dict[ray.ObjectRef, str] = dict()

        start = datetime.now()
        result_pset = RayPartitionSet({})
        with profiler("profile_DynamicRayRunner.run_{datetime.now().isoformat()}.json"):

            try:

                while True:
                    # Dispatch tasks while cores are available.
                    cores_available = cores - len(inflight_constructions)
                    for i in range(cores_available):

                        next_step = next(phys_plan)

                        while next_step is not None and len(next_step.instructions) == 0:
                            assert isinstance(next_step, MaterializationRequest)
                            [partition] = next_step.inputs
                            next_step.result = RayMaterializationResult(partition)
                            next_step = next(phys_plan)

                        if next_step is None:
                            break

                        constructions_to_dispatch.append(next_step)

                    dispatch = datetime.now()
                    logger.debug(
                        f"{(dispatch - start).total_seconds()}s: DynamicRayRunner dispatched batch of {len(constructions_to_dispatch)} tasks."
                    )
                    for construction in constructions_to_dispatch:
                        results = self._build_partitions(construction)
                        logger.debug(f"{construction} -> {results}")
                        inflight_constructions[construction.id()] = construction
                        for result in results:
                            inflight_ref_to_construction[result] = construction.id()

                    constructions_to_dispatch.clear()

                    # All tasks dispatched. Await a result
                    # while inflight_ref_to_construction:
                    dispatch = datetime.now()
                    [ready], _ = ray.wait(list(inflight_ref_to_construction.keys()), fetch_local=False)
                    cons_id = inflight_ref_to_construction[ready]
                    logger.debug(f"+{(datetime.now() - dispatch).total_seconds()}s to await a result from {cons_id}")

                    # Flush the entire task associated with the result

                    cons = inflight_constructions[cons_id]
                    if isinstance(cons, MaterializationRequest):
                        del inflight_ref_to_construction[ready]
                    elif isinstance(cons, MaterializationRequestMulti):

                        assert cons.results is not None
                        for result in cons.results:
                            del inflight_ref_to_construction[result.partition()]

                    del inflight_constructions[cons_id]

            except StopIteration as e:
                for i, partition in enumerate(result_partitions := e.value):
                    result_pset.set_partition(i, partition)

            for construction in constructions_to_dispatch:
                results = self._build_partitions(construction)
                inflight_constructions[construction.id()] = construction
                for result in results:
                    inflight_ref_to_construction[result] = construction.id()

            constructions_to_dispatch.clear()

            while inflight_ref_to_construction:
                dispatch = datetime.now()
                [ready], _ = ray.wait(list(inflight_ref_to_construction.keys()), fetch_local=False)
                cons_id = inflight_ref_to_construction[ready]
                logger.debug(f"+{(datetime.now() - dispatch).total_seconds()}s to await a result of {cons_id}")

                # Flush the entire task associated with the result
                cons = inflight_constructions[cons_id]
                if isinstance(cons, MaterializationRequest):
                    del inflight_ref_to_construction[ready]
                elif isinstance(cons, MaterializationRequestMulti):

                    assert cons.results is not None
                    for result in cons.results:
                        del inflight_ref_to_construction[result.partition()]

                del inflight_constructions[cons_id]

        pset_entry = self._part_set_cache.put_partition_set(result_pset)
        return pset_entry

    def _build_partitions(self, partspec: MaterializationRequestBase[ray.ObjectRef]) -> list[ray.ObjectRef]:
        ray_options: dict[str, Any] = {
            "num_returns": partspec.num_results,
        }
        if len(partspec.inputs) > 1:
            ray_options["scheduling_strategy"] = "SPREAD"

        construct_remote = build_partitions.options(**ray_options)
        partitions = construct_remote.remote(partspec.instructions, *partspec.inputs)
        # Handle ray bug that ignores list interpretation when num_returns=1
        if partspec.num_results == 1:
            partitions = [partitions]

        if isinstance(partspec, MaterializationRequestMulti):
            partspec.results = [RayMaterializationResult(partition) for partition in partitions]
        elif isinstance(partspec, MaterializationRequest):
            [partition] = partitions
            partspec.result = RayMaterializationResult(partition)
        else:
            raise TypeError(f"Could not type match input {partspec}")

        return partitions


@dataclass(frozen=True)
class RayMaterializationResult(MaterializationResult[ray.ObjectRef]):
    _partition: ray.ObjectRef

    def partition(self) -> ray.ObjectRef:
        return self._partition

    def metadata(self) -> PartitionMetadata:
        return ray.get(get_meta.remote(self._partition))

    def cancel(self) -> None:
        return ray.cancel(self._partition)

    def _noop(self, _: ray.ObjectRef) -> None:
        return None
