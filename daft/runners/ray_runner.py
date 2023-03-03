from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from loguru import logger

try:
    import ray
except ImportError:
    logger.error(
        f"Error when importing Ray. Please ensure that getdaft was installed with the Ray extras tag: getdaft[ray] (https://www.getdaft.io/projects/docs/en/latest/learn/install.html)"
    )
    raise

from daft.datasources import SourceInfo
from daft.execution import physical_plan_factory
from daft.execution.execution_step import (
    FanoutInstruction,
    Instruction,
    MaterializedResult,
    MultiOutputPartitionTask,
    PartitionTask,
    ReduceInstruction,
    SingleOutputPartitionTask,
)
from daft.filesystem import glob_path_with_stats
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

if TYPE_CHECKING:
    from ray.data.block import Block as RayDatasetBlock
    from ray.data.dataset import Dataset as RayDataset

_RAY_FROM_ARROW_REFS_AVAILABLE = True
try:
    from ray.data import from_arrow_refs
except ImportError:
    _RAY_FROM_ARROW_REFS_AVAILABLE = False

from daft.logical.schema import Schema


@ray.remote
def _glob_path_into_details_vpartitions(
    path: str, schema: Schema, source_info: SourceInfo | None
) -> list[tuple[PartID, vPartition]]:
    listing_infos = glob_path_with_stats(path, source_info)
    if len(listing_infos) == 0:
        raise FileNotFoundError(f"No files found at {path}")

    # Hardcoded to 1 partition
    partition = vPartition.from_pydict(
        {
            "path": [file_info.path for file_info in listing_infos],
            "size": [file_info.size for file_info in listing_infos],
            "type": [file_info.type for file_info in listing_infos],
            "rows": [file_info.rows for file_info in listing_infos],
        },
    )
    assert partition.schema() == schema, f"Constructed partition must have schema: {schema}"

    partition_ref = ray.put(partition)
    partition_refs = [(0, partition_ref)]

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


@ray.remote
def remote_len_partition(p: vPartition) -> int:
    return len(p)


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
        return vPartition.concat(all_partitions)

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

        result: list[int] = ray.get([remote_len_partition.remote(self._partitions[pid]) for pid in partition_ids])
        return result

    def num_partitions(self) -> int:
        return len(self._partitions)

    def wait(self) -> None:
        ray.wait([o for o in self._partitions.values()])


class RayPartitionSetFactory(PartitionSetFactory[ray.ObjectRef]):
    def glob_paths_details(
        self,
        source_path: str,
        source_info: SourceInfo | None = None,
    ) -> tuple[RayPartitionSet, Schema]:
        schema = self._get_listing_paths_details_schema()
        partition_refs = ray.get(_glob_path_into_details_vpartitions.remote(source_path, schema, source_info))
        return RayPartitionSet({part_id: part for part_id, part in partition_refs}), schema


def _get_ray_task_options(resource_request: ResourceRequest) -> dict[str, Any]:
    options = {}
    # FYI: Ray's default resource behaviour is documented here:
    # https://docs.ray.io/en/latest/ray-core/tasks/resources.html
    if resource_request.num_cpus is not None:
        # Ray worker pool will thrash if a request comes in for fractional cpus,
        # so we floor the request to at least 1 cpu here.
        options["num_cpus"] = max(1, resource_request.num_cpus)
    if resource_request.num_gpus is not None:
        options["num_gpus"] = resource_request.num_gpus
    if resource_request.memory_bytes is not None:
        options["memory"] = resource_request.memory_bytes
    return options


def build_partitions(
    instruction_stack: list[Instruction], *inputs: vPartition
) -> list[list[PartitionMetadata] | vPartition]:
    partitions = list(inputs)
    for instruction in instruction_stack:
        partitions = instruction.run(partitions)

    metadatas = [PartitionMetadata.from_table(p) for p in partitions]

    return [metadatas, *partitions]


# Give the same function different names to aid in profiling data distribution.


@ray.remote
def single_partition_pipeline(
    instruction_stack: list[Instruction], *inputs: vPartition
) -> list[list[PartitionMetadata] | vPartition]:
    return build_partitions(instruction_stack, *inputs)


@ray.remote
def fanout_pipeline(
    instruction_stack: list[Instruction], *inputs: vPartition
) -> list[list[PartitionMetadata] | vPartition]:
    return build_partitions(instruction_stack, *inputs)


@ray.remote(scheduling_strategy="SPREAD")
def reduce_pipeline(
    instruction_stack: list[Instruction], *inputs: vPartition
) -> list[list[PartitionMetadata] | vPartition]:
    return build_partitions(instruction_stack, *inputs)


@ray.remote(scheduling_strategy="SPREAD")
def reduce_and_fanout(
    instruction_stack: list[Instruction], *inputs: vPartition
) -> list[list[PartitionMetadata] | vPartition]:
    return build_partitions(instruction_stack, *inputs)


@ray.remote
def get_meta(partition: vPartition) -> PartitionMetadata:
    return PartitionMetadata.from_table(partition)


class Scheduler:
    def __init__(
        self,
        max_tasks_per_core: float | None,
        max_refs_per_core: float | None,
        batch_dispatch_coeff: float | None,
    ) -> None:
        """
        max_tasks_per_core:
            Maximum allowed inflight tasks per core.
        max_refs_per_core:
            Maximum allowed inflight objectrefs per core.
        batch_dispatch_coeff:
            When dispatching or awaiting tasks, do it in batches of size coeff * number of cores.
            If 0, batching is disabled (i.e. batch size is set to 1).
        """
        # The default values set below were determined in empirical benchmarking to deliver the best performance
        # (runtime, data locality, memory pressure) across different configurations.
        # They differ from the original intended values of the scheduler; the original values are discussed in comments.

        # Theoretically, this should be around 2;
        # higher values increase the likelihood of dispatching redundant tasks,
        # and lower values reduce the cluster's ability to pipeline tasks and introduces worker idling during scheduling.
        self.max_tasks_per_core = max_tasks_per_core if max_tasks_per_core is not None else 4.0

        # This default is a vacuous limit for now.
        # The option exists because Ray clusters anecdotally sometimes choke when there are too many object refs in flight.
        self.max_refs_per_core = max_refs_per_core if max_refs_per_core is not None else 10000.0

        # Theoretically, this should be 1.0: we should batch enough tasks for all the cores in a single dispatch;
        # otherwise, we begin dispatching downstream dependencies (that are not immediately executable)
        # before saturating all the cores (and their pipelines).
        self.batch_dispatch_coeff = batch_dispatch_coeff if batch_dispatch_coeff is not None else 1.0

        self.reserved_cores = 0

    def run_plan(
        self,
        plan: logical_plan.LogicalPlan,
        psets: dict[str, ray.ObjectRef],
    ) -> list[ray.ObjectRef]:

        from loguru import logger

        phys_plan = physical_plan_factory.get_materializing_physical_plan(plan, psets)

        # Note: For autoscaling clusters, we will probably want to query cores dynamically.
        # Keep in mind this call takes about 0.3ms.
        cores = int(ray.cluster_resources()["CPU"]) - self.reserved_cores
        batch_dispatch_size = int(cores * self.batch_dispatch_coeff) or 1

        inflight_tasks: dict[str, PartitionTask[ray.ObjectRef]] = dict()
        inflight_ref_to_task: dict[ray.ObjectRef, str] = dict()

        result_partitions = None
        start = datetime.now()
        profile_filename = (
            f"profile_RayRunner.run()_"
            f"{datetime.replace(datetime.now(), second=0, microsecond=0).isoformat()[:-3]}.json"
        )
        with profiler(profile_filename):
            while True:

                while (
                    len(inflight_tasks) < self.max_tasks_per_core * cores
                    and len(inflight_ref_to_task) < self.max_refs_per_core * cores
                ):

                    # Get the next batch of tasks to dispatch.
                    tasks_to_dispatch = []
                    try:
                        for _ in range(batch_dispatch_size):

                            next_step = next(phys_plan)

                            # If this task is a no-op, just run it locally immediately.
                            while next_step is not None and len(next_step.instructions) == 0:
                                assert isinstance(next_step, SingleOutputPartitionTask)
                                next_step.set_result(
                                    [RayMaterializedResult(partition) for partition in next_step.inputs]
                                )
                                next_step = next(phys_plan)

                            if next_step is None:
                                break

                            tasks_to_dispatch.append(next_step)

                    except StopIteration as e:
                        result_partitions = e.value

                    # Dispatch the batch of tasks.
                    logger.debug(
                        f"{(datetime.now() - start).total_seconds()}s: RayRunner dispatching {len(tasks_to_dispatch)} tasks:"
                    )
                    for task in tasks_to_dispatch:
                        results = _build_partitions(task)
                        logger.debug(f"{task} -> {results}")
                        inflight_tasks[task.id()] = task
                        for result in results:
                            inflight_ref_to_task[result] = task.id()

                    # Exit if the plan is complete and the result references are available.
                    if result_partitions is not None:
                        return result_partitions

                # Await a batch of tasks.
                for i in range(min(batch_dispatch_size, len(inflight_tasks))):
                    dispatch = datetime.now()
                    [ready], _ = ray.wait(list(inflight_ref_to_task.keys()), fetch_local=False)
                    task_id = inflight_ref_to_task[ready]
                    logger.debug(f"+{(datetime.now() - dispatch).total_seconds()}s to await a result from {task_id}")

                    # Mark the entire task associated with the result as done.
                    task = inflight_tasks[task_id]
                    if isinstance(task, SingleOutputPartitionTask):
                        del inflight_ref_to_task[ready]
                    elif isinstance(task, MultiOutputPartitionTask):
                        for partition in task.partitions():
                            del inflight_ref_to_task[partition]

                    del inflight_tasks[task_id]


@ray.remote(num_cpus=1)
class SchedulerActor(Scheduler):
    def __init__(self, *n, **kw) -> None:
        super().__init__(*n, **kw)
        self.reserved_cores = 1


def _build_partitions(task: PartitionTask[ray.ObjectRef]) -> list[ray.ObjectRef]:
    """Run a PartitionTask and return the resulting list of partitions."""
    ray_options: dict[str, Any] = {
        "num_returns": task.num_results + 1,
    }

    if task.resource_request is not None:
        ray_options = {**ray_options, **_get_ray_task_options(task.resource_request)}

    if isinstance(task.instructions[0], ReduceInstruction):
        build_remote = reduce_and_fanout if isinstance(task.instructions[-1], FanoutInstruction) else reduce_pipeline
    else:
        build_remote = (
            fanout_pipeline if isinstance(task.instructions[-1], FanoutInstruction) else single_partition_pipeline
        )

    build_remote = build_remote.options(**ray_options)
    [metadatas_ref, *partitions] = build_remote.remote(task.instructions, *task.inputs)

    task.set_result([RayMaterializedResult(partition, metadatas_ref, i) for i, partition in enumerate(partitions)])

    return partitions


class RayRunner(Runner):
    def __init__(
        self,
        address: str | None,
        max_tasks_per_core: float | None,
        max_refs_per_core: float | None,
        batch_dispatch_coeff: float | None,
    ) -> None:
        super().__init__()
        if ray.is_initialized():
            logger.warning(f"Ray has already been initialized, Daft will reuse the existing Ray context.")
        self.ray_context = ray.init(address=address, ignore_reinit_error=True)
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

        if isinstance(self.ray_context, ray.client_builder.ClientContext):
            # Run scheduler remotely if the cluster is connected remotely.
            self.scheduler_actor = SchedulerActor.remote(  # type: ignore
                max_tasks_per_core=max_tasks_per_core,
                max_refs_per_core=max_refs_per_core,
                batch_dispatch_coeff=batch_dispatch_coeff,
            )
        else:
            self.scheduler = Scheduler(
                max_tasks_per_core=max_tasks_per_core,
                max_refs_per_core=max_refs_per_core,
                batch_dispatch_coeff=batch_dispatch_coeff,
            )

    def run(self, plan: logical_plan.LogicalPlan) -> PartitionCacheEntry:
        result_pset = RayPartitionSet({})

        plan = self.optimize(plan)

        psets = {
            key: entry.value.values()
            for key, entry in self._part_set_cache._uuid_to_partition_set.items()
            if entry.value is not None
        }
        if isinstance(self.ray_context, ray.client_builder.ClientContext):
            partitions = ray.get(
                self.scheduler_actor.run_plan.remote(
                    plan=plan,
                    psets=psets,
                )
            )
        else:
            partitions = self.scheduler.run_plan(
                plan=plan,
                psets=psets,
            )

        for i, partition in enumerate(partitions):
            result_pset.set_partition(i, partition)

        pset_entry = self._part_set_cache.put_partition_set(result_pset)

        return pset_entry

    def put_partition_set_into_cache(self, pset: PartitionSet) -> PartitionCacheEntry:
        if isinstance(pset, LocalPartitionSet):
            pset = RayPartitionSet({pid: ray.put(val) for pid, val in pset._partitions.items()})

        return self._part_set_cache.put_partition_set(pset=pset)

    def optimize(self, plan: logical_plan.LogicalPlan) -> logical_plan.LogicalPlan:
        return self._optimizer.optimize(plan)

    def partition_set_factory(self) -> PartitionSetFactory:
        return RayPartitionSetFactory()


@dataclass(frozen=True)
class RayMaterializedResult(MaterializedResult[ray.ObjectRef]):
    _partition: ray.ObjectRef
    _metadatas: ray.ObjectRef | None = None
    _metadata_index: int | None = None

    def partition(self) -> ray.ObjectRef:
        return self._partition

    def vpartition(self) -> vPartition:
        return ray.get(self._partition)

    def metadata(self) -> PartitionMetadata:
        if self._metadatas is not None and self._metadata_index is not None:
            return ray.get(self._metadatas)[self._metadata_index]
        else:
            return ray.get(get_meta.remote(self._partition))

    def cancel(self) -> None:
        return ray.cancel(self._partition)

    def _noop(self, _: ray.ObjectRef) -> None:
        return None
