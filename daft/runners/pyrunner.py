from __future__ import annotations

import multiprocessing
from dataclasses import dataclass

import psutil

from daft.execution import physical_plan_factory
from daft.execution.execution_step import (
    ExecutionStep,
    MaterializedResult,
    MultiOutputExecutionStep,
    SingleOutputExecutionStep,
)
from daft.execution.logical_op_runners import LogicalPartitionOpRunner
from daft.filesystem import glob_path, glob_path_with_stats
from daft.internal.gpu import cuda_device_count
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
from daft.logical.schema import Schema
from daft.resource_request import ResourceRequest
from daft.runners.partitioning import (
    PartID,
    PartitionCacheEntry,
    PartitionMetadata,
    PartitionSet,
    PartitionSetFactory,
    vPartition,
)
from daft.runners.profiler import profiler
from daft.runners.runner import Runner


@dataclass
class LocalPartitionSet(PartitionSet[vPartition]):
    _partitions: dict[PartID, vPartition]

    def items(self) -> list[tuple[PartID, vPartition]]:
        return sorted(self._partitions.items())

    def _get_merged_vpartition(self) -> vPartition:
        ids_and_partitions = self.items()
        assert ids_and_partitions[0][0] == 0
        assert ids_and_partitions[-1][0] + 1 == len(ids_and_partitions)
        return vPartition.merge_partitions([part for id, part in ids_and_partitions], verify_partition_id=False)

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

    def len_of_partitions(self) -> list[int]:
        partition_ids = sorted(list(self._partitions.keys()))
        return [len(self._partitions[pid]) for pid in partition_ids]

    def num_partitions(self) -> int:
        return len(self._partitions)

    def wait(self) -> None:
        pass


class LocalPartitionSetFactory(PartitionSetFactory[vPartition]):
    def glob_paths(
        self,
        source_path: str,
    ) -> tuple[LocalPartitionSet, Schema]:
        filepaths = glob_path(source_path)

        if len(filepaths) == 0:
            raise FileNotFoundError(f"No files found at {source_path}")

        schema = self._get_listing_paths_schema()

        pset = LocalPartitionSet(
            {
                i: vPartition.from_pydict(
                    data={self.FS_LISTING_PATH_COLUMN_NAME: [path]}, schema=schema, partition_id=i
                )
                for i, path in enumerate(filepaths)  # Hardcoded to 1 path per partition
            },
        )

        return pset, schema

    def glob_paths_details(
        self,
        source_path: str,
    ) -> tuple[LocalPartitionSet, Schema]:
        files_info = glob_path_with_stats(source_path)

        if len(files_info) == 0:
            raise FileNotFoundError(f"No files found at {source_path}")

        schema = self._get_listing_paths_details_schema()
        pset = LocalPartitionSet(
            {
                # Hardcoded to 1 partition
                0: vPartition.from_pydict(
                    data={
                        self.FS_LISTING_PATH_COLUMN_NAME: [f.path for f in files_info],
                        self.FS_LISTING_SIZE_COLUMN_NAME: [f.size for f in files_info],
                        self.FS_LISTING_TYPE_COLUMN_NAME: [f.type for f in files_info],
                    },
                    schema=schema,
                    partition_id=0,
                ),
            }
        )
        return pset, schema


class LocalLogicalPartitionOpRunner(LogicalPartitionOpRunner):
    ...


class PyRunner(Runner):
    def __init__(self) -> None:
        super().__init__()
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

    def optimize(self, plan: logical_plan.LogicalPlan) -> logical_plan.LogicalPlan:
        # From PyRunner
        return self._optimizer.optimize(plan)

    def partition_set_factory(self) -> PartitionSetFactory:
        return LocalPartitionSetFactory()

    def run(self, plan: logical_plan.LogicalPlan) -> PartitionCacheEntry:
        plan = self.optimize(plan)

        psets = {
            key: entry.value.values()
            for key, entry in self._part_set_cache._uuid_to_partition_set.items()
            if entry.value is not None
        }
        phys_plan = physical_plan_factory.get_materializing_physical_plan(plan, psets)

        result_pset = LocalPartitionSet({})

        with profiler("profile_PyRunner.run_{datetime.now().isoformat()}.json"):
            try:
                while True:
                    next_step = next(phys_plan)
                    assert next_step is not None, "Got a None ExecutionStep in singlethreaded mode"
                    self._check_resource_requests(next_step.resource_request)
                    self._build_partitions(next_step)
            except StopIteration as e:
                for i, partition in enumerate(e.value):
                    result_pset.set_partition(i, partition)

        pset_entry = self.put_partition_set_into_cache(result_pset)
        return pset_entry

    def _check_resource_requests(self, resource_request: ResourceRequest | None) -> None:
        """Validates that the requested ResourceRequest is possible to run locally"""
        if resource_request is None:
            return
        if resource_request.num_cpus is not None and resource_request.num_cpus > multiprocessing.cpu_count():
            raise RuntimeError(
                f"Requested {resource_request.num_cpus} CPUs but found only {multiprocessing.cpu_count()} available"
            )
        if resource_request.num_gpus is not None and resource_request.num_gpus > cuda_device_count():
            raise RuntimeError(
                f"Requested {resource_request.num_gpus} GPUs but found only {cuda_device_count()} available"
            )
        if resource_request.memory_bytes is not None and resource_request.memory_bytes > psutil.virtual_memory().total:
            raise RuntimeError(
                f"Requested {resource_request.memory_bytes} bytes of memory but found only {psutil.virtual_memory().total} available"
            )

    def _build_partitions(self, partspec: ExecutionStep[vPartition]) -> None:
        partitions = partspec.inputs
        for instruction in partspec.instructions:
            partitions = instruction.run(partitions)
        if isinstance(partspec, MultiOutputExecutionStep):
            partspec.results = [PyMaterializedResult(partition) for partition in partitions]
        elif isinstance(partspec, SingleOutputExecutionStep):
            [partition] = partitions
            partspec.result = PyMaterializedResult(partition)
        else:
            raise TypeError(f"Cannot typematch input {partspec}")


@dataclass(frozen=True)
class PyMaterializedResult(MaterializedResult[vPartition]):
    _partition: vPartition

    def partition(self) -> vPartition:
        return self._partition

    def vpartition(self) -> vPartition:
        return self._partition

    def metadata(self) -> PartitionMetadata:
        return self._partition.metadata()

    def cancel(self) -> None:
        return None

    def _noop(self, _: vPartition) -> None:
        return None
