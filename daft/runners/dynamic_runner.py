from __future__ import annotations

import multiprocessing
from dataclasses import dataclass
from typing import Iterator

import psutil

from daft.execution import physical_plan_factory
from daft.execution.execution_step import (
    MaterializationRequest,
    MaterializationRequestBase,
    MaterializationRequestMulti,
    MaterializationResult,
)
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
from daft.resource_request import ResourceRequest
from daft.runners.partitioning import (
    PartitionCacheEntry,
    PartitionMetadata,
    PartitionSetFactory,
    vPartition,
)
from daft.runners.profiler import profiler
from daft.runners.pyrunner import LocalPartitionSet, LocalPartitionSetFactory
from daft.runners.runner import Runner


class DynamicRunner(Runner):
    """A dynamic version of PyRunner that uses DynamicSchedule to determine execution steps."""

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
        phys_plan: Iterator[
            None | MaterializationRequestBase[vPartition]
        ] = physical_plan_factory.get_materializing_physical_plan(plan, psets)

        result_pset = LocalPartitionSet({})

        with profiler("profile_DynamicRunner.run_{datetime.now().isoformat()}.json"):
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

    def _build_partitions(self, partspec: MaterializationRequestBase[vPartition]) -> None:
        partitions = partspec.inputs
        for instruction in partspec.instructions:
            partitions = instruction.run(partitions)
        if isinstance(partspec, MaterializationRequestMulti):
            partspec.results = [PyMaterializationResult(partition) for partition in partitions]
        elif isinstance(partspec, MaterializationRequest):
            [partition] = partitions
            partspec.result = PyMaterializationResult(partition)
        else:
            raise TypeError(f"Cannot typematch input {partspec}")


@dataclass(frozen=True)
class PyMaterializationResult(MaterializationResult[vPartition]):
    _partition: vPartition

    def partition(self) -> vPartition:
        return self._partition

    def metadata(self) -> PartitionMetadata:
        return self._partition.metadata()

    def cancel(self) -> None:
        return None

    def _noop(self, _: vPartition) -> None:
        return None
