import logging
from typing import Generator, Optional

from daft.daft import (
    DistributedPhysicalPlan,
    LocalPhysicalPlan,
    NativeExecutor,
    PyDaftExecutionConfig,
    PyMicroPartition,
)
from daft.recordbatch.micropartition import MicroPartition

try:
    import ray
except ImportError:
    raise

logger = logging.getLogger(__name__)


@ray.remote
def run_swordfish_plan(
    local_physical_plan: LocalPhysicalPlan,
    psets: dict[str, list[PyMicroPartition]],
    daft_execution_config: PyDaftExecutionConfig,
    results_buffer_size: Optional[int] = None,
) -> Generator[MicroPartition, None, None]:
    native_executor = NativeExecutor()
    partition_iter = native_executor.run(
        local_physical_plan, psets, daft_execution_config, results_buffer_size
    )
    for partition in partition_iter:
        yield MicroPartition._from_pymicropartition(partition)


class DistributedSwordfishRunner:
    def __init__(self):
        pass

    def run_plan(
        self,
        distributed_plan: DistributedPhysicalPlan,
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: Optional[int] = None,
    ) -> Generator[MicroPartition, None, None]:
        result_gens = []
        while True:
            local_physical_plan = distributed_plan.next_plan()
            logger.info(f"Local physical plan: {local_physical_plan}")
            if local_physical_plan is None:
                break
            result_gen = run_swordfish_plan.options(
                scheduling_strategy="SPREAD",
                num_cpus=local_physical_plan.cpu_cost(),
                memory=local_physical_plan.memory_cost(),
            ).remote(
                local_physical_plan, {}, daft_execution_config, results_buffer_size
            )
            result_gens.append(result_gen)

        for result_gen in result_gens:
            for partition in result_gen:
                yield partition
