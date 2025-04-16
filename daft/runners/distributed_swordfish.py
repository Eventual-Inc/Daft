import logging
from typing import AsyncGenerator, Iterator, List, Optional

from daft.daft import (
    DistributedPhysicalPlan,
    LocalPhysicalPlan,
    NativeExecutor,
    PyDaftExecutionConfig,
)
from daft.recordbatch.micropartition import MicroPartition

try:
    import ray
    import ray.util.scheduling_strategies
except ImportError:
    raise

logger = logging.getLogger(__name__)


@ray.remote
class SwordfishActor:
    def __init__(self):
        self.native_executor = NativeExecutor()
        self.actor_handle = ray.get_runtime_context().current_actor

    async def _run_plan(
        self,
        local_physical_plan: LocalPhysicalPlan,
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: Optional[int] = None,
    ) -> AsyncGenerator[MicroPartition, None]:

        async for partition in self.native_executor.run_distributed(
            local_physical_plan, daft_execution_config, results_buffer_size
        ):
            if partition is None:
                break
            yield MicroPartition._from_pymicropartition(partition)

    async def run_plan_and_collect(
        self,
        local_physical_plan: LocalPhysicalPlan,
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: Optional[int] = None,
    ) -> List[ray.ObjectRef]:
        result_gen = self.actor_handle._run_plan.remote(
            local_physical_plan, daft_execution_config, results_buffer_size
        )
        res = []
        async for partition in result_gen:
            res.append(partition)
        return res


class SwordfishActorManager:
    def __init__(self):
        actors = []
        for node in ray.nodes():
            if (
                "Resources" in node
                and "CPU" in node["Resources"]
                and node["Resources"]["CPU"] > 0
            ):
                actor = SwordfishActor.options(
                    num_cpus=node["Resources"]["CPU"],
                    scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                        node_id=node["NodeID"], soft=True
                    ),
                ).remote()
                actors.append(actor)
        self.actor_pool = ray.util.ActorPool(actors)

    def run_plan(
        self,
        distributed_physical_plan: DistributedPhysicalPlan,
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: Optional[int] = None,
    ) -> Iterator[MicroPartition]:
        local_physical_plans = distributed_physical_plan.to_local_physical_plans()
        result_iter = self.actor_pool.map_unordered(
            lambda actor, local_physical_plan: actor.run_plan_and_collect.remote(
                local_physical_plan, daft_execution_config, results_buffer_size
            ),
            local_physical_plans,
        )
        for result_list in result_iter:
            for result in result_list:
                yield result


def run_distributed_swordfish(
    distributed_physical_plan: DistributedPhysicalPlan,
    daft_execution_config: PyDaftExecutionConfig,
    results_buffer_size: Optional[int] = None,
) -> Iterator[MicroPartition]:
    yield from SwordfishActorManager().run_plan(
        distributed_physical_plan, daft_execution_config, results_buffer_size
    )
