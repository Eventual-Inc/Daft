import logging
from collections import deque
from typing import AsyncGenerator, AsyncIterator, Generator, Optional

from daft.daft import (
    DistributedPhysicalPlan,
    LocalPhysicalPlan,
    NativeExecutor,
    PlanInputConsumer,
    PyDaftExecutionConfig,
    PyMicroPartition,
    ScanTask,
)
from daft.logical.builder import LogicalPlanBuilder
from daft.recordbatch.micropartition import MicroPartition

try:
    import ray
except ImportError:
    raise

logger = logging.getLogger(__name__)


@ray.remote
class SwordfishActor:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.current_executor: Optional[NativeExecutor] = None
        self.current_consumer: Optional[PlanInputConsumer] = None
        self.current_result_gen: Optional[AsyncIterator[PyMicroPartition]] = None

    async def run_local_plan(
        self,
        plan: LocalPhysicalPlan,
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: Optional[int] = None,
    ) -> None:
        assert self.current_executor is None
        assert self.current_consumer is None
        executor = NativeExecutor()

        output_producer, input_consumer = executor.run_distributed(plan, daft_execution_config, results_buffer_size)
        self.current_executor = executor
        self.current_consumer = input_consumer

        self.current_result_gen = output_producer

    async def get_results(self) -> AsyncGenerator[MicroPartition]:
        assert self.current_result_gen is not None
        async for partition in self.current_result_gen:
            if partition is None:
                break
            yield MicroPartition._from_pymicropartition(partition)

    async def put_input(self, input_id: int, input: ScanTask) -> str:
        assert self.current_consumer is not None
        await self.current_consumer.put_input(0, input)
        return self.node_id

    async def mark_inputs_finished(self) -> None:
        assert self.current_consumer is not None
        self.current_consumer = None

    async def mark_plan_finished(self) -> None:
        assert self.current_executor is not None
        self.current_executor = None


class DistributedSwordfishRunner:
    def __init__(self):
        self.actors = {}
        self.refresh_actors()

    def refresh_actors(self) -> dict[str, SwordfishActor]:
        new_actors = {}
        for node in ray.nodes():
            if "Resources" in node and "CPU" in node["Resources"] and node["Resources"]["CPU"] > 0:
                node_id = node["NodeID"]
                if node_id not in self.actors:
                    actor = SwordfishActor.options(  # type: ignore
                        scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                            node_id=node_id,
                            soft=False,
                        ),
                    ).remote(node_id)
                    self.actors[node_id] = actor
                    new_actors[node_id] = actor
        return new_actors

    def run_plan(
        self,
        builder: LogicalPlanBuilder,
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: Optional[int] = None,
    ) -> Generator[MicroPartition, None, None]:
        distributed_plan = DistributedPhysicalPlan.from_logical_plan_builder(builder._builder)
        local_plan = distributed_plan.get_local_physical_plan()

        inputs = deque([(i, input) for i, input in enumerate(distributed_plan.get_inputs())])
        original_num_inputs = len(inputs)

        logger.info(
            "Running distributed plan: %s, num inputs: %d",
            distributed_plan.repr(),
            len(inputs),
        )

        self.refresh_actors()

        logger.info("Starting plan on %d actors", len(self.actors))
        # Start plan on actors
        ray.get(
            [
                actor.run_local_plan.remote(local_plan, daft_execution_config, results_buffer_size)
                for actor in self.actors.values()
            ]
        )

        # Get result generators from actors
        result_gens = [actor.get_results.remote() for actor in self.actors.values()]

        # store the put_input refs for each node
        put_input_refs_per_node = {node_id: None for node_id in self.actors}

        # dispatch all inputs to actors
        while inputs:
            # wait on any put_input refs to complete
            ready, _ = ray.wait(
                [ref for ref in put_input_refs_per_node.values() if ref is not None],
                timeout=0.1,
            )

            if len(ready) > 0:
                # clear completed put_input refs
                for ref in ready:
                    node_id = ray.get(ref)
                    put_input_refs_per_node[node_id] = None

            # refresh actors
            new_actors = self.refresh_actors()
            if len(new_actors) > 0:
                for node_id in new_actors:
                    # start new plan on new actors
                    ray.get(
                        new_actors[node_id].run_local_plan.remote(  # type: ignore
                            local_plan, daft_execution_config, results_buffer_size
                        )
                    )
                    # get the result generator for the new actor
                    result_gens[node_id] = new_actors[node_id].get_results.remote()  # type: ignore
                    # put it in the put_input_refs_per_node dict
                    put_input_refs_per_node[node_id] = None

            # dispatch inputs to available actors
            num_inputs_dispatched = 0
            for node_id, input_ref in put_input_refs_per_node.items():
                if input_ref is None:
                    try:
                        next_input_id, next_input = inputs.popleft()
                        put_input_refs_per_node[node_id] = self.actors[node_id].put_input.remote(
                            next_input_id, next_input
                        )
                    except IndexError:
                        # no more inputs to dispatch
                        break
                    num_inputs_dispatched += 1
            if num_inputs_dispatched > 0:
                inputs_dispatched = original_num_inputs - len(inputs)
                logger.info(
                    "Dispatched %d inputs to swordfish actors, %d inputs remaining",
                    inputs_dispatched,
                    len(inputs),
                )

        logger.info("Finished dispatching inputs to swordfish actors")
        # finish all the put input refs
        ray.get([ref for ref in put_input_refs_per_node.values() if ref is not None])
        # tell the actors that no more inputs will be provided
        ray.get([actor.mark_inputs_finished.remote() for actor in self.actors.values()])

        # now get outputs from actors
        num_outputs_received = 0
        for result_gen in result_gens:
            for partition in result_gen:
                num_outputs_received += 1
                logger.info("Received output %d from swordfish actors", num_outputs_received)
                # this does not guarantee order. we need to guarantee order if there is a sort for example.
                yield partition

        logger.info("Finished receiving %d outputs from swordfish actors", num_outputs_received)

        # tell the actors that the plan is finished
        ray.get([actor.mark_plan_finished.remote() for actor in self.actors.values()])
