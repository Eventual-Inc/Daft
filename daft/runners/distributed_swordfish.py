import asyncio
from collections import deque
from typing import Optional

from daft.daft import (
    DistributedPhysicalPlan,
    LocalPhysicalPlan,
    NativeExecutor,
    PlanInputConsumer,
    PyDaftExecutionConfig,
    ScanTask,
)
from daft.logical.builder import LogicalPlanBuilder
from daft.recordbatch.micropartition import MicroPartition

try:
    import ray
    from ray._raylet import ObjectRefGenerator
except ImportError:
    raise


@ray.remote
class SwordfishActor:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.current_executor: Optional[NativeExecutor] = None
        self.current_consumer: Optional[PlanInputConsumer] = None

    async def run_local_plan(
        self,
        plan: LocalPhysicalPlan,
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: Optional[int] = None,
    ):
        assert self.current_executor is None
        assert self.current_consumer is None
        executor = NativeExecutor()

        output_producer, input_consumer = executor.run_distributed(plan, daft_execution_config, results_buffer_size)
        self.current_executor = executor
        self.current_consumer = input_consumer

        async for partition in output_producer:
            if partition is None:
                break
            yield MicroPartition._from_pymicropartition(partition)

    async def put_input(self, input_id: int, input: ScanTask):
        assert self.current_consumer is not None
        await self.current_consumer.put_input(input_id, input)
        return self.node_id

    async def mark_inputs_finished(self):
        assert self.current_consumer is not None
        self.current_consumer = None

    async def mark_plan_finished(self):
        assert self.current_output_producer is not None
        assert self.current_executor is not None
        self.current_output_producer = None
        self.current_executor = None


@ray.remote
class ActorInputAssigner:
    def __init__(self, inputs: deque[ScanTask]):
        self.actors: dict[str, SwordfishActor] = {}
        self.actor_lock = asyncio.Lock()
        self.inputs = inputs
        self.input_refs: dict[str, Optional[ray.ObjectRef]] = {}

    async def add_actor(self, actor: SwordfishActor, node_id: str):
        async with self.actor_lock:
            self.actors[node_id] = actor
            self.input_refs[node_id] = None

    async def get_actors(self):
        async with self.actor_lock:
            return self.actors

    async def start_assigning_inputs(self):
        while self.inputs and all([self.input_refs[node_id] is not None for node_id in self.input_refs]):
            # dispatch new inputs to actors
            actors = await self.get_actors()
            for node_id, input_ref in self.input_refs.items():
                if input_ref is None:
                    try:
                        next_input = self.inputs.popleft()
                        input_ref = actors[node_id].put_input.remote(0, next_input)
                        self.input_refs[node_id] = input_ref
                    except IndexError:
                        await asyncio.gather(*[actor.mark_inputs_finished.remote() for actor in actors.values()])
                        break

            # await actors to finish processing
            to_wait = self.input_refs.values()
            ready, _ = await asyncio.wait(to_wait, return_when=asyncio.FIRST_COMPLETED)
            for ready_ref in ready:
                node_id = ray.get(ready_ref)
                self.input_refs[node_id] = None


class DistributedSwordfishRunner:
    def __init__(self):
        self.actors = {}
        self.refresh_actors()

    def refresh_actors(self):
        new_actors = {}
        for node in ray.nodes():
            if "Resources" in node and "CPU" in node["Resources"] and node["Resources"]["CPU"] > 0:
                node_id = node["NodeID"]
                if node_id not in self.actors:
                    actor = SwordfishActor.options(  # type: ignore[attr-defined]
                        scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                            node_id=node_id,
                            soft=False,
                        ),
                    ).remote()
                    self.actors[node_id] = actor
                    new_actors[node_id] = actor
        return new_actors

    def run_plan(
        self,
        builder: LogicalPlanBuilder,
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: Optional[int] = None,
    ):
        # Make the distributed plan
        distributed_plan = DistributedPhysicalPlan.from_logical_plan_builder(
            builder._builder, daft_execution_config=daft_execution_config
        )
        local_plan = distributed_plan.get_local_physical_plan()
        inputs = deque(distributed_plan.get_inputs())

        # refresh actors
        self.refresh_actors()

        # Create an actor to assign inputs to actors
        actor_input_assigner = ActorInputAssigner.remote(inputs)  # type: ignore[attr-defined]
        ray.get([actor_input_assigner.add_actor.remote(actor, node_id) for node_id, actor in self.actors.items()])

        # launch plan on all actors
        result_gens = []
        for node_id, actor in self.actors.items():
            result_gen = actor.run_local_plan.remote(local_plan, daft_execution_config, results_buffer_size)
            result_gens.append(result_gen)

        # start assigning inputs to actors
        assign_inputs_done = actor_input_assigner.start_assigning_inputs.remote()  # type: ignore[attr-defined]

        # Scheduling loop
        input_exhausted = False
        while result_gens and not input_exhausted:
            # refresh actors to get new actors if new nodes are up, and submit plan to them
            new_actors = self.refresh_actors()
            for node_id, actor in new_actors.items():
                result_gen = actor.run_local_plan.remote(local_plan, daft_execution_config, results_buffer_size)
                result_gens.append(result_gen)
                ray.get(actor_input_assigner.add_actor.remote(actor, node_id))

            # TODO THINK ABOUT ORDERING OF GENERATORS
            ready, result_gens = ray.wait(result_gens, timeout=0.1, fetch_local=False)
            for gen in ready:
                assert isinstance(gen, ObjectRefGenerator)
                try:
                    partition = next(gen)
                    yield partition
                except StopIteration:
                    pass
                else:
                    result_gens.append(gen)

            # check if inputs are exhausted
            if not input_exhausted:
                ready, _ = ray.wait([assign_inputs_done], timeout=0.1, fetch_local=False)
                if len(ready) > 0:
                    input_exhausted = True

        # wait for all actors to finish
        ray.get([actor.mark_plan_finished.remote() for actor in self.actors.values()])
