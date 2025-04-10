from collections import deque
import ray

from daft.daft import (
    LocalPhysicalPlan,
    NativeExecutor,
    PyDaftExecutionConfig,
    ScanTask,
    DistributedPhysicalPlan,
)
from daft.logical.builder import LogicalPlanBuilder
from daft.recordbatch.micropartition import MicroPartition


@ray.remote
class SwordfishActor:
    def __init__(self):
        self.current_executor = None
        self.current_consumer = None
        self.current_output_producer = None
        print("SwordfishActor initialized")

    def run_local_plan(
        self,
        plan: LocalPhysicalPlan,
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: int | None = None,
    ):
        print("run_local_plan")
        assert self.current_executor is None
        assert self.current_consumer is None
        executor = NativeExecutor()
        print("starting executor")
        output_producer, input_consumer = executor.run_distributed(
            plan, daft_execution_config, results_buffer_size
        )
        print("executor started")
        self.current_executor = executor
        self.current_consumer = input_consumer
        self.current_output_producer = output_producer

    async def get_outputs(self):
        async for partition in self.current_output_producer:
            if partition is None:
                break
            yield MicroPartition._from_pymicropartition(partition)

    async def put_input(self, input_id: int, input: ScanTask):
        assert self.current_consumer is not None
        print("putting input")
        await self.current_consumer.put_input(input_id, input)
        print("input put")

    async def mark_inputs_finished(self):
        assert self.current_consumer is not None
        self.current_consumer = None
        print("inputs finished")
        
    async def mark_plan_finished(self):
        assert self.current_output_producer is not None
        assert self.current_executor is not None
        self.current_output_producer = None
        self.current_executor = None
        print("plan finished")

class DistributedSwordfishRunner:
    def __init__(self):
        print("DistributedSwordfishRunner initialized")
        pass

    def run_plan(self, builder: LogicalPlanBuilder, daft_execution_config: PyDaftExecutionConfig, results_buffer_size: int | None = None):
        distributed_plan = DistributedPhysicalPlan.from_logical_plan_builder(builder._builder)
        print("distributed plan created")
        local_plan = distributed_plan.get_local_physical_plan()
        print("local plan created")
        inputs = deque(distributed_plan.get_inputs())
        print("inputs created")
        actors = [SwordfishActor.remote() for _ in range(len(inputs))]
        print("actor created")
        
        for actor in actors:
            ray.get(actor.run_local_plan.remote(local_plan, daft_execution_config, results_buffer_size))
        print("run local plan done")

        result_gens = [actor.get_outputs.remote() for actor in actors]
        print("result gen created")

        input_refs = [actor.put_input.remote(i, inputs.popleft()) for i, actor in enumerate(actors)]
        
        while inputs:
            print("putting input")
            ray.get(actor.put_input.remote(0, inputs.popleft()))
            print("input put")

        ray.get(actor.mark_inputs_finished.remote())

        for partition in result_gen:
            print("yielding partition in runner")
            yield partition
            print("yielded partition in runner")

        ray.get(actor.mark_plan_finished.remote())
        
        print("result gen done")
        

        