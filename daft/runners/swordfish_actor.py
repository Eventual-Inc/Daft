import ray

@ray.remote
class SwordfishActor:
    def __init__(self):
        pass

    async def run_plan(self, builder: LogicalPlanBuilder) -> PartitionCacheEntry:
        pass

    async def put_input(self, input_id: str, input: MicroPartition):
        pass
