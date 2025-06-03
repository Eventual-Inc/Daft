from __future__ import annotations

import ray

import daft
from daft import DataType, ResourceRequest
from daft.daft import PyDaftExecutionConfig
from daft.expressions import ExpressionsProjection
from daft.recordbatch import MicroPartition
from daft.runners.partitioning import PartialPartitionMetadata
from daft.runners.ray_runner import RayRoundRobinActorPool


@daft.udf(return_dtype=DataType.int64())
class MyUDF:
    def __init__(self):
        self.state = 0

    def __call__(self, x):
        self.state += 1
        return [i + self.state for i in x.to_pylist()]


def test_ray_actor_pool():
    projection = ExpressionsProjection([MyUDF(daft.col("x"))])
    pool = RayRoundRobinActorPool(
        "my-pool", 1, ResourceRequest(num_cpus=1), projection, execution_config=PyDaftExecutionConfig.from_env()
    )
    initial_partition = ray.put(MicroPartition.from_pydict({"x": [1, 1, 1]}))
    ppm = PartialPartitionMetadata(num_rows=None, size_bytes=None)
    pool.setup()

    result = pool.submit(partial_metadatas=[ppm], inputs=[initial_partition])
    [partial_metadata, result_data] = ray.get(result)
    assert len(partial_metadata) == 1
    pm = partial_metadata[0]
    assert isinstance(pm, PartialPartitionMetadata)
    assert pm.num_rows == 3
    assert result_data.to_pydict() == {"x": [2, 2, 2]}

    result = pool.submit(partial_metadatas=[ppm], inputs=[initial_partition])
    [partial_metadata, result_data] = ray.get(result)
    assert len(partial_metadata) == 1
    pm = partial_metadata[0]
    assert isinstance(pm, PartialPartitionMetadata)
    assert pm.num_rows == 3
    assert result_data.to_pydict() == {"x": [3, 3, 3]}

    result = pool.submit(partial_metadatas=[ppm], inputs=[initial_partition])
    [partial_metadata, result_data] = ray.get(result)
    assert len(partial_metadata) == 1
    pm = partial_metadata[0]
    assert isinstance(pm, PartialPartitionMetadata)
    assert pm.num_rows == 3
    assert result_data.to_pydict() == {"x": [4, 4, 4]}
