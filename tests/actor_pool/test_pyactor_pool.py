from concurrent.futures import wait

import daft
from daft import DataType, ResourceRequest
from daft.execution.execution_step import StatefulUDFProject
from daft.expressions import ExpressionsProjection
from daft.runners.partitioning import PartialPartitionMetadata
from daft.runners.pyrunner import PyActorPool
from daft.table import MicroPartition


@daft.udf(return_dtype=DataType.int64())
class MyStatefulUDF:
    def __init__(self):
        pass

    def __call__(self, x):
        return x


def test_pyactor_pool():
    projection = ExpressionsProjection([MyStatefulUDF(daft.col("x"))])
    pool = PyActorPool("my-pool", 2, ResourceRequest(num_cpus=1), projection)

    with pool as pool_id:
        assert pool_id == "my-pool"

        result = pool.submit(
            instruction_stack=[StatefulUDFProject(projection=projection)],
            partitions=[MicroPartition.from_pydict({"x": [1, 2, 3]})],
            final_metadata=[PartialPartitionMetadata(num_rows=None, size_bytes=None)],
        )
        done, _ = wait([result], timeout=None)
        result_data = list(done)[0].result()[0]

        assert result_data.partition().to_pydict() == {"x": [1, 2, 3]}
