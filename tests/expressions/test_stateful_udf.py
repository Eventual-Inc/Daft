from __future__ import annotations

import os

import pytest

import daft
from daft import ResourceRequest
from daft.context import get_actor_context, get_context, set_planning_config
from daft.datatype import DataType
from daft.internal.gpu import cuda_visible_devices
from daft.udf import udf

pytestmark = pytest.mark.skipif(
    get_context().daft_execution_config.enable_native_executor is True,
    reason="Native executor fails for these tests",
)


@pytest.fixture(scope="module", autouse=True)
def enable_actor_pool():
    try:
        original_config = get_context().daft_planning_config

        set_planning_config(
            config=get_context().daft_planning_config.with_config_values(enable_actor_pool_projections=True)
        )
        yield
    finally:
        set_planning_config(config=original_config)


@pytest.mark.parametrize("concurrency", [1, 2, 4])
def test_stateful_udf_context_rank(concurrency):
    @udf(return_dtype=DataType.int64())
    class GetRank:
        def __init__(self):
            actor_context = get_actor_context()
            self._rank = actor_context.rank

        def __call__(self, data):
            actor_context = get_actor_context()

            assert actor_context.rank == self._rank

            import time

            time.sleep(0.1)

            return [self._rank] * len(data)

    GetRank = GetRank.with_concurrency(concurrency)

    df = daft.from_pydict({"x": [1, 2, 3, 4]})
    df = df.into_partitions(4)
    df = df.select(GetRank(df["x"]))

    result = df.to_pydict()
    ranks = set(result["x"])
    for i in range(concurrency):
        assert i in ranks, f"rank {i} not found in {ranks}"


@pytest.mark.parametrize("concurrency", [1, 2, 4])
def test_stateful_udf_context_resource_request(concurrency):
    @udf(return_dtype=DataType.int64(), num_cpus=1, memory_bytes=5_000_000)
    class TestResourceRequest:
        def __init__(self, resource_request: ResourceRequest):
            self.resource_request = resource_request

            actor_context = get_actor_context()
            assert actor_context.resource_request == self.resource_request

        def __call__(self, data):
            actor_context = get_actor_context()
            assert actor_context.resource_request == self.resource_request

            import time

            time.sleep(0.1)

            return data

    TestResourceRequest = TestResourceRequest.with_concurrency(concurrency)
    TestResourceRequest = TestResourceRequest.with_init_args(ResourceRequest(num_cpus=1, memory_bytes=5_000_000))

    df = daft.from_pydict({"x": [1, 2, 3, 4]})
    df = df.into_partitions(4)
    df = df.select(TestResourceRequest(df["x"]))

    df.collect()


@pytest.mark.parametrize("concurrency", [1, 2])
@pytest.mark.parametrize("num_gpus", [1, 2])
def test_stateful_udf_cuda_env_var(concurrency, num_gpus):
    if concurrency * num_gpus > len(cuda_visible_devices()):
        pytest.skip("Not enough GPUs available")

    @udf(return_dtype=DataType.string(), num_gpus=num_gpus)
    class GetCudaVisibleDevices:
        def __init__(self):
            self.cuda_visible_devices = os.environ["CUDA_VISIBLE_DEVICES"]

        def __call__(self, data):
            assert os.environ["CUDA_VISIBLE_DEVICES"] == self.cuda_visible_devices

            import time

            time.sleep(0.1)

            return [self.cuda_visible_devices] * len(data)

    GetCudaVisibleDevices = GetCudaVisibleDevices.with_concurrency(concurrency)

    df = daft.from_pydict({"x": [1, 2, 3, 4]})
    df = df.into_partitions(4)
    df = df.select(GetCudaVisibleDevices(df["x"]))

    result = df.to_pydict()

    unique_visible_devices = set(result["x"])
    assert len(unique_visible_devices) == concurrency

    all_devices = (",".join(unique_visible_devices)).split(",")
    assert len(all_devices) == concurrency * num_gpus


@pytest.mark.skipif(len(cuda_visible_devices()) == 0, reason="No GPUs available")
def test_stateful_udf_fractional_gpu():
    @udf(return_dtype=DataType.string(), num_gpus=0.5)
    class FractionalGpuUdf:
        def __init__(self):
            self.cuda_visible_devices = os.environ["CUDA_VISIBLE_DEVICES"]

        def __call__(self, data):
            assert os.environ["CUDA_VISIBLE_DEVICES"] == self.cuda_visible_devices

            import time

            time.sleep(0.1)

            return [self.cuda_visible_devices] * len(data)

    df = daft.from_pydict({"x": [1, 2]})
    df = df.into_partitions(2)
    df = df.select(FractionalGpuUdf(df["x"]))

    result = df.to_pydict()

    unique_visible_devices = set(result["x"])
    assert len(unique_visible_devices) == 1
