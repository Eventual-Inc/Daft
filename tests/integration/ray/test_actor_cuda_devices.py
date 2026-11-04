from __future__ import annotations

import os
from contextlib import contextmanager

import pytest
import ray

import daft
from daft.datatype import DataType
from daft.internal.gpu import cuda_visible_devices
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(get_tests_daft_runner_name() == "native", reason="Tests Ray-specific behavior")


@contextmanager
def reset_runner_with_gpus(num_gpus):
    """If current runner does not have enough GPUs, create a new runner with mocked GPU resources."""
    if len(cuda_visible_devices()) < num_gpus:
        assert get_tests_daft_runner_name() == "ray"
        try:
            ray.shutdown()
            ray.init(num_gpus=num_gpus)
            yield
        finally:
            ray.shutdown()
            ray.init()
    else:
        yield


@pytest.mark.parametrize("max_concurrency", [1, 2])
@pytest.mark.parametrize("num_gpus", [1, 2])
def test_actor_pool_udf_cuda_env_var(max_concurrency, num_gpus):
    with reset_runner_with_gpus(max_concurrency * num_gpus):

        @daft.cls(gpus=num_gpus, max_concurrency=max_concurrency)
        class GetCudaVisibleDevices:
            def __init__(self):
                self.cuda_visible_devices = os.environ["CUDA_VISIBLE_DEVICES"]

            @daft.method.batch(return_dtype=DataType.string())
            def __call__(self, data):
                assert os.environ["CUDA_VISIBLE_DEVICES"] == self.cuda_visible_devices

                import time

                time.sleep(0.1)

                return [self.cuda_visible_devices] * len(data)

        df = daft.from_pydict({"x": [1, 2, 3, 4]})
        df = df.repartition(4)
        df = df.select(GetCudaVisibleDevices()(df["x"]))

        result = df.to_pydict()

        unique_visible_devices = set(result["x"])
        assert len(unique_visible_devices) == max_concurrency

        all_devices = (",".join(unique_visible_devices)).split(",")
        assert len(all_devices) == max_concurrency * num_gpus


def test_actor_pool_udf_fractional_gpu():
    with reset_runner_with_gpus(1):

        @daft.cls(gpus=0.5, max_concurrency=2)
        class FractionalGpuUdf:
            def __init__(self):
                self.cuda_visible_devices = os.environ["CUDA_VISIBLE_DEVICES"]

            @daft.method.batch(return_dtype=DataType.string())
            def __call__(self, data):
                assert os.environ["CUDA_VISIBLE_DEVICES"] == self.cuda_visible_devices

                import time

                time.sleep(0.1)

                return [self.cuda_visible_devices] * len(data)

        df = daft.from_pydict({"x": [1, 2]})
        df = df.into_partitions(2)
        df = df.select(FractionalGpuUdf()(df["x"]))

        result = df.to_pydict()

        unique_visible_devices = set(result["x"])
        assert len(unique_visible_devices) == 1
