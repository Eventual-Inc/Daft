from __future__ import annotations

import os
from contextlib import contextmanager

import pytest
import ray

import daft
from daft import udf
from daft.context import get_context, set_planning_config
from daft.datatype import DataType
from daft.internal.gpu import cuda_visible_devices
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() == "native", reason="Native runner does not support GPU tests yet"
)


@pytest.fixture(scope="module")
def enable_actor_pool():
    try:
        original_config = get_context().daft_planning_config

        set_planning_config(
            config=get_context().daft_planning_config.with_config_values(enable_actor_pool_projections=True)
        )
        yield
    finally:
        set_planning_config(config=original_config)


@contextmanager
def reset_runner_with_gpus(num_gpus, monkeypatch):
    """If current runner does not have enough GPUs, create a new runner with mocked GPU resources"""
    if len(cuda_visible_devices()) < num_gpus:
        if get_tests_daft_runner_name() == "ray":
            try:
                ray.shutdown()
                ray.init(num_gpus=num_gpus)
                yield
            finally:
                ray.shutdown()
                ray.init()
        else:
            try:
                monkeypatch.setenv("CUDA_VISIBLE_DEVICES", ",".join(str(i) for i in range(num_gpus)))

                # Need to reset runner to recompute resources
                original_runner = daft.context.get_context()._runner
                daft.context.get_context()._runner = None
                yield
            finally:
                daft.context.get_context()._runner = original_runner
    else:
        yield


@pytest.mark.parametrize("concurrency", [1, 2])
@pytest.mark.parametrize("num_gpus", [1, 2])
def test_stateful_udf_cuda_env_var(enable_actor_pool, monkeypatch, concurrency, num_gpus):
    with reset_runner_with_gpus(concurrency * num_gpus, monkeypatch):

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
        df = df.repartition(4)
        df = df.select(GetCudaVisibleDevices(df["x"]))

        result = df.to_pydict()

        unique_visible_devices = set(result["x"])
        assert len(unique_visible_devices) == concurrency

        all_devices = (",".join(unique_visible_devices)).split(",")
        assert len(all_devices) == concurrency * num_gpus


def test_stateful_udf_fractional_gpu(enable_actor_pool, monkeypatch):
    with reset_runner_with_gpus(1, monkeypatch):

        @udf(return_dtype=DataType.string(), num_gpus=0.5)
        class FractionalGpuUdf:
            def __init__(self):
                self.cuda_visible_devices = os.environ["CUDA_VISIBLE_DEVICES"]

            def __call__(self, data):
                assert os.environ["CUDA_VISIBLE_DEVICES"] == self.cuda_visible_devices

                import time

                time.sleep(0.1)

                return [self.cuda_visible_devices] * len(data)

        FractionalGpuUdf = FractionalGpuUdf.with_concurrency(2)

        df = daft.from_pydict({"x": [1, 2]})
        df = df.into_partitions(2)
        df = df.select(FractionalGpuUdf(df["x"]))

        result = df.to_pydict()

        unique_visible_devices = set(result["x"])
        assert len(unique_visible_devices) == 1


@pytest.mark.skipif(get_tests_daft_runner_name() != "py", reason="Test can only be run on PyRunner")
def test_stateful_udf_no_cuda_devices(enable_actor_pool, monkeypatch):
    monkeypatch.setattr(daft.internal.gpu, "_raw_device_count_nvml", lambda: 0)
    monkeypatch.delenv("CUDA_VISIBLE_DEVICES", raising=False)

    original_runner = daft.context.get_context()._runner

    try:
        daft.context.get_context()._runner = None

        @udf(return_dtype=DataType.string(), num_gpus=1)
        class UdfWithGpus:
            def __init__(self):
                pass

            def __call__(self, data):
                return [str(i) for i in range(len(data))]

        UdfWithGpus = UdfWithGpus.with_concurrency(1)

        df = daft.from_pydict({"x": [1, 2, 3, 4]})
        df = df.select(UdfWithGpus(df["x"]))

        with pytest.raises(RuntimeError):
            df.collect()
    finally:
        daft.context.get_context()._runner = original_runner


@pytest.mark.skipif(get_tests_daft_runner_name() != "py", reason="Test can only be run on PyRunner")
def test_stateful_udf_no_cuda_visible_device_envvar(enable_actor_pool, monkeypatch):
    monkeypatch.setattr(daft.internal.gpu, "_raw_device_count_nvml", lambda: 1)
    monkeypatch.delenv("CUDA_VISIBLE_DEVICES", raising=False)

    original_runner = daft.context.get_context()._runner

    try:
        daft.context.get_context()._runner = None

        @udf(return_dtype=DataType.string(), num_gpus=1)
        class UdfWithGpus:
            def __init__(self):
                pass

            def __call__(self, data):
                return [str(i) for i in range(len(data))]

        UdfWithGpus = UdfWithGpus.with_concurrency(1)

        df = daft.from_pydict({"x": [1, 2, 3, 4]})
        df = df.select(UdfWithGpus(df["x"]))

        df.collect()

        UdfWithGpus2 = UdfWithGpus.with_concurrency(2)

        df = daft.from_pydict({"x": [1, 2, 3, 4]})
        df = df.select(UdfWithGpus2(df["x"]))

        with pytest.raises(RuntimeError):
            df.collect()
    finally:
        daft.context.get_context()._runner = original_runner
