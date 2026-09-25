from __future__ import annotations

import os

import pytest
import ray

import daft
from daft.expressions import col
from daft.internal.gpu import cuda_visible_devices
from tests.conftest import get_tests_daft_runner_name


def no_gpu_available() -> bool:
    return len(cuda_visible_devices()) == 0


DATA = {"id": [i for i in range(100)]}


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="requires Native Runner to be in use")
def test_requesting_too_many_cpus():
    df = daft.from_pydict(DATA)

    @daft.func(return_dtype=daft.DataType.int64(), cpus=1000)
    def my_udf(c):
        return 1

    df = df.with_column("foo", my_udf(col("id")))

    with pytest.raises(Exception):
        df.collect()


###
# Assert RayRunner behavior for requests
###


class _AssertResources:
    @daft.method(return_dtype=daft.DataType.int64())
    def __call__(self, c, num_cpus=None, num_gpus=None):
        assigned_resources = ray.get_runtime_context().get_assigned_resources()

        for resource, ray_resource_key in [(num_cpus, "CPU"), (num_gpus, "GPU")]:
            if resource is None:
                assert ray_resource_key not in assigned_resources or assigned_resources[ray_resource_key] is None
            else:
                assert ray_resource_key in assigned_resources
                assert assigned_resources[ray_resource_key] == resource

        return c


RAY_VERSION_LT_2 = int(ray.__version__.split(".")[0]) < 2


@pytest.mark.skipif(
    RAY_VERSION_LT_2, reason="The ray.get_runtime_context().get_assigned_resources() was only added in Ray >= 2.0"
)
@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_with_column_rayrunner():
    assert_resources_cls = daft.cls(cpus=1, max_concurrency=1)(_AssertResources)

    df = daft.from_pydict(DATA).repartition(2)
    assert_resources = assert_resources_cls()
    df = df.with_column(
        "resources_ok",
        assert_resources(col("id"), num_cpus=1),
    )

    df.collect()


@pytest.mark.skipif(
    RAY_VERSION_LT_2, reason="The ray.get_runtime_context().get_assigned_resources() was only added in Ray >= 2.0"
)
@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
def test_with_column_folded_rayrunner():
    # UDFs with different resource requests must not be folded onto the same actors
    assert_resources_1_cpu_cls = daft.cls(cpus=1, max_concurrency=1)(_AssertResources)
    assert_resources_2_cpu_cls = daft.cls(cpus=2, max_concurrency=1)(_AssertResources)

    df = daft.from_pydict(DATA).repartition(2)
    df = df.with_column(
        "one_cpu_request",
        assert_resources_1_cpu_cls()(col("id"), num_cpus=1),
    )
    df = df.with_column(
        "two_cpu_request",
        assert_resources_2_cpu_cls()(col("id"), num_cpus=2),
    )
    df.collect()


###
# GPU tests - can only run if machine has a GPU
###


class _AssertNumCudaVisibleDevices:
    @daft.method(return_dtype=daft.DataType.int64())
    def __call__(self, c, num_gpus: int = 0):
        cuda_visible_devices_env = os.getenv("CUDA_VISIBLE_DEVICES")
        # Env var not set: program is free to use any number of GPUs
        if cuda_visible_devices_env is None:
            result = len(cuda_visible_devices())
        # Env var set to empty: program has no access to any GPUs
        elif cuda_visible_devices_env == "":
            result = 0
        else:
            result = len(cuda_visible_devices_env.split(","))
        assert result == num_gpus
        return c


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
@pytest.mark.skipif(no_gpu_available(), reason="requires GPUs to be available")
@pytest.mark.parametrize("num_gpus", [0, 1])
def test_with_column_rayrunner_gpu(num_gpus):
    assert_num_cuda_visible_devices_cls = daft.cls(gpus=num_gpus, max_concurrency=1)(_AssertNumCudaVisibleDevices)

    df = daft.from_pydict(DATA).repartition(2)
    df = df.with_column(
        "num_cuda_visible_devices",
        assert_num_cuda_visible_devices_cls()(col("id"), num_gpus=num_gpus),
    )

    df.collect()


@pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")
@pytest.mark.skipif(no_gpu_available(), reason="requires GPUs to be available")
def test_with_column_max_resources_rayrunner_gpu():
    assert_num_cuda_visible_devices_0_cls = daft.cls(gpus=0, max_concurrency=1)(_AssertNumCudaVisibleDevices)
    assert_num_cuda_visible_devices_1_cls = daft.cls(gpus=1, max_concurrency=1)(_AssertNumCudaVisibleDevices)

    df = daft.from_pydict(DATA).repartition(2)

    # Because of projection folding optimizations, both UDFs should run with num_gpus=1 even though 0_gpu_col requested for 0 GPUs
    df = df.with_column(
        "0_gpu_col",
        assert_num_cuda_visible_devices_0_cls()(col("id"), num_gpus=1),
    )
    df = df.with_column(
        "1_gpu_col",
        assert_num_cuda_visible_devices_1_cls()(col("id"), num_gpus=1),
    )

    df.collect()


def test_improper_num_gpus():
    with pytest.raises(ValueError):

        @daft.func(return_dtype=daft.DataType.int64(), gpus=-1)
        def foo(c):
            return c

    with pytest.raises(ValueError):

        @daft.func(return_dtype=daft.DataType.int64(), gpus=1.5)
        def foo(c):
            return c
