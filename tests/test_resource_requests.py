from __future__ import annotations

import multiprocessing
import os

import psutil
import pytest
import ray

import daft
from daft import ResourceRequest, udf
from daft.context import get_context
from daft.expressions import col
from daft.internal.gpu import cuda_device_count


def no_gpu_available() -> bool:
    return cuda_device_count() == 0


DATA = {"id": [i for i in range(100)]}


@udf(return_dtype=daft.DataType.int64())
def my_udf(c):
    return [1] * len(c)


###
# Assert PyRunner behavior for GPU requests:
# Fail if requesting more GPUs than is available, but otherwise we do not modify anything
# about local task execution (tasks have access to all GPUs available on the host,
# even if they specify more)
###


@pytest.mark.skipif(get_context().runner_config.name not in {"py"}, reason="requires PyRunner to be in use")
def test_requesting_too_many_cpus():
    df = daft.from_pydict(DATA)

    df = df.with_column(
        "foo",
        my_udf(col("id")),
        resource_request=ResourceRequest(num_cpus=multiprocessing.cpu_count() + 1),
    )

    with pytest.raises(RuntimeError):
        df.collect()


@pytest.mark.skipif(get_context().runner_config.name not in {"py"}, reason="requires PyRunner to be in use")
def test_requesting_too_many_gpus():
    df = daft.from_pydict(DATA)
    df = df.with_column("foo", my_udf(col("id")), resource_request=ResourceRequest(num_gpus=cuda_device_count() + 1))

    with pytest.raises(RuntimeError):
        df.collect()


@pytest.mark.skipif(get_context().runner_config.name not in {"py"}, reason="requires PyRunner to be in use")
def test_requesting_too_much_memory():
    df = daft.from_pydict(DATA)

    df = df.with_column(
        "foo",
        my_udf(col("id")),
        resource_request=ResourceRequest(memory_bytes=psutil.virtual_memory().total + 1),
    )

    with pytest.raises(RuntimeError):
        df.collect()


###
# Assert RayRunner behavior for requests
###


@udf(return_dtype=daft.DataType.int64())
def assert_resources(c, num_cpus=None, num_gpus=None, memory=None):
    assigned_resources = ray.get_runtime_context().get_assigned_resources()

    for resource, ray_resource_key in [(num_cpus, "CPU"), (num_gpus, "GPU"), (memory, "memory")]:
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
@pytest.mark.skipif(get_context().runner_config.name not in {"ray"}, reason="requires RayRunner to be in use")
def test_with_column_rayrunner():
    df = daft.from_pydict(DATA).repartition(2)

    df = df.with_column(
        "resources_ok",
        assert_resources(col("id"), num_cpus=1, num_gpus=None, memory=1_000_000),
        resource_request=ResourceRequest(num_cpus=1, memory_bytes=1_000_000, num_gpus=None),
    )

    df.collect()


@pytest.mark.skipif(
    RAY_VERSION_LT_2, reason="The ray.get_runtime_context().get_assigned_resources() was only added in Ray >= 2.0"
)
@pytest.mark.skipif(get_context().runner_config.name not in {"ray"}, reason="requires RayRunner to be in use")
def test_with_column_folded_rayrunner():
    df = daft.from_pydict(DATA).repartition(2)

    # Because of Projection Folding optimizations, the expected resource request is the max of the three .with_column requests
    expected = dict(num_cpus=1, num_gpus=None, memory=5_000_000)
    df = df.with_column(
        "no_requests",
        assert_resources(col("id"), **expected),
    )
    df = df.with_column(
        "more_memory_request",
        assert_resources(col("id"), **expected),
        resource_request=ResourceRequest(num_cpus=1, memory_bytes=5_000_000, num_gpus=None),
    )
    df = df.with_column(
        "more_cpu_request",
        assert_resources(col("id"), **expected),
        resource_request=ResourceRequest(num_cpus=1, memory_bytes=None, num_gpus=None),
    )
    df.collect()


###
# GPU tests - can only run if machine has a GPU
###


@udf(return_dtype=daft.DataType.int64())
def assert_num_cuda_visible_devices(c, num_gpus: int = 0):
    cuda_visible_devices = os.getenv("CUDA_VISIBLE_DEVICES")
    # Env var not set: program is free to use any number of GPUs
    if cuda_visible_devices is None:
        result = cuda_device_count()
    # Env var set to empty: program has no access to any GPUs
    elif cuda_visible_devices == "":
        result = 0
    else:
        result = len(cuda_visible_devices.split(","))
    assert result == num_gpus
    return c


@pytest.mark.skipif(get_context().runner_config.name not in {"py"}, reason="requires PyRunner to be in use")
@pytest.mark.skipif(no_gpu_available(), reason="requires GPUs to be available")
def test_with_column_pyrunner_gpu():
    df = daft.from_pydict(DATA).repartition(5)

    # We do not do any masking of devices for the local PyRunner, even if the user requests fewer GPUs
    # than the host actually has.
    df = df.with_column(
        "foo",
        assert_num_cuda_visible_devices(col("id"), num_gpus=cuda_device_count()),
        resource_request=ResourceRequest(num_gpus=1),
    )

    df.collect()


@pytest.mark.skipif(get_context().runner_config.name not in {"ray"}, reason="requires RayRunner to be in use")
@pytest.mark.skipif(no_gpu_available(), reason="requires GPUs to be available")
@pytest.mark.parametrize("num_gpus", [None, 1])
def test_with_column_rayrunner_gpu(num_gpus):
    df = daft.from_pydict(DATA).repartition(2)

    df = df.with_column(
        "num_cuda_visible_devices",
        assert_num_cuda_visible_devices(col("id"), num_gpus=num_gpus if num_gpus is not None else 0),
        resource_request=ResourceRequest(num_gpus=num_gpus),
    )

    df.collect()


@pytest.mark.skipif(get_context().runner_config.name not in {"ray"}, reason="requires RayRunner to be in use")
@pytest.mark.skipif(no_gpu_available(), reason="requires GPUs to be available")
def test_with_column_max_resources_rayrunner_gpu():
    df = daft.from_pydict(DATA).repartition(2)

    # Because of projection folding optimizations, both UDFs should run with num_gpus=1 even though 0_gpu_col requested for 0 GPUs
    df = df.with_column(
        "0_gpu_col",
        assert_num_cuda_visible_devices(col("id"), num_gpus=1),
        resource_request=ResourceRequest(num_gpus=0),
    )
    df = df.with_column(
        "1_gpu_col",
        assert_num_cuda_visible_devices(col("id"), num_gpus=1),
        resource_request=ResourceRequest(num_gpus=1),
    )

    df.collect()
