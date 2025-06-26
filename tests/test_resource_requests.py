from __future__ import annotations

import copy
import os

import pytest
import ray

import daft
from daft import udf
from daft.daft import SystemInfo
from daft.expressions import col
from daft.internal.gpu import cuda_visible_devices
from tests.conftest import get_tests_daft_runner_name


def no_gpu_available() -> bool:
    return len(cuda_visible_devices()) == 0


DATA = {"id": [i for i in range(100)]}


@udf(return_dtype=daft.DataType.int64())
def my_udf(c):
    return [1] * len(c)


###
# Test behavior of overriding options
###


def test_partial_resource_request_overrides():
    new_udf = my_udf.override_options(num_cpus=1.0)
    assert new_udf.resource_request.num_cpus == 1.0
    assert new_udf.resource_request.num_gpus is None
    assert new_udf.resource_request.memory_bytes is None

    new_udf = new_udf.override_options(num_gpus=8.0)
    assert new_udf.resource_request.num_cpus == 1.0
    assert new_udf.resource_request.num_gpus == 8.0
    assert new_udf.resource_request.memory_bytes is None

    new_udf = new_udf.override_options(num_gpus=None)
    assert new_udf.resource_request.num_cpus == 1.0
    assert new_udf.resource_request.num_gpus is None
    assert new_udf.resource_request.memory_bytes is None

    new_udf = new_udf.override_options(memory_bytes=100)
    assert new_udf.resource_request.num_cpus == 1.0
    assert new_udf.resource_request.num_gpus is None
    assert new_udf.resource_request.memory_bytes == 100


def test_resource_request_pickle_roundtrip():
    new_udf = my_udf.override_options(num_cpus=1.0)
    assert new_udf.resource_request.num_cpus == 1.0
    assert new_udf.resource_request.num_gpus is None
    assert new_udf.resource_request.memory_bytes is None

    assert new_udf == copy.deepcopy(new_udf)

    new_udf = new_udf.override_options(num_gpus=8.0)
    assert new_udf.resource_request.num_cpus == 1.0
    assert new_udf.resource_request.num_gpus == 8.0
    assert new_udf.resource_request.memory_bytes is None
    assert new_udf == copy.deepcopy(new_udf)


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="requires Native Runner to be in use")
def test_requesting_too_many_cpus():
    df = daft.from_pydict(DATA)

    my_udf_parametrized = my_udf.override_options(num_cpus=1000)
    df = df.with_column(
        "foo",
        my_udf_parametrized(col("id")),
    )

    with pytest.raises(Exception):
        df.collect()


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="requires Native Runner to be in use")
def test_requesting_too_much_memory():
    df = daft.from_pydict(DATA)
    system_info = SystemInfo()

    my_udf_parametrized = my_udf.override_options(memory_bytes=system_info.total_memory() + 1)
    df = df.with_column(
        "foo",
        my_udf_parametrized(col("id")),
    )

    with pytest.raises(Exception):
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


@udf(return_dtype=daft.DataType.int64())
class AssertResourcesStateful:
    def __init__(self):
        pass

    def __call__(self, c, num_cpus=None, num_gpus=None, memory=None):
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
@pytest.mark.skipif(get_tests_daft_runner_name() not in {"ray"}, reason="requires RayRunner to be in use")
def test_with_column_rayrunner():
    df = daft.from_pydict(DATA).repartition(2)

    assert_resources_parametrized = assert_resources.override_options(num_cpus=1, memory_bytes=1_000_000, num_gpus=None)
    df = df.with_column(
        "resources_ok",
        assert_resources_parametrized(col("id"), num_cpus=1, num_gpus=None, memory=1_000_000),
    )

    df.collect()


@pytest.mark.skipif(
    RAY_VERSION_LT_2, reason="The ray.get_runtime_context().get_assigned_resources() was only added in Ray >= 2.0"
)
@pytest.mark.skipif(get_tests_daft_runner_name() not in {"ray"}, reason="requires RayRunner to be in use")
def test_with_column_folded_rayrunner():
    df = daft.from_pydict(DATA).repartition(2)

    # Because of Projection Folding optimizations, the expected resource request is the max of the three .with_column requests
    expected = dict(num_cpus=1, num_gpus=None, memory=5_000_000)
    df = df.with_column(
        "no_requests",
        assert_resources(col("id"), **expected),
    )

    assert_resources_1 = assert_resources.override_options(num_cpus=1, memory_bytes=5_000_000, num_gpus=None)
    assert_resources_2 = assert_resources.override_options(num_cpus=1, memory_bytes=None, num_gpus=None)
    df = df.with_column(
        "more_memory_request",
        assert_resources_1(col("id"), **expected),
    )
    df = df.with_column(
        "more_cpu_request",
        assert_resources_2(col("id"), **expected),
    )
    df.collect()


@pytest.mark.skipif(
    RAY_VERSION_LT_2, reason="The ray.get_runtime_context().get_assigned_resources() was only added in Ray >= 2.0"
)
@pytest.mark.skipif(get_tests_daft_runner_name() not in {"ray"}, reason="requires RayRunner to be in use")
def test_with_column_rayrunner_class():
    assert_resources = AssertResourcesStateful.with_concurrency(1)

    df = daft.from_pydict(DATA).repartition(2)

    assert_resources_parametrized = assert_resources.override_options(num_cpus=1, memory_bytes=1_000_000, num_gpus=None)
    df = df.with_column(
        "resources_ok",
        assert_resources_parametrized(col("id"), num_cpus=1, num_gpus=None, memory=1_000_000),
    )

    df.collect()


@pytest.mark.skipif(
    RAY_VERSION_LT_2, reason="The ray.get_runtime_context().get_assigned_resources() was only added in Ray >= 2.0"
)
@pytest.mark.skipif(get_tests_daft_runner_name() not in {"ray"}, reason="requires RayRunner to be in use")
def test_with_column_folded_rayrunner_class():
    assert_resources = AssertResourcesStateful.with_concurrency(1)

    df = daft.from_pydict(DATA).repartition(2)

    df = df.with_column(
        "no_requests",
        assert_resources(col("id"), num_cpus=1),  # UDFs have 1 CPU by default
    )

    assert_resources_1 = assert_resources.override_options(num_cpus=1, memory_bytes=5_000_000)
    df = df.with_column(
        "more_memory_request",
        assert_resources_1(col("id"), num_cpus=1, memory=5_000_000),
    )
    df.collect()


###
# GPU tests - can only run if machine has a GPU
###


@udf(return_dtype=daft.DataType.int64(), num_gpus=1)
def assert_num_cuda_visible_devices(c, num_gpus: int = 0):
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


@pytest.mark.skipif(get_tests_daft_runner_name() not in {"ray"}, reason="requires RayRunner to be in use")
@pytest.mark.skipif(no_gpu_available(), reason="requires GPUs to be available")
@pytest.mark.parametrize("num_gpus", [None, 1])
def test_with_column_rayrunner_gpu(num_gpus):
    df = daft.from_pydict(DATA).repartition(2)

    assert_num_cuda_visible_devices_parametrized = assert_num_cuda_visible_devices.override_options(num_gpus=num_gpus)
    df = df.with_column(
        "num_cuda_visible_devices",
        assert_num_cuda_visible_devices_parametrized(col("id"), num_gpus=num_gpus if num_gpus is not None else 0),
    )

    df.collect()


@pytest.mark.skipif(get_tests_daft_runner_name() not in {"ray"}, reason="requires RayRunner to be in use")
@pytest.mark.skipif(no_gpu_available(), reason="requires GPUs to be available")
def test_with_column_max_resources_rayrunner_gpu():
    df = daft.from_pydict(DATA).repartition(2)

    # Because of projection folding optimizations, both UDFs should run with num_gpus=1 even though 0_gpu_col requested for 0 GPUs
    assert_num_cuda_visible_devices_0 = assert_num_cuda_visible_devices.override_options(num_gpus=0)
    assert_num_cuda_visible_devices_1 = assert_num_cuda_visible_devices.override_options(num_gpus=1)
    df = df.with_column(
        "0_gpu_col",
        assert_num_cuda_visible_devices_0(col("id"), num_gpus=1),
    )
    df = df.with_column(
        "1_gpu_col",
        assert_num_cuda_visible_devices_1(col("id"), num_gpus=1),
    )

    df.collect()


def test_improper_num_gpus():
    with pytest.raises(ValueError, match="DaftError::ValueError"):

        @udf(return_dtype=daft.DataType.int64(), num_gpus=-1)
        def foo(c):
            return c

    with pytest.raises(ValueError, match="DaftError::ValueError"):

        @udf(return_dtype=daft.DataType.int64(), num_gpus=1.5)
        def foo(c):
            return c

    @udf(return_dtype=daft.DataType.int64())
    def foo(c):
        return c

    with pytest.raises(ValueError, match="DaftError::ValueError"):
        foo = foo.override_options(num_gpus=-1)

    with pytest.raises(ValueError, match="DaftError::ValueError"):
        foo = foo.override_options(num_gpus=1.5)
