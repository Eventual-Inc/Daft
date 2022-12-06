from __future__ import annotations

import multiprocessing
import os

import pandas as pd
import psutil
import pytest

from daft import DataFrame, udf
from daft.context import get_context
from daft.expressions import col
from daft.internal.gpu import cuda_device_count
from tests.conftest import assert_df_equals


def no_gpu_available() -> bool:
    return cuda_device_count() == 0


DATA = {"id": [i for i in range(100)]}


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


@udf(return_type=int, num_gpus=cuda_device_count() + 1)
def requesting_too_many_gpus(c):
    return [1] * len(c)


@udf(return_type=int, num_cpus=multiprocessing.cpu_count() + 1)
def requesting_too_many_cpus(c):
    return [1] * len(c)


@udf(return_type=int, memory_bytes=psutil.virtual_memory().total + 1)
def requesting_too_much_memory(c):
    return [1] * len(c)


###
# Assert PyRunner behavior for GPU requests:
# Fail if requesting more GPUs than is available, but otherwise we do not modify anything
# about local task execution (tasks have access to all GPUs available on the host,
# even if they specify more)
###


@pytest.mark.skipif(get_context().runner_config.name != "py", reason="requires PyRunner to be in use")
def test_requesting_too_many_cpus():
    df = DataFrame.from_pydict(DATA)
    df = df.with_column("foo", requesting_too_many_cpus(col("id")))

    with pytest.raises(RuntimeError):
        df.to_pandas()


@pytest.mark.skipif(get_context().runner_config.name != "py", reason="requires PyRunner to be in use")
def test_requesting_too_many_gpus():
    df = DataFrame.from_pydict(DATA)
    df = df.with_column("foo", requesting_too_many_gpus(col("id")))

    with pytest.raises(RuntimeError):
        df.to_pandas()


@pytest.mark.skipif(get_context().runner_config.name != "py", reason="requires PyRunner to be in use")
def test_requesting_too_much_memory():
    df = DataFrame.from_pydict(DATA)
    df = df.with_column("foo", requesting_too_much_memory(col("id")))

    with pytest.raises(RuntimeError):
        df.to_pandas()


@pytest.mark.skipif(get_context().runner_config.name != "py", reason="requires PyRunner to be in use")
@pytest.mark.skipif(no_gpu_available(), reason="requires GPUs to be available")
def test_with_column_pyrunner():
    df = DataFrame.from_pydict(DATA).repartition(5)
    f = udf(return_type=int, num_gpus=1)(assert_num_cuda_visible_devices)
    # We do not do any masking of devices for the local PyRunner, even if the user requests fewer GPUs
    # than the host actually has.
    df = df.with_column("foo", f(col("id"), num_gpus=cuda_device_count()))
    pd_df = pd.DataFrame.from_dict(DATA)
    pd_df["foo"] = pd_df["id"]
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")


###
# Assert RayRunner behavior for GPU requests:
# Number of GPUs reported by each task should be exactly equal to the number requested.
###


@udf(return_type=int, num_gpus=1)
def noop_assert_gpu_available(c):
    cuda_visible_devices = os.getenv("CUDA_VISIBLE_DEVICES")
    assert (
        cuda_visible_devices is not None and cuda_visible_devices != "" and len(cuda_visible_devices.split(",")) == 1
    ), "One and only one GPU must be made available"
    return c


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="requires RayRunner to be in use")
@pytest.mark.skipif(no_gpu_available(), reason="requires GPUs to be available")
@pytest.mark.parametrize("num_gpus", [None, 1])
def test_with_column_rayrunner(num_gpus):
    df = DataFrame.from_pydict(DATA).repartition(2)
    f = udf(return_type=int, num_gpus=num_gpus)(assert_num_cuda_visible_devices)
    df = df.with_column("num_cuda_visible_devices", f(col("id"), num_gpus=num_gpus if num_gpus is not None else 0))
    pd_df = pd.DataFrame.from_dict(DATA)
    pd_df["num_cuda_visible_devices"] = pd_df["id"]
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="requires RayRunner to be in use")
@pytest.mark.skipif(no_gpu_available(), reason="requires GPUs to be available")
def test_with_column_max_resources_rayrunner():
    df = DataFrame.from_pydict(DATA).repartition(2)
    f0 = udf(return_type=int, num_gpus=0)(assert_num_cuda_visible_devices)
    f1 = udf(return_type=int, num_gpus=1)(assert_num_cuda_visible_devices)
    # This operation will take the max resource request of f0 and f1, hence both ops will have num_gpus=1
    df = df.select(
        col("id"),
        f0(col("id"), num_gpus=1).alias("f0"),
        f1(col("id"), num_gpus=1).alias("f1"),
    )
    pd_df = pd.DataFrame.from_dict(DATA)
    pd_df["f0"] = pd_df["id"]
    pd_df["f1"] = pd_df["id"]
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="requires RayRunner to be in use")
@pytest.mark.skipif(no_gpu_available(), reason="requires GPUs to be available")
def test_sort_rayrunner():
    df = DataFrame.from_pydict(DATA).repartition(2)
    df = df.sort(noop_assert_gpu_available(col("id")))
    pd_df = pd.DataFrame.from_dict(DATA)
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")


@pytest.mark.skipif(get_context().runner_config.name != "ray", reason="requires RayRunner to be in use")
@pytest.mark.skipif(no_gpu_available(), reason="requires GPUs to be available")
def test_repartition_rayrunner():
    df = DataFrame.from_pydict(DATA).repartition(2, noop_assert_gpu_available(col("id")))
    pd_df = pd.DataFrame.from_dict(DATA)
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")
