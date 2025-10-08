from __future__ import annotations

import contextlib
import io
import os
import subprocess
import sys

import pytest

import daft
from tests.conftest import get_tests_daft_runner_name


@contextlib.contextmanager
def with_null_env():
    old_daft_runner = os.getenv("DAFT_RUNNER")
    del os.environ["DAFT_RUNNER"]

    try:
        yield
    finally:
        if old_daft_runner is not None:
            os.environ["DAFT_RUNNER"] = old_daft_runner


def test_explicit_set_runner_native():
    """Test that a freshly imported context doesn't have a runner config set and can be set explicitly to Native."""
    explicit_set_runner_script_native = """
import daft
print(daft.runners._get_runner())
daft.context.set_runner_native()
print(daft.runners._get_runner().name)
    """

    with with_null_env():
        result = subprocess.run(
            [sys.executable, "-c", explicit_set_runner_script_native],
            capture_output=True,
        )
        assert result.stdout.decode().strip() == "None\nnative"


def test_implicit_set_runner_native():
    """Test that a freshly imported context doesn't have a runner config set and is set implicitly to Native."""
    implicit_set_runner_script = """
import daft
print(daft.runners._get_runner())
df = daft.from_pydict({"foo": [1, 2, 3]})
print(daft.runners._get_runner().name)
    """

    with with_null_env():
        result = subprocess.run([sys.executable, "-c", implicit_set_runner_script], capture_output=True)
        assert result.stdout.decode().strip() == "None\nnative"


def test_explicit_set_runner_ray():
    """Test that a freshly imported context doesn't have a runner config set and can be set explicitly to Ray."""
    explicit_set_runner_script_ray = """
import daft
print(daft.runners._get_runner())
daft.context.set_runner_ray()
print(daft.runners._get_runner().name)
    """

    with with_null_env():
        result = subprocess.run([sys.executable, "-c", explicit_set_runner_script_ray], capture_output=True)
        assert result.stdout.decode().strip() == "None\nray"


def test_implicit_set_runner_ray():
    """Test that a freshly imported context doesn't have a runner config set and can be set implicitly to Ray."""
    implicit_set_runner_script_ray = """
import daft
import ray
ray.init()
print(daft.runners._get_runner())
df = daft.from_pydict({"foo": [1, 2, 3]})
print(daft.runners._get_runner().name)
    """

    with with_null_env():
        result = subprocess.run([sys.executable, "-c", implicit_set_runner_script_ray], capture_output=True)
        assert result.stdout.decode().strip() == "None\nray"


def test_cannot_switch_local_to_ray():
    """Test that a runner cannot be switched from local to Ray."""
    script = """
import daft
daft.context.set_runner_native()
daft.context.set_runner_ray()
"""
    with with_null_env():
        result = subprocess.run([sys.executable, "-c", script], capture_output=True)
        assert "DaftError::InternalError Cannot set runner more than once" in result.stderr.decode().strip()


@pytest.mark.parametrize(
    "set_new_runner_command",
    [
        pytest.param("daft.context.set_runner_native()", id="ray_to_native"),
        pytest.param("daft.context.set_runner_ray()", id="ray_to_ray"),
    ],
)
def test_cannot_switch_from_ray(set_new_runner_command):
    """Test that a runner cannot be switched from Ray."""
    script = f"""
import daft
daft.context.set_runner_ray()
{set_new_runner_command}
"""
    with with_null_env():
        result = subprocess.run([sys.executable, "-c", script], capture_output=True)
        assert "DaftError::InternalError Cannot set runner more than once" in result.stderr.decode().strip()


@pytest.mark.parametrize("daft_runner_envvar", ["ray", "native"])
def test_env_var(daft_runner_envvar):
    """Test that environment variables are correctly picked up."""
    autodetect_script = """
import daft
df = daft.from_pydict({"foo": [1, 2, 3]})
print(daft.runners._get_runner().name)
    """

    with with_null_env():
        result = subprocess.run(
            [sys.executable, "-c", autodetect_script],
            capture_output=True,
            env={"DAFT_RUNNER": daft_runner_envvar},
        )
        assert result.stdout.decode().strip() == daft_runner_envvar


def test_in_ray_job():
    """Test that Ray job ID environment variable is being detected."""
    autodetect_script = """
import daft
df = daft.from_pydict({"foo": [1, 2, 3]})
print(daft.runners._get_runner().name)
    """

    with with_null_env():
        result = subprocess.run(
            [sys.executable, "-c", autodetect_script],
            capture_output=True,
            env={"RAY_JOB_ID": "dummy"},
        )
        assert result.stdout.decode().strip() == "ray"


def test_get_or_create_runner_from_multiple_threads():
    concurrent_get_or_create_runner_script = """
import concurrent.futures
from daft.runners import get_or_create_runner

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    futures = [
        executor.submit(lambda: get_or_create_runner()) for _ in range(10)
    ]

    results = [future.result() for future in concurrent.futures.as_completed(futures)]
    print("ok")
    """

    with with_null_env():
        result = subprocess.run(
            [sys.executable, "-c", concurrent_get_or_create_runner_script],
            capture_output=True,
        )
        assert result.stdout.decode().strip() == "ok"


def test_cannot_set_runner_ray_after_py():
    cannot_set_runner_ray_after_py_script = """
import daft
df = daft.from_pydict({"foo": [1, 2, 3]})
print(daft.runners._get_runner().name)
daft.context.set_runner_ray()
    """

    with with_null_env():
        result = subprocess.run(
            [sys.executable, "-c", cannot_set_runner_ray_after_py_script],
            capture_output=True,
        )
        assert result.stdout.decode().strip() in {"native"}
        assert "DaftError::InternalError Cannot set runner more than once" in result.stderr.decode().strip()


@pytest.mark.parametrize("daft_runner_envvar", ["ray", "native"])
def test_get_or_infer_runner_type_from_env(daft_runner_envvar):
    get_or_infer_runner_type_py_script = """
import daft

print(daft.runners.get_or_infer_runner_type())

@daft.udf(return_dtype=daft.DataType.string())
def my_udf(foo):
    runner_type = daft.runners.get_or_infer_runner_type()
    return [f"{runner_type}_{f}" for f in foo]

df = daft.from_pydict({"foo": [7]})
pd = df.with_column(column_name="bar", expr=my_udf(df["foo"])).to_pydict()
print(pd["bar"][0])
    """

    with with_null_env():
        result = subprocess.run(
            [sys.executable, "-c", get_or_infer_runner_type_py_script],
            capture_output=True,
            env={"DAFT_RUNNER": daft_runner_envvar},
        )

        assert result.stdout.decode().strip() == f"{daft_runner_envvar}\n{daft_runner_envvar}_7"


def test_get_or_infer_runner_type_with_set_runner_native():
    get_or_infer_runner_type_py_script = """
import daft

daft.context.set_runner_native()

print(daft.runners.get_or_infer_runner_type())


@daft.udf(return_dtype=daft.DataType.string())
def my_udf(foo):
    runner_type = daft.runners.get_or_infer_runner_type()
    return [f"{runner_type}_{f}" for f in foo]


df = daft.from_pydict({"foo": [7]})
pd = df.with_column(column_name="bar", expr=my_udf(df["foo"])).to_pydict()
print(pd["bar"][0])
    """

    with with_null_env():
        result = subprocess.run([sys.executable, "-c", get_or_infer_runner_type_py_script], capture_output=True)
        assert result.stdout.decode().strip() == "native\nnative_7"


def test_get_or_infer_runner_type_with_set_runner_ray():
    get_or_infer_runner_type_py_script = """
import daft

daft.context.set_runner_ray()

print(daft.runners.get_or_infer_runner_type())


@daft.udf(return_dtype=daft.DataType.string())
def my_udf(foo):
    runner_type = daft.runners.get_or_infer_runner_type()
    return [f"{runner_type}_{f}" for f in foo]


df = daft.from_pydict({"foo": [7]})
pd = df.with_column(column_name="bar", expr=my_udf(df["foo"])).to_pydict()
print(pd["bar"][0])
    """

    with with_null_env():
        result = subprocess.run([sys.executable, "-c", get_or_infer_runner_type_py_script], capture_output=True)
        assert result.stdout.decode().strip() == "ray\nray_7"


@pytest.mark.parametrize("daft_runner_envvar", ["ray", "native"])
def test_get_or_infer_runner_type_with_inconsistent_settings(daft_runner_envvar):
    get_or_infer_runner_type_py_script = """
import daft

print(daft.runners.get_or_infer_runner_type())
daft.context.set_runner_ray()
print(daft.runners.get_or_infer_runner_type())
    """

    with with_null_env():
        result = subprocess.run(
            [sys.executable, "-c", get_or_infer_runner_type_py_script],
            capture_output=True,
            env={"DAFT_RUNNER": daft_runner_envvar},
        )
        assert result.stdout.decode().strip() == f"{daft_runner_envvar}\nray"


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="requires Native Runner to be in use")
def test_use_default_scantask_max_parallelism():
    with with_null_env():
        str_io = io.StringIO()
        df = daft.range(start=0, end=1024, partitions=10)
        df.explain(show_all=True, file=str_io)
        assert "Num Parallel Scan Tasks = 8" in str_io.getvalue().strip()


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="requires Native Runner to be in use")
def test_set_scantask_max_parallelism_less_than_partition_num():
    with daft.execution_config_ctx(scantask_max_parallel=7):
        str_io = io.StringIO()
        df = daft.range(start=0, end=1024, partitions=10)
        df.explain(show_all=True, file=str_io)
        assert "Num Parallel Scan Tasks = 7" in str_io.getvalue().strip()


@pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="requires Native Runner to be in use")
def test_set_scantask_max_parallelism_greater_than_partition_num():
    with daft.execution_config_ctx(scantask_max_parallel=17):
        str_io = io.StringIO()
        df = daft.range(start=0, end=1024, partitions=10)
        df.explain(show_all=True, file=str_io)
        assert "Num Parallel Scan Tasks = 10" in str_io.getvalue().strip()
