import contextlib
import os
import subprocess
import sys

import pytest


@contextlib.contextmanager
def with_null_env():
    old_daft_runner = os.getenv("DAFT_RUNNER")
    del os.environ["DAFT_RUNNER"]

    try:
        yield
    finally:
        if old_daft_runner is not None:
            os.environ["DAFT_RUNNER"] = old_daft_runner


def test_explicit_set_runner_py():
    """Test that a freshly imported context doesn't have a runner config set and can be set explicitly to Python"""

    explicit_set_runner_script = """
import daft
print(daft.context.get_context()._runner)
daft.context.set_runner_py()
print(daft.context.get_context()._runner.name)
    """

    with with_null_env():
        result = subprocess.run([sys.executable, "-c", explicit_set_runner_script], capture_output=True)
        assert result.stdout.decode().strip() == "None\npy"


def test_implicit_set_runner_py():
    """Test that a freshly imported context doesn't have a runner config set and can be set implicitly to Python"""

    implicit_set_runner_script = """
import daft
print(daft.context.get_context()._runner)
df = daft.from_pydict({"foo": [1, 2, 3]})
print(daft.context.get_context()._runner.name)
    """

    with with_null_env():
        result = subprocess.run([sys.executable, "-c", implicit_set_runner_script], capture_output=True)
        assert result.stdout.decode().strip() == "None\npy" or result.stdout.decode().strip() == "None\nnative"


def test_explicit_set_runner_ray():
    """Test that a freshly imported context doesn't have a runner config set and can be set explicitly to Ray"""

    explicit_set_runner_script_ray = """
import daft
print(daft.context.get_context()._runner)
daft.context.set_runner_ray()
print(daft.context.get_context()._runner.name)
    """

    with with_null_env():
        result = subprocess.run([sys.executable, "-c", explicit_set_runner_script_ray], capture_output=True)
        assert result.stdout.decode().strip() == "None\nray"


def test_implicit_set_runner_ray():
    """Test that a freshly imported context doesn't have a runner config set and can be set implicitly to Ray"""

    implicit_set_runner_script_ray = """
import daft
import ray
ray.init()
print(daft.context.get_context()._runner)
df = daft.from_pydict({"foo": [1, 2, 3]})
print(daft.context.get_context()._runner.name)
    """

    with with_null_env():
        result = subprocess.run([sys.executable, "-c", implicit_set_runner_script_ray], capture_output=True)
        assert result.stdout.decode().strip() == "None\nray"


def test_switch_local_runners():
    """Test that a runner can be switched from Python to Native"""

    switch_local_runners_script = """
import daft
print(daft.context.get_context()._runner)
daft.context.set_runner_py()
print(daft.context.get_context()._runner.name)
daft.context.set_runner_native()
print(daft.context.get_context()._runner.name)
daft.context.set_runner_py()
print(daft.context.get_context()._runner.name)
    """

    with with_null_env():
        result = subprocess.run([sys.executable, "-c", switch_local_runners_script], capture_output=True)
        assert result.stdout.decode().strip() == "None\npy\nnative\npy"


@pytest.mark.parametrize(
    "set_local_command",
    [
        pytest.param("daft.context.set_runner_native()", id="native_to_ray"),
        pytest.param("daft.context.set_runner_py()", id="py_to_ray"),
    ],
)
def test_cannot_switch_local_to_ray(set_local_command):
    """Test that a runner cannot be switched from local to Ray"""

    script = f"""
import daft
{set_local_command}
daft.context.set_runner_ray()
"""
    with with_null_env():
        result = subprocess.run([sys.executable, "-c", script], capture_output=True)
        assert result.stderr.decode().strip().endswith("RuntimeError: Cannot set runner more than once")


@pytest.mark.parametrize(
    "set_new_runner_command",
    [
        pytest.param("daft.context.set_runner_native()", id="ray_to_native"),
        pytest.param("daft.context.set_runner_py()", id="ray_to_py"),
        pytest.param("daft.context.set_runner_ray()", id="ray_to_ray"),
    ],
)
def test_cannot_switch_from_ray(set_new_runner_command):
    """Test that a runner cannot be switched from Ray"""

    script = f"""
import daft
daft.context.set_runner_ray()
{set_new_runner_command}
"""
    with with_null_env():
        result = subprocess.run([sys.executable, "-c", script], capture_output=True)
        assert result.stderr.decode().strip().endswith("RuntimeError: Cannot set runner more than once")


@pytest.mark.parametrize("daft_runner_envvar", ["py", "ray", "native"])
def test_env_var(daft_runner_envvar):
    """Test that environment variables are correctly picked up"""

    autodetect_script = """
import daft
df = daft.from_pydict({"foo": [1, 2, 3]})
print(daft.context.get_context()._runner.name)
    """

    with with_null_env():
        result = subprocess.run(
            [sys.executable, "-c", autodetect_script], capture_output=True, env={"DAFT_RUNNER": daft_runner_envvar}
        )
        assert result.stdout.decode().strip() == daft_runner_envvar


def test_in_ray_job():
    """Test that Ray job ID environment variable is being picked up"""

    autodetect_script = """
import daft
df = daft.from_pydict({"foo": [1, 2, 3]})
print(daft.context.get_context()._runner.name)
    """

    with with_null_env():
        result = subprocess.run(
            [sys.executable, "-c", autodetect_script], capture_output=True, env={"RAY_JOB_ID": "dummy"}
        )
        assert result.stdout.decode().strip() == "ray"


# TODO: Figure out why these tests are failing for Py3.8
# def test_in_ray_worker():
#     """Test that running Daft in a Ray worker defaults to Python runner"""

#     autodetect_script = """
# import ray
# import os
# import daft

# @ray.remote
# def init_and_return_daft_runner():

#     # Attempt to confuse our runner inference code
#     assert ray.is_initialized()
#     os.environ["RAY_JOB_ID"] = "dummy"

#     df = daft.from_pydict({"foo": [1, 2, 3]})
#     return daft.context.get_context()._runner.name

# ray.init()
# print(ray.get(init_and_return_daft_runner.remote()))
#     """

#     with with_null_env():
#         result = subprocess.run([sys.executable, "-c", autodetect_script], capture_output=True)
#         assert result.stdout.decode().strip() in {"py", "native"}


# def test_in_ray_worker_launch_query():
#     """Test that running Daft in a Ray worker defaults to Python runner"""

#     autodetect_script = """
# import ray
# import os
# import daft

# @ray.remote
# def init_and_return_daft_runner():

#     assert ray.is_initialized()
#     daft.context.set_runner_ray()

#     df = daft.from_pydict({"foo": [1, 2, 3]})
#     return daft.context.get_context()._runner.name

# ray.init()
# print(ray.get(init_and_return_daft_runner.remote()))
#     """

#     with with_null_env():
#         result = subprocess.run([sys.executable, "-c", autodetect_script], capture_output=True)
#         assert result.stdout.decode().strip() == "ray"


def test_cannot_set_runner_ray_after_py():
    cannot_set_runner_ray_after_py_script = """
import daft
df = daft.from_pydict({"foo": [1, 2, 3]})
print(daft.context.get_context()._runner.name)
daft.context.set_runner_ray()
    """

    with with_null_env():
        result = subprocess.run([sys.executable, "-c", cannot_set_runner_ray_after_py_script], capture_output=True)
        assert result.stdout.decode().strip() in {"py", "native"}
        assert "RuntimeError: Cannot set runner more than once" in result.stderr.decode().strip()
