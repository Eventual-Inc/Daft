import contextlib
import os
import subprocess
import sys


@contextlib.contextmanager
def with_null_env():
    old_daft_runner = os.getenv("DAFT_RUNNER")
    del os.environ["DAFT_RUNNER"]

    try:
        yield
    finally:
        if old_daft_runner is not None:
            os.environ["DAFT_RUNNER"] = old_daft_runner


explicit_set_runner_script = """
import daft
print(daft.context.get_context()._runner)
daft.context.set_runner_py()
print(daft.context.get_context()._runner.name)
"""


def test_explicit_set_runner_py():
    """Test that a freshly imported context doesn't have a runner config set and can be set explicitly to Python"""
    with with_null_env():
        result = subprocess.run([sys.executable, "-c", explicit_set_runner_script], capture_output=True)
        assert result.stdout.decode().strip() == "None\npy"


implicit_set_runner_script = """
import daft
print(daft.context.get_context()._runner)
df = daft.from_pydict({"foo": [1, 2, 3]})
print(daft.context.get_context()._runner.name)
"""


def test_implicit_set_runner_py():
    """Test that a freshly imported context doesn't have a runner config set and can be set implicitly to Python"""
    with with_null_env():
        result = subprocess.run([sys.executable, "-c", implicit_set_runner_script], capture_output=True)
        assert result.stdout.decode().strip() == "None\npy" or result.stdout.decode().strip() == "None\nnative"


explicit_set_runner_script_ray = """
import daft
print(daft.context.get_context()._runner)
daft.context.set_runner_ray()
print(daft.context.get_context()._runner.name)
"""


def test_explicit_set_runner_ray():
    """Test that a freshly imported context doesn't have a runner config set and can be set explicitly to Ray"""
    with with_null_env():
        result = subprocess.run([sys.executable, "-c", explicit_set_runner_script_ray], capture_output=True)
        assert result.stdout.decode().strip() == "None\nray"


implicit_set_runner_script_ray = """
import daft
import ray
ray.init()
print(daft.context.get_context()._runner)
df = daft.from_pydict({"foo": [1, 2, 3]})
print(daft.context.get_context()._runner.name)
"""


def test_implicit_set_runner_ray():
    """Test that a freshly imported context doesn't have a runner config set and can be set implicitly to Ray"""
    with with_null_env():
        result = subprocess.run([sys.executable, "-c", implicit_set_runner_script_ray], capture_output=True)
        assert result.stdout.decode().strip() == "None\nray"
