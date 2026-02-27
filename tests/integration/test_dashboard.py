from __future__ import annotations

import contextlib
import os
import subprocess
import sys
import time

import pytest
import requests

import daft
from daft import udf


@contextlib.contextmanager
def with_null_env():
    old_daft_runner = os.getenv("DAFT_RUNNER")
    if old_daft_runner is not None:
        del os.environ["DAFT_RUNNER"]

    try:
        yield
    finally:
        if old_daft_runner is not None:
            os.environ["DAFT_RUNNER"] = old_daft_runner


@pytest.fixture(scope="module")
def dashboard_url():
    # Use a random port to avoid conflicts with default 3238 or other tests
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        random_port = s.getsockname()[1]

    # Start the dashboard server as a subprocess, simulating real user behavior
    # "daft dashboard start --port <port>"
    import os
    import shutil
    import subprocess
    import sys

    daft_bin = shutil.which("daft")
    if not daft_bin:
        # Fallback: try to find it in the same directory as python executable
        daft_bin = os.path.join(os.path.dirname(sys.executable), "daft")
        if not os.path.exists(daft_bin):
            raise RuntimeError("Could not find 'daft' executable")

    cmd = [daft_bin, "dashboard", "start", "--port", str(random_port)]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    url = f"http://127.0.0.1:{random_port}"
    os.environ["DAFT_DASHBOARD_URL"] = url
    # Ensure Rust reqwest client doesn't use proxy for localhost
    os.environ["NO_PROXY"] = "localhost,127.0.0.1"

    # Wait for server to be reachable
    import time

    start_time = time.time()
    server_ready = False

    while time.time() - start_time < 10:
        try:
            requests.get(f"{url}/api/ping", timeout=1)
            server_ready = True
            break
        except Exception:
            # Check if process exited prematurely
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                raise RuntimeError(f"Dashboard process exited prematurely. Stdout: {stdout}, Stderr: {stderr}")
            time.sleep(0.5)

    if not server_ready:
        process.kill()
        raise RuntimeError(f"Dashboard server failed to start on port {random_port}")

    # Ensure subscriber is refreshed with the new URL
    from daft.daft import refresh_dashboard_subscriber

    refresh_dashboard_subscriber()

    yield url

    # Cleanup
    process.terminate()
    process.wait()


@pytest.mark.integration
def test_dashboard_queries_api(dashboard_url):
    """Verifies that the queries API returns the expected fields and structure."""
    # 1. Run a simple query to populate dashboard data
    try:
        daft.set_runner_native()
    except Exception as e:
        if "Cannot set runner more than once" in str(e):
            pass
        else:
            raise e

    df = daft.from_pydict({"a": [1, 2, 3]})
    # Force execution by adding a filter, ensuring it goes through the Native Runner's notification path
    df = df.where(df["a"] > 1)
    df.collect()

    # 2. Fetch data from the queries API
    response = requests.get(f"{dashboard_url}/client/queries", timeout=1)
    assert response.status_code == 200
    data = response.json()

    assert isinstance(data, dict), f"Failed to get queries from {dashboard_url}"
    assert len(data) > 0, "No queries found in dashboard"

    # 3. Verify the structure of the latest query entry
    latest_query = next(iter(data.values()))

    # Required fields that must always be present
    assert "id" in latest_query
    assert "status" in latest_query
    assert "start_sec" in latest_query
    assert "runner" in latest_query
    assert "entrypoint" in latest_query

    # Optional fields (may be omitted by serde if None)
    # end_sec, ray_dashboard_url, error_message

    # Verify field types/values
    assert isinstance(latest_query["id"], str)

    # status can be a string or a dict
    status = latest_query["status"]
    if isinstance(status, dict):
        assert "status" in status
        assert status["status"] in ["Finished", "Failed", "Running"]
    else:
        assert isinstance(status, str)

    assert isinstance(latest_query["start_sec"], float)

    if "end_sec" in latest_query and latest_query["end_sec"] is not None:
        assert isinstance(latest_query["end_sec"], float)

    # The runner might be "Native (Swordfish)" or "Ray (Flotilla)" depending on execution
    assert latest_query["runner"] in ["Native (Swordfish)", "Ray (Flotilla)"]


@pytest.mark.integration
def test_dashboard_ray_flotilla(dashboard_url):
    daft.set_runner_ray(address="auto", noop_if_initialized=True)

    @udf(return_dtype=daft.DataType.int64())
    def slow_inc(x):
        time.sleep(0.1)
        return [i + 1 for i in x.to_pylist()]

    df = daft.from_pydict({"a": list(range(100))})
    df = df.repartition(5)
    df = df.with_column("b", slow_inc(df["a"]))
    df = df.repartition(2)

    result = df.collect()
    assert len(result) == 100


@pytest.mark.integration
def test_dashboard_native_swordfish(dashboard_url):
    daft.set_runner_ray(address="auto", noop_if_initialized=True)

    df = daft.from_pydict({"a": list(range(100))})
    df = df.with_column("b", df["a"] + 1)
    df = df.repartition(3)

    result = df.collect()
    assert len(result) == 100


@pytest.mark.integration
def test_enable_dashboard_for_ray_runner(dashboard_url):
    get_or_infer_runner_type_py_script = """
import daft
daft.range(start=0, end=1024, partitions=10).collect()
    """

    with with_null_env():
        result = subprocess.run(
            [sys.executable, "-c", get_or_infer_runner_type_py_script],
            capture_output=True,
            env={"DAFT_RUNNER": "ray", "DAFT_DASHBOARD_URL": dashboard_url},
        )
        assert result.returncode == 0
        assert "Dashboard isn't currently supported in Ray Runner" not in result.stderr.decode()
