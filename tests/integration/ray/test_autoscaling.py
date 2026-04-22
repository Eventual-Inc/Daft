from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

import pytest
import ray

import daft
from daft import col
from daft.internal.gpu import cuda_visible_devices
from tests.conftest import get_tests_daft_runner_name
from tests.integration.ray.autoscaling_cluster import autoscaling_cluster_context

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        get_tests_daft_runner_name() != "ray",
        reason="Autoscaling tests require Ray runner to be in use",
    ),
]


def _num_worker_nodes() -> int:
    return sum(1 for node in ray.nodes() if node["Alive"] and node["Resources"].get("CPU", 0) > 0)


def test_basic_autoscaling_cluster():
    """Test basic AutoscalingCluster functionality."""
    head_resources = {"CPU": 0}
    worker_node_types = {
        "worker": {
            "resources": {"CPU": 1},
            "node_config": {},
            "min_workers": 0,
            "max_workers": 1,
        }
    }

    with autoscaling_cluster_context(head_resources, worker_node_types):
        # Test basic Daft operations on the autoscaling cluster
        df = daft.from_pydict({"x": [1, 2, 3, 4, 5], "y": [6, 7, 8, 9, 10]})
        result = df.filter(col("x") > 3).to_pydict()
        assert result["x"] == [4, 5]
        assert result["y"] == [9, 10]


def test_basic_autoscaling_cluster_with_existing_workers():
    head_resources = {"CPU": 0}
    worker_node_types = {
        "worker1": {
            "resources": {"CPU": 1},
            "node_config": {},
            "min_workers": 1,
            "max_workers": 4,
        }
    }

    with autoscaling_cluster_context(head_resources, worker_node_types):

        @daft.func.batch(return_dtype=daft.DataType.list(daft.DataType.string()))
        def fake_udf_needs_2_cpus(x):
            time.sleep(1)
            return x

        # Test basic Daft operations on the autoscaling cluster
        df = daft.from_pydict({"x": [i for i in range(100)]}).repartition(50, "x")
        df.select(fake_udf_needs_2_cpus(col("x"))).to_pydict()


def test_basic_autoscaling_gpu_cluster():
    """Test basic AutoscalingCluster GPU functionality."""
    head_resources = {"CPU": 0, "GPU": 0}
    worker_node_types = {
        "worker": {
            "resources": {"CPU": 1, "GPU": 2},
            "node_config": {},
            "min_workers": 0,
            "max_workers": 1,
        }
    }

    with autoscaling_cluster_context(head_resources, worker_node_types):
        # Test basic Daft operations on the autoscaling cluster
        @daft.cls(gpus=2)
        class FakeGpuUdf:
            @daft.method.batch(return_dtype=daft.DataType.list(daft.DataType.string()))
            def __call__(self, x):
                visible_devices = cuda_visible_devices()
                return [visible_devices] * len(x)

        fake_gpu_udf = FakeGpuUdf()

        df = daft.from_pydict({"x": [1, 2, 3, 4, 5], "y": [6, 7, 8, 9, 10]})
        result = df.select(fake_gpu_udf(col("x"))).to_pydict()
        assert result["x"] == [["0", "1"]] * 5


def test_autoscaling_cluster_when_pending_tasks_exceed_cluster_cpu_cap(tmp_path):
    head_resources = {"CPU": 0}
    worker_node_types = {
        "worker": {
            "resources": {"CPU": 2},
            "node_config": {},
            "min_workers": 0,
            "max_workers": 3,
        }
    }

    for i in range(8):
        (tmp_path / f"part-{i}.csv").write_text(f"x\n{i}\n")

    with autoscaling_cluster_context(head_resources, worker_node_types):
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(lambda: daft.read_csv(str(tmp_path / "*.csv")).collect().to_pydict())

            saw_worker = False
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                worker_count = _num_worker_nodes()
                assert worker_count <= 3
                if worker_count > 0:
                    saw_worker = True
                if future.done():
                    break
                time.sleep(0.25)

            remaining_timeout = max(1, deadline - time.monotonic())
            result = future.result(timeout=remaining_timeout)
            assert saw_worker, "Expected Ray autoscaler to start at least one worker node"

    assert sorted(result["x"]) == list(range(8))
