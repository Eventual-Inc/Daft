from __future__ import annotations

import time

import pytest

import daft
from daft.expressions import col
from daft.internal.gpu import cuda_visible_devices
from tests.conftest import get_tests_daft_runner_name
from tests.ray.autoscaling_cluster import autoscaling_cluster_context

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="Autoscaling tests require Ray runner to be in use",
)


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

        @daft.udf(return_dtype=daft.DataType.list(daft.DataType.string()))
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
        @daft.udf(return_dtype=daft.DataType.list(daft.DataType.string()), num_gpus=2)
        def fake_gpu_udf(x):
            visible_devices = cuda_visible_devices()
            return [visible_devices] * len(x)

        df = daft.from_pydict({"x": [1, 2, 3, 4, 5], "y": [6, 7, 8, 9, 10]})
        result = df.select(fake_gpu_udf(col("x"))).to_pydict()
        assert result["x"] == [["0", "1"]] * 5
