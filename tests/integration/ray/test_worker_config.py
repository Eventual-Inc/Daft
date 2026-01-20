from __future__ import annotations

import pytest

import daft
from daft import WorkerConfig, col
from tests.conftest import get_tests_daft_runner_name
from tests.integration.ray.autoscaling_cluster import autoscaling_cluster_context

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        get_tests_daft_runner_name() != "ray",
        reason="Worker config tests require Ray runner to be in use",
    ),
]


def test_worker_config_homogeneous():
    """Test creating workers with homogeneous worker config."""
    head_resources = {"CPU": 0}
    worker_node_types = {
        "worker": {
            "resources": {"CPU": 8, "memory": 8 * 1024**3},
            "node_config": {},
            "min_workers": 2,
            "max_workers": 2,
        }
    }

    with autoscaling_cluster_context(head_resources, worker_node_types):
        # Configure with single worker config (homogeneous)
        worker_config = WorkerConfig(
            num_replicas=2,
            num_cpus=4.0,
            memory_bytes=4 * 1024**3,
        )
        daft.set_runner_ray(worker_config=worker_config)

        # Test basic Daft operations
        df = daft.from_pydict({"x": [1, 2, 3, 4, 5], "y": [6, 7, 8, 9, 10]})
        result = df.filter(col("x") > 3).to_pydict()
        assert result["x"] == [4, 5]
        assert result["y"] == [9, 10]


def test_worker_config_heterogeneous():
    """Test creating workers with heterogeneous worker config."""
    head_resources = {"CPU": 0}
    worker_node_types = {
        "worker": {
            "resources": {"CPU": 16, "memory": 16 * 1024**3},
            "node_config": {},
            "min_workers": 1,
            "max_workers": 1,
        }
    }

    with autoscaling_cluster_context(head_resources, worker_node_types):
        # Configure with multiple worker configs (heterogeneous)
        worker_configs = [
            WorkerConfig(num_replicas=1, num_cpus=8.0, memory_bytes=8 * 1024**3),
            WorkerConfig(num_replicas=2, num_cpus=4.0, memory_bytes=4 * 1024**3),
        ]
        daft.set_runner_ray(worker_config=worker_configs)

        # Test basic Daft operations
        df = daft.from_pydict({"x": list(range(20))})
        result = df.filter(col("x") > 10).count()
        assert result == 9


def test_worker_config_with_gpus():
    """Test creating workers with GPU configuration."""
    head_resources = {"CPU": 0, "GPU": 0}
    worker_node_types = {
        "worker": {
            "resources": {"CPU": 8, "GPU": 1, "memory": 8 * 1024**3},
            "node_config": {},
            "min_workers": 1,
            "max_workers": 1,
        }
    }

    with autoscaling_cluster_context(head_resources, worker_node_types):
        # Configure with GPU workers
        worker_config = WorkerConfig(
            num_replicas=1,
            num_cpus=4.0,
            memory_bytes=4 * 1024**3,
            num_gpus=1.0,
        )
        daft.set_runner_ray(worker_config=worker_config)

        # Test basic Daft operations
        df = daft.from_pydict({"x": [1, 2, 3]})
        result = df.to_pydict()
        assert result["x"] == [1, 2, 3]


def test_worker_config_validation():
    """Test WorkerConfig validation."""
    # Valid config
    config = WorkerConfig(
        num_replicas=2,
        num_cpus=4.0,
        memory_bytes=8 * 1024**3,
        num_gpus=0.0,
    )
    assert config.num_replicas == 2
    assert config.num_cpus == 4.0
    assert config.memory_bytes == 8 * 1024**3
    assert config.num_gpus == 0.0

    # Invalid num_replicas
    with pytest.raises(ValueError, match="num_replicas must be positive"):
        WorkerConfig(num_replicas=0, num_cpus=4.0, memory_bytes=8 * 1024**3)

    # Invalid num_cpus
    with pytest.raises(ValueError, match="num_cpus must be positive"):
        WorkerConfig(num_replicas=2, num_cpus=-1.0, memory_bytes=8 * 1024**3)

    # Invalid memory_bytes
    with pytest.raises((ValueError, OverflowError)):
        WorkerConfig(num_replicas=2, num_cpus=4.0, memory_bytes=-1000)

    # Invalid num_gpus
    with pytest.raises(ValueError, match="num_gpus cannot be negative"):
        WorkerConfig(num_replicas=2, num_cpus=4.0, memory_bytes=8 * 1024**3, num_gpus=-1.0)


def test_backwards_compatibility_no_worker_config():
    """Test that not specifying worker_config maintains existing behavior."""
    head_resources = {"CPU": 0}
    worker_node_types = {
        "worker": {
            "resources": {"CPU": 4},
            "node_config": {},
            "min_workers": 1,
            "max_workers": 2,
        }
    }

    with autoscaling_cluster_context(head_resources, worker_node_types):
        # Don't specify worker_config - should use automatic node discovery
        daft.set_runner_ray()

        # Test basic Daft operations
        df = daft.from_pydict({"x": [1, 2, 3, 4, 5], "y": [6, 7, 8, 9, 10]})
        result = df.filter(col("x") > 3).to_pydict()
        assert result["x"] == [4, 5]
        assert result["y"] == [9, 10]

        # Should use automatic node discovery in automatic mode
        # (Just verify the code runs without explicit cluster configuration)


def test_worker_config_api_signature():
    """Test that set_runner_ray accepts cluster_config parameter."""
    import inspect

    sig = inspect.signature(daft.set_runner_ray)
    params = list(sig.parameters.keys())

    assert "cluster_config" in params, "cluster_config parameter should exist"
    assert sig.parameters["cluster_config"].default is None, "cluster_config should default to None"
