from __future__ import annotations

import os
import subprocess
import sys

import pytest

from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="Autoscaling tests require Ray runner to be in use",
)

SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "fake_cluster.yaml")
RAY_SETUP_SCRIPT = f'''
import copy
import json
import os
import subprocess
import tempfile
from contextlib import contextmanager

import ray
import yaml

class AutoscalingCluster:
    """Create a local autoscaling cluster for testing."""

    def __init__(
        self,
        head_resources: dict,
        worker_node_types: dict,
        autoscaler_v2: bool = False,
        **config_kwargs,
    ):
        """Create the cluster.

        Args:
            head_resources: resources of the head node, including CPU.
            worker_node_types: autoscaler node types config for worker nodes.
        """
        self._head_resources = head_resources
        self._config = self._generate_config(
            head_resources,
            worker_node_types,
            autoscaler_v2=autoscaler_v2,
            **config_kwargs,
        )
        self._autoscaler_v2 = autoscaler_v2

    def _generate_config(self, head_resources, worker_node_types, autoscaler_v2=False, **config_kwargs):
        example_yaml_path = "{SCRIPT_PATH}"
        with open(example_yaml_path) as f:
            base_config = yaml.safe_load(f)
        custom_config = copy.deepcopy(base_config)
        custom_config["available_node_types"] = worker_node_types
        custom_config["available_node_types"]["ray.head.default"] = {{
            "resources": head_resources,
            "node_config": {{}},
            "max_workers": 0,
        }}

        # Autoscaler v2 specific configs
        if autoscaler_v2:
            custom_config["provider"]["launch_multiple"] = True
            custom_config["provider"]["head_node_id"] = "fake_head_node_id"
        custom_config.update(config_kwargs)
        return custom_config

    def start(self, _system_config=None, override_env: dict | None = None):
        """Start the cluster.

        After this call returns, you can connect to the cluster with
        ray.init("auto").
        """
        subprocess.check_call(["ray", "stop", "--force"])
        _, fake_config = tempfile.mkstemp()
        with open(fake_config, "w") as f:
            f.write(json.dumps(self._config))
        cmd = [
            "ray",
            "start",
            f"--autoscaling-config={{fake_config}}",
            "--head",
        ]
        if "CPU" in self._head_resources:
            cmd.append("--num-cpus={{}}".format(self._head_resources.pop("CPU")))
        if "GPU" in self._head_resources:
            cmd.append("--num-gpus={{}}".format(self._head_resources.pop("GPU")))
        if "object_store_memory" in self._head_resources:
            cmd.append("--object-store-memory={{}}".format(self._head_resources.pop("object_store_memory")))
        if self._head_resources:
            cmd.append(f"--resources='{{json.dumps(self._head_resources)}}'")
        if _system_config is not None:
            cmd.append("--system-config={{}}".format(json.dumps(_system_config, separators=(",", ":"))))
        env = os.environ.copy()
        env.update({{"AUTOSCALER_UPDATE_INTERVAL_S": "1", "RAY_FAKE_CLUSTER": "1"}})
        if self._autoscaler_v2:
            # Set the necessary environment variables for autoscaler v2.
            env.update(
                {{
                    "RAY_enable_autoscaler_v2": "1",
                    "RAY_CLOUD_INSTANCE_ID": "fake_head_node_id",
                    "RAY_OVERRIDE_NODE_ID_FOR_TESTING": "fake_head_node_id",
                }}
            )
        if override_env:
            env.update(override_env)
        subprocess.check_call(cmd, env=env)

    def shutdown(self):
        """Terminate the cluster."""
        subprocess.check_call(["ray", "stop", "--force"])

@contextmanager
def autoscaling_cluster_context(head_resources: dict, worker_node_types: dict, autoscaler_v2: bool = False):
    """Context manager for managing AutoscalingCluster with proper setup and teardown.

    The cluster is created before ray.init() as requested, and properly cleaned up afterward.

    Note: Starting and stopping the cluster can be time consuming. Use sparingly, and reuse the cluster if possible.
    """
    try:
        if ray.is_initialized():
            ray.shutdown()

        # Create the autoscaling cluster before ray.init()
        cluster = AutoscalingCluster(
            head_resources=head_resources,
            worker_node_types=worker_node_types,
            autoscaler_v2=autoscaler_v2,
        )

        # Start the cluster
        cluster.start()

        # Initialize Ray with the cluster
        ray.init()

        yield cluster

    finally:
        # Clean shutdown sequence
        ray.shutdown()
        cluster.shutdown()
'''


def test_basic_autoscaling_cluster():
    """Test basic AutoscalingCluster functionality."""
    script = f"""
import daft
from daft import col
{RAY_SETUP_SCRIPT}

head_resources = {{"CPU": 0}}
worker_node_types = {{
    "worker": {{
        "resources": {{"CPU": 1}},
        "node_config": {{}},
        "min_workers": 0,
        "max_workers": 1,
    }}
}}

with autoscaling_cluster_context(head_resources, worker_node_types):
    # Test basic Daft operations on the autoscaling cluster
    df = daft.from_pydict({{"x": [1, 2, 3, 4, 5], "y": [6, 7, 8, 9, 10]}})
    result = df.filter(col("x") > 3).to_pydict()
    print(result["x"])
    print(result["y"])
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
    )
    print(result.stderr.decode().strip())
    result.check_returncode()
    assert result.stdout.decode().strip().endswith("[4, 5]\n[9, 10]")


def test_basic_autoscaling_cluster_with_existing_workers():
    script = f"""
import daft
from daft import col
import time
{RAY_SETUP_SCRIPT}

head_resources = {{"CPU": 0}}
worker_node_types = {{
    "worker1": {{
        "resources": {{"CPU": 1}},
        "node_config": {{}},
        "min_workers": 1,
        "max_workers": 4,
    }}
}}

with autoscaling_cluster_context(head_resources, worker_node_types):

    @daft.udf(return_dtype=daft.DataType.list(daft.DataType.string()))
    def fake_udf_needs_2_cpus(x):
        time.sleep(1)
        return x

    # Test basic Daft operations on the autoscaling cluster
    df = daft.from_pydict({{"x": [i for i in range(100)]}}).repartition(50, "x")
    df.select(fake_udf_needs_2_cpus(col("x"))).to_pydict()
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
    )
    print(result.stderr.decode().strip())
    result.check_returncode()


def test_basic_autoscaling_gpu_cluster():
    """Test basic AutoscalingCluster GPU functionality."""
    script = f"""
import daft
from daft import col
from daft.internal.gpu import cuda_visible_devices
{RAY_SETUP_SCRIPT}

head_resources = {{"CPU": 0, "GPU": 0}}
worker_node_types = {{
    "worker": {{
        "resources": {{"CPU": 1, "GPU": 2}},
        "node_config": {{}},
        "min_workers": 0,
        "max_workers": 1,
    }}
}}

with autoscaling_cluster_context(head_resources, worker_node_types):
    # Test basic Daft operations on the autoscaling cluster
    @daft.udf(return_dtype=daft.DataType.list(daft.DataType.string()), num_gpus=2)
    def fake_gpu_udf(x):
        visible_devices = cuda_visible_devices()
        return [visible_devices] * len(x)

    df = daft.from_pydict({{"x": [1, 2, 3, 4, 5], "y": [6, 7, 8, 9, 10]}})
    result = df.select(fake_gpu_udf(col("x"))).to_pydict()
    print(result["x"])
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
    )
    result.check_returncode()
    assert result.stdout.decode().strip().endswith("[['0', '1'], ['0', '1'], ['0', '1'], ['0', '1'], ['0', '1']]")
