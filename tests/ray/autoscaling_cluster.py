# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Borrowed and modified from [`ray`](https://github.com/ray-project/ray/blob/master/python/ray/cluster_utils.py).
from __future__ import annotations

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
        example_yaml_path = os.path.join(os.path.dirname(__file__), "fake_cluster.yaml")
        with open(example_yaml_path) as f:
            base_config = yaml.safe_load(f)
        custom_config = copy.deepcopy(base_config)
        custom_config["available_node_types"] = worker_node_types
        custom_config["available_node_types"]["ray.head.default"] = {
            "resources": head_resources,
            "node_config": {},
            "max_workers": 0,
        }

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
            f"--autoscaling-config={fake_config}",
            "--head",
        ]
        if "CPU" in self._head_resources:
            cmd.append("--num-cpus={}".format(self._head_resources.pop("CPU")))
        if "GPU" in self._head_resources:
            cmd.append("--num-gpus={}".format(self._head_resources.pop("GPU")))
        if "object_store_memory" in self._head_resources:
            cmd.append("--object-store-memory={}".format(self._head_resources.pop("object_store_memory")))
        if self._head_resources:
            cmd.append(f"--resources='{json.dumps(self._head_resources)}'")
        if _system_config is not None:
            cmd.append("--system-config={}".format(json.dumps(_system_config, separators=(",", ":"))))
        env = os.environ.copy()
        env.update({"AUTOSCALER_UPDATE_INTERVAL_S": "1", "RAY_FAKE_CLUSTER": "1"})
        if self._autoscaler_v2:
            # Set the necessary environment variables for autoscaler v2.
            env.update(
                {
                    "RAY_enable_autoscaler_v2": "1",
                    "RAY_CLOUD_INSTANCE_ID": "fake_head_node_id",
                    "RAY_OVERRIDE_NODE_ID_FOR_TESTING": "fake_head_node_id",
                }
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
        from daft.daft import reset_runner

        reset_runner()

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

        reset_runner()
