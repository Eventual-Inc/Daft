#!/usr/bin/env python3
"""
conftest.py for Ray tests

This file provides pytest fixtures for testing Ray functionality,
including local cluster with placement groups and multi-worker groups.
"""

import logging
import os
import sys
import time
from typing import Dict

import pytest

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try to import ray, with fallback to local source
try:
    import ray
except ImportError:
    repo_python = os.path.join(os.path.dirname(__file__), "..", "..", "ray", "python")
    if os.path.isdir(repo_python):
        sys.path.insert(0, repo_python)
        import ray
    else:
        ray = None

if ray:
    from ray.cluster_utils import Cluster
    from ray.util.placement_group import placement_group, remove_placement_group
    from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


@pytest.fixture
def multi_group_cluster():
    """
    Create a multi-group cluster setup for testing.

    This fixture provides a complete setup with:
    - A Ray cluster with nodes labeled for different groups
    - Placement groups for each group

    The fixture yields the cluster resources and handles cleanup after the test.
    Note: This fixture only creates the placement groups and cluster resources,
    but does not pre-allocate any actors, leaving resources available for tests.
    """
    if not ray:
        pytest.skip("Ray not available")

    logger.info("Initializing multi-group cluster")

    # Initialize head node
    cluster = Cluster(
        initialize_head=True,
        connect=True,
        head_node_args={
            "num_cpus": 1,
            "dashboard_host": "0.0.0.0",
        },
    )

    # Number of groups and workers per group
    num_groups = 2
    workers_per_group = 2

    # Add worker nodes with custom resources for different groups
    for g in range(num_groups):
        for k in range(workers_per_group):
            cluster.add_node(
                num_cpus=num_groups,
                resources={f"group_{g}": num_groups},
                labels={"group": str(g), "index": str(k)},
            )

    cluster.wait_for_nodes()

    # Connect to the cluster
    if ray.is_initialized():
        ray.shutdown()
    ray.init(address=cluster.address)

    logger.info("Connected to Ray cluster")

    # Create placement groups for each group
    placement_groups = []

    # Create placement groups (without pre-allocating actors)
    for g in range(num_groups):
        # Create placement group for this group
        bundles = [{"CPU": workers_per_group, f"group_{g}": workers_per_group}]
        bundle_label_selector = [{"group": str(g)}]
        pg = placement_group(
            bundles=bundles, strategy="STRICT_SPREAD", name=f"pg-group-{g}", bundle_label_selector=bundle_label_selector
        )
        ray.get(pg.ready())
        placement_groups.append(pg)
        logger.info(
            f"Created placement group {pg.id} for group {g}, bundles: {bundles}, label_selector: {bundle_label_selector}"
        )

    logger.info("All placement groups created successfully")

    # Return cluster info and placement groups (no pre-allocated workers)
    cluster_info = {
        "cluster": cluster,
        "placement_groups": placement_groups,
        "num_groups": num_groups,
        "workers_per_group": workers_per_group,
    }

    try:
        yield cluster_info
    finally:
        # Cleanup placement groups
        logger.info(f"Cleaning up {len(placement_groups)} placement groups")
        for pg in placement_groups:
            try:
                logger.info(f"Removing placement group {pg.id.hex()}")
                remove_placement_group(pg)
                # Wait for placement group to be removed
                timeout = 100
                start_time = time.time()
                while time.time() - start_time < timeout:
                    pg_state = ray.util.placement_group_table(pg)["state"]
                    if pg_state == "REMOVED":
                        logger.info(f"Placement group {pg.id.hex()} successfully removed")
                        break
                    time.sleep(0.1)
                else:
                    logger.warning(f"Timeout waiting for placement group {pg.id.hex()} to be removed")
            except Exception as e:
                logger.error(f"Error removing placement group {pg.id.hex()}: {e}")

        # Shutdown ray and cluster
        try:
            logger.info("Shutting down cluster")
            cluster.shutdown()
        except Exception as e:
            logger.error(f"Error shutting down cluster: {e}")

        logger.info("Multi-group cluster cleanup completed")
