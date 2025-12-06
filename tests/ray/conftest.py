#!/usr/bin/env python3
"""conftest.py for Ray tests.

This file provides pytest fixtures for testing Ray functionality,
including local cluster with placement groups and multi-worker groups.
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
import time
from uuid import uuid4

import pytest

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try to import ray, with fallback to local source
try:
    import ray
    from ray import exceptions as ray_exceptions  # Import for GetTimeoutError
except ImportError:
    repo_python = os.path.join(os.path.dirname(__file__), "..", "..", "ray", "python")
    if os.path.isdir(repo_python):
        sys.path.insert(0, repo_python)
        import ray
        from ray import exceptions as ray_exceptions  # Import for GetTimeoutError
    else:
        ray = None
        ray_exceptions = None

if ray:
    from ray.cluster_utils import Cluster
    from ray.util.placement_group import placement_group, remove_placement_group


@pytest.fixture
def multi_group_cluster():
    """Create a multi-group cluster setup for testing.

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
    # Use a per-fixture tmpdir and unique namespace to avoid cross-test interference
    tmpdir = tempfile.mkdtemp()
    os.environ["RAY_TMPDIR"] = tmpdir
    namespace = f"label_selector_fixture_{uuid4().hex[:8]}"

    # Initialize head node with better port configuration to avoid conflicts
    # Use separate port ranges for different components
    cluster = Cluster(
        initialize_head=True,
        connect=False,  # avoid implicit ray.init; we'll connect explicitly with a unique namespace
        head_node_args={
            "num_cpus": 1,
            # Let Ray automatically handle ports to avoid conflicts
            # We disable dashboard to reduce potential port conflicts
            "dashboard_port": None,
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

    # Connect to the cluster with improved initialization and cleanup
    ctx = None
    try:
        # Initialize new Ray cluster with stronger parameters
        logger.info("Initializing new Ray cluster, address: %s, namespace: %s", cluster.address, namespace)
        ray_context = ray.init(
            cluster.address,
            namespace=namespace,
            ignore_reinit_error=True,
        )
        ctx = ray_context
        logger.info("Successfully initialized new Ray cluster")

        # Verify cluster is actually initialized and healthy
        logger.info("Verifying Ray cluster health")
        ray.cluster_resources()  # This will throw if cluster is not healthy
        logger.info("Ray cluster health verified")

    except Exception as e:
        logger.error("Error initializing Ray cluster: %s", e)
        # Try to clean up on failure
        try:
            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass
        raise

    # Use the Ray Client context to ensure fixture-scoped Ray API calls target the new cluster
    with ctx:
        logger.info("Using Ray cluster via context, address: %s", cluster.address)

        # Create placement groups for each group
        placement_groups = []

        # Create placement groups (without pre-allocating actors) with timeout
        for g in range(num_groups):
            # Create placement group for this group
            bundles = [{"CPU": workers_per_group, f"group_{g}": workers_per_group}]
            bundle_label_selector = [{"group": str(g)}]
            pg = placement_group(
                bundles=bundles,
                strategy="STRICT_SPREAD",
                name=f"pg-group-{g}",
                bundle_label_selector=bundle_label_selector,
            )
            # Add timeout to placement group ready call to prevent hanging
            try:
                ray.get(pg.ready(), timeout=30)
            except ray_exceptions.GetTimeoutError:
                logger.warning("Placement group %s not ready within 30 seconds, continuing anyway", pg.id)
            placement_groups.append(pg)
            logger.info(
                "Created placement group %s for group %s, bundles: %s, label_selector: %s",
                pg.id,
                g,
                bundles,
                bundle_label_selector,
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
            # Add timeout protection for the entire fixture cleanup
            cleanup_start_time = time.time()
            cleanup_timeout = 60  # 60 seconds max for entire cleanup

            # Cleanup placement groups with better error handling and timeout
            logger.info("Cleaning up %d placement groups", len(placement_groups))
            for pg in placement_groups:
                if time.time() - cleanup_start_time > cleanup_timeout:
                    logger.warning("Cleanup timeout reached, forcing immediate cleanup")
                    break

                try:
                    logger.info("Removing placement group %s", pg.id.hex())
                    remove_placement_group(pg)
                    # Wait for placement group to be removed with shorter timeout
                    timeout = 10  # Reduced from 100 to 10 seconds
                    start_time = time.time()
                    removed = False
                    while time.time() - start_time < timeout:
                        if time.time() - cleanup_start_time > cleanup_timeout:
                            logger.warning("Overall cleanup timeout reached, breaking")
                            break

                        try:
                            pg_state = ray.util.placement_group_table(pg)["state"]
                            if pg_state == "REMOVED":
                                logger.info("Placement group %s successfully removed", pg.id.hex())
                                removed = True
                                break
                        except Exception as e:
                            # If placement group is already gone, consider it removed
                            logger.info("Placement group %s may already be removed: %s", pg.id, e)
                            removed = True
                            break
                        time.sleep(0.2)  # Check more frequently

                    if not removed:
                        logger.warning(
                            "Timeout waiting for placement group %s to be removed, skipping cleanup", pg.id.hex()
                        )
                except Exception as e:
                    logger.error("Error removing placement group %s: %s", pg.id.hex(), e)
                    # Continue with cleanup of other placement groups
