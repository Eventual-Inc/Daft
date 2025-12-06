from __future__ import annotations

import pytest

ray = pytest.importorskip("ray")
RAY_VERSION = getattr(ray, "__version__", "0.0.0")
RAY_VERSION_TUPLE = tuple(map(int, RAY_VERSION.split(".")[:3]))

import daft


@pytest.mark.skipif(
    RAY_VERSION_TUPLE < (2, 49, 0), reason="Ray version must be >= 2.49.0 for label selector functionality"
)
@pytest.mark.skip(reason="multi group will modify current ray cluster state, so only can run this ut local")
def test_label_selector_with_multi_group_cluster(multi_group_cluster):
    """Test label selector functionality with multi-group cluster setup."""
    # Get cluster information
    cluster_info = multi_group_cluster
    num_groups = cluster_info["num_groups"]
    workers_per_group = cluster_info["workers_per_group"]

    # Verify cluster setup
    assert num_groups == 2
    assert workers_per_group == 2

    # Test with label selectors
    @daft.udf(
        return_dtype=daft.DataType.string(), ray_options={"label_selector": {"group": "0"}}, concurrency=2, num_cpus=0.5
    )
    class Group0UDF:
        def __init__(self):
            pass

        def __call__(self, data):
            return [f"group0_processed_{item}" for item in data.to_pylist()]

    @daft.udf(
        return_dtype=daft.DataType.string(), ray_options={"label_selector": {"group": "1"}}, concurrency=2, num_cpus=0.5
    )
    class Group1UDF:
        def __init__(self):
            pass

        def __call__(self, data):
            return [f"group1_processed_{item}" for item in data.to_pylist()]

    # Connect to the multi-group cluster created by the fixture
    cluster_address = cluster_info["cluster"].address
    daft.set_runner_ray(address=cluster_address, noop_if_initialized=True)
    df = daft.from_pydict({"data": ["a", "b", "c"]})

    df = df.with_column("p_group0", Group0UDF(df["data"]))
    result_df = df.with_column("p_group1", Group1UDF(df["data"]))

    result = result_df.to_pydict()

    # Verify the results
    expected_group0 = ["group0_processed_a", "group0_processed_b", "group0_processed_c"]
    expected_group1 = ["group1_processed_a", "group1_processed_b", "group1_processed_c"]

    assert result["p_group0"] == expected_group0
    assert result["p_group1"] == expected_group1
