from __future__ import annotations

import platform
import subprocess
import sys
import time

import pytest
import ray._private.ray_constants as ray_constants

from daft import DataType, col, get_or_infer_runner_type, udf

ray = pytest.importorskip("ray")
RAY_VERSION = getattr(ray, "__version__", "0.0.0")
RAY_VERSION_TUPLE = tuple(map(int, RAY_VERSION.split(".")[:3]))

import daft


@pytest.fixture
def input_df():
    return daft.range(start=0, end=1024, partitions=100).with_column(
        "name", col("id").apply(func=lambda x: f"user_{x}", return_dtype=DataType.string())
    )


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
    @udf(
        return_dtype=daft.DataType.string(), ray_options={"label_selector": {"group": "0"}}, concurrency=2, num_cpus=0.5
    )
    class Group0UDF:
        def __init__(self):
            pass

        def __call__(self, data):
            return [f"group0_processed_{item}" for item in data.to_pylist()]

    @udf(
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


@udf(return_dtype=daft.DataType.string(), num_cpus=0.1, num_gpus=0)
def gen_email(names):
    from faker import Faker

    fake = Faker()
    return [fake.email(domain="daft.ai") for _ in names]


@pytest.mark.skipif(
    get_or_infer_runner_type() == "native" or sys.version_info[:2] not in ray_constants.RUNTIME_ENV_CONDA_PY_VERSIONS,
    reason=f"Native runner or Python version is {sys.version_info}",
)
def test_udf_with_conda_inject_dependencies(input_df):
    # missing required modules
    with pytest.raises(Exception) as exc_info:
        gen_email_udf = gen_email.override_options(
            ray_options={
                "runtime_env": {
                    "conda": {
                        "name": "test",
                        "dependencies": [
                            "pip",
                            {"pip": ["daft"]},
                        ],
                    }
                }
            }
        ).with_concurrency(1)
        input_df.with_column("email", gen_email_udf(col("name"))).collect()

    value = str(exc_info.value)
    assert "No module named 'faker'" in value, f"Unexpected UDF Exception: {value}"

    # install required modules at runtime
    gen_email_udf = gen_email.override_options(
        ray_options={
            "runtime_env": {
                "conda": {
                    "dependencies": [
                        "pip",
                        {"pip": ["daft", "faker"]},
                    ],
                }
            }
        }
    ).with_concurrency(1)
    df = input_df.with_column("email", gen_email_udf(col("name"))).select("email")
    assert 1024 == df.count_rows()


@pytest.mark.skipif(
    get_or_infer_runner_type() == "native" or sys.version_info[:2] not in ray_constants.RUNTIME_ENV_CONDA_PY_VERSIONS,
    reason=f"Native runner or Python version is {sys.version_info}",
)
def test_udf_with_prepared_conda_env(input_df):
    conda_env_name = f"test_udf_with_prepared_conda_env_{int(time.time())}"
    try:
        prepare_conda_env_cmd = [
            "conda",
            "create",
            "-n",
            conda_env_name,
            "-y",
            f"python={platform.python_version()}",
            "pip",
            "&&",
            "conda",
            "run",
            "-n",
            conda_env_name,
            "pip",
            "install",
            f"ray=={ray.__version__}",
            "daft",
            "faker",
        ]
        subprocess.run(" ".join(prepare_conda_env_cmd), check=True, capture_output=True, shell=True)

        gen_email_udf = gen_email.override_options(
            ray_options={"runtime_env": {"conda": conda_env_name}}
        ).with_concurrency(1)
        df = input_df.with_column("email", gen_email_udf(col("name"))).select("email")
        assert 1024 == df.count_rows()
    except Exception as e:
        assert False, f"Execute UDF in prepared Conda env {conda_env_name} error: {e}"
    finally:
        try:
            remove_conda_env_cmd = ["conda", "env", "remove", "-n", conda_env_name, "-y"]
            subprocess.run(remove_conda_env_cmd, check=False, capture_output=True, shell=True)
        except Exception:
            pass
