from __future__ import annotations

import pytest

import daft
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray", reason="Merge scan tasks are only supported on the Ray runner"
)

ASSET_S3 = "s3://daft-public-data/test_fixtures/json-dev/sampled-tpch.jsonl"


@pytest.mark.integration()
def test_jsonl_split_remote_equals_unsplit(aws_public_s3_config):
    df_unsplit = daft.read_json(ASSET_S3, io_config=aws_public_s3_config)
    assert df_unsplit.num_partitions() == 1

    with daft.context.execution_config_ctx(
        enable_scan_task_split_and_merge=True, scan_tasks_min_size_bytes=1, scan_tasks_max_size_bytes=64
    ):
        df_split = daft.read_json(ASSET_S3, io_config=aws_public_s3_config)
        assert df_split.num_partitions() == 100

    assert df_unsplit.count_rows() == df_split.count_rows()
