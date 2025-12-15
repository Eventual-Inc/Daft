from __future__ import annotations

import gzip

import pytest

import daft
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray", reason="Merge scan tasks are only supported on the Ray runner"
)


def test_jsonl_split_local_equals_unsplit(tmp_path):
    # Create a test JSONL file in tmp_path - simplified format with more rows
    jsonl_path = tmp_path / "test-split.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as f:
        for i in range(10000):
            f.write(f'{{"id": {i}, "value": {i * 2}, "name": "item_{i}"}}\n')

    df_unsplit = daft.read_json(str(jsonl_path))
    assert df_unsplit.num_partitions() == 1

    with daft.context.execution_config_ctx(
        enable_scan_task_split_and_merge=True, scan_tasks_min_size_bytes=1, scan_tasks_max_size_bytes=128 * 1024
    ):
        df_split = daft.read_json(str(jsonl_path))
        assert df_split.num_partitions() == 4

    assert df_unsplit.count_rows() == df_split.count_rows() == 10000


def test_local_multiple_jsonl_files_partitions(tmp_path):
    # Build multiple JSONL files and ensure multiple partitions
    paths = []
    for k in range(5):
        p = tmp_path / f"part-{k}.jsonl"
        with p.open("w", encoding="utf-8") as f:
            for i in range(10000):
                f.write(f'{{"id": {i}, "value": {i * 2}, "name": "item_{i}"}}\n')
        paths.append(str(p))

    with daft.context.execution_config_ctx(enable_scan_task_split_and_merge=True):
        # Note: merge-by-size requires estimable in-memory task sizes (estimate_in_memory_size_bytes).
        # For temporary JSONL files constructed in tests this results in ensemble merging.
        # With default config (96MB min, 384MB max), 5 ~1.9KB files get merged into 1 partition.
        df = daft.read_json(paths)
        # Each file is ~1.9KB, so with default 96MB/384MB thresholds, they get merged into 1 partition
        assert df.num_partitions() == 1

    # disable merge-by-size
    with daft.context.execution_config_ctx(enable_scan_task_split_and_merge=False):
        df = daft.read_json(paths)
        assert df.num_partitions() == 5

    with daft.context.execution_config_ctx(enable_scan_task_split_and_merge=True, max_sources_per_scan_task=1):
        df2 = daft.read_json(paths)
        assert df2.num_partitions() == len(paths) == 5

    with daft.context.execution_config_ctx(enable_scan_task_split_and_merge=True, max_sources_per_scan_task=3):
        df3 = daft.read_json(paths)
        assert df3.num_partitions() == 2

    with daft.context.execution_config_ctx(
        enable_scan_task_split_and_merge=True, scan_tasks_min_size_bytes=1, scan_tasks_max_size_bytes=128 * 1024
    ):
        df4 = daft.read_json(paths)
        assert df4.num_partitions() == 20 > len(paths)

    with daft.context.execution_config_ctx(
        enable_scan_task_split_and_merge=True,
        scan_tasks_min_size_bytes=1,
        scan_tasks_max_size_bytes=128 * 1024,
        max_sources_per_scan_task=1,
    ):
        df5 = daft.read_json(paths)
        assert df5.num_partitions() == 20 > len(paths)

    assert df.count_rows() == df2.count_rows() == df3.count_rows() == df4.count_rows() == df5.count_rows() == 50000


def test_json_single_not_split(tmp_path):
    # Create a test single-line JSON file in tmp_path - simplified
    json_array_path = tmp_path / "test-array.json"
    with json_array_path.open("w", encoding="utf-8") as f:
        f.write('[{"x": 42, "y": "apple"}, {"x": 17, "y": "banana"}]')

    # Single-line JSON (JSON array format) should not be split
    df = daft.read_json(str(json_array_path))
    assert df.num_partitions() == 1

    with daft.context.execution_config_ctx(
        enable_scan_task_split_and_merge=True, scan_tasks_min_size_bytes=1, scan_tasks_max_size_bytes=4
    ):
        df2 = daft.read_json(str(json_array_path))
        assert df2.num_partitions() == 1

    assert df.count_rows() == df2.count_rows() == 2


def test_jsonl_gzip_not_split(tmp_path):
    # Construct a large compressed JSONL and assert it is not split
    p = tmp_path / "large.jsonl.gz"
    with gzip.open(p, "wt", encoding="utf-8") as f:
        for i in range(20000):
            f.write(f'{{"id": {i}, "value": {i * 2}, "name": "item_{i}"}}\n')

    df = daft.read_json(str(p))
    assert df.num_partitions() == 1

    with daft.context.execution_config_ctx(
        enable_scan_task_split_and_merge=True, scan_tasks_min_size_bytes=1, scan_tasks_max_size_bytes=128
    ):
        df2 = daft.read_json(str(p))
        assert df2.num_partitions() == 1

    assert df.count_rows() == df2.count_rows() == 20000
