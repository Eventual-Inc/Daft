from __future__ import annotations

import contextlib
import os

import pytest

import daft


@contextlib.contextmanager
def override_merge_scan_tasks_configs(merge_scan_tasks_min_size_bytes: int, merge_scan_tasks_max_size_bytes: int):
    old_context = daft.context.pop_context()

    try:
        daft.context.set_config(
            merge_scan_tasks_min_size_bytes=merge_scan_tasks_min_size_bytes,
            merge_scan_tasks_max_size_bytes=merge_scan_tasks_max_size_bytes,
        )
        yield
    finally:
        daft.context.set_context(old_context)


@pytest.fixture(scope="function")
def csv_files(tmpdir):
    """Writes 3 CSV files, each of 10 bytes in size, to tmpdir and yield tmpdir"""

    for i in range(3):
        path = tmpdir / f"file.{i}.csv"
        path.write_text("a,b,c\n1,2,", "utf8")  # 10 bytes

    return tmpdir


@pytest.mark.skipif(os.getenv("DAFT_MICROPARTITIONS", "1") == "0", reason="Test can only run on micropartitions")
def test_merge_scan_task_exceed_max(csv_files):
    with override_merge_scan_tasks_configs(0, 0):
        df = daft.read_csv(str(csv_files))
        assert (
            df.num_partitions() == 3
        ), "Should have 3 partitions since all merges are more than the maximum (>0 bytes)"


@pytest.mark.skipif(os.getenv("DAFT_MICROPARTITIONS", "1") == "0", reason="Test can only run on micropartitions")
def test_merge_scan_task_below_max(csv_files):
    with override_merge_scan_tasks_configs(1, 20):
        df = daft.read_csv(str(csv_files))
        assert (
            df.num_partitions() == 2
        ), "Should have 2 partitions [(CSV1, CSV2), (CSV3)] since the second merge is too large (>20 bytes)"


@pytest.mark.skipif(os.getenv("DAFT_MICROPARTITIONS", "1") == "0", reason="Test can only run on micropartitions")
def test_merge_scan_task_above_min(csv_files):
    with override_merge_scan_tasks_configs(0, 40):
        df = daft.read_csv(str(csv_files))
        assert (
            df.num_partitions() == 2
        ), "Should have 2 partitions [(CSV1, CSV2), (CSV3)] since the first merge is above the minimum (>0 bytes)"


@pytest.mark.skipif(os.getenv("DAFT_MICROPARTITIONS", "1") == "0", reason="Test can only run on micropartitions")
def test_merge_scan_task_below_min(csv_files):
    with override_merge_scan_tasks_configs(35, 40):
        df = daft.read_csv(str(csv_files))
        assert (
            df.num_partitions() == 1
        ), "Should have 1 partition [(CSV1, CSV2, CSV3)] since both merges are below the minimum and maximum"
