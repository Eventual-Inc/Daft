from __future__ import annotations

import pytest

import daft


@pytest.fixture(scope="function")
def csv_files(tmpdir):
    """Writes 3 CSV files, each of 10 bytes in size, to tmpdir and yield tmpdir"""

    for i in range(3):
        path = tmpdir / f"file.{i}.csv"
        path.write_text("a,b,c\n1,2,", "utf8")  # 10 bytes

    return tmpdir


def test_merge_scan_task_exceed_max(csv_files):
    with daft.execution_config_ctx(
        scan_tasks_min_size_bytes=0,
        scan_tasks_max_size_bytes=0,
    ):
        df = daft.read_csv(str(csv_files))
        assert (
            df.num_partitions() == 3
        ), "Should have 3 partitions since all merges are more than the maximum (>0 bytes)"


def test_merge_scan_task_below_max(csv_files):
    with daft.execution_config_ctx(
        scan_tasks_min_size_bytes=11,
        scan_tasks_max_size_bytes=12,
    ):
        df = daft.read_csv(str(csv_files))
        assert (
            df.num_partitions() == 2
        ), "Should have 2 partitions [(CSV1, CSV2), (CSV3)] since the second merge is too large (>22 bytes)"


def test_merge_scan_task_above_min(csv_files):
    with daft.execution_config_ctx(
        scan_tasks_min_size_bytes=9,
        scan_tasks_max_size_bytes=20,
    ):
        df = daft.read_csv(str(csv_files))
        assert (
            df.num_partitions() == 2
        ), "Should have 2 partitions [(CSV1, CSV2), (CSV3)] since the first merge is above the minimum (>19 bytes)"


def test_merge_scan_task_below_min(csv_files):
    with daft.execution_config_ctx(
        scan_tasks_min_size_bytes=17,
        scan_tasks_max_size_bytes=20,
    ):
        df = daft.read_csv(str(csv_files))
        assert (
            df.num_partitions() == 1
        ), "Should have 1 partition [(CSV1, CSV2, CSV3)] since both merges are below the minimum and maximum"


def test_merge_scan_task_limit_override(csv_files):
    # A LIMIT operation should override merging of scan tasks, making it only merge up-to the estimated size of the limit
    #
    # With a very small CSV inflation factor, the merger will think that these CSVs provide very few rows of data and will be more aggressive
    with daft.execution_config_ctx(
        scan_tasks_min_size_bytes=17,
        scan_tasks_max_size_bytes=20,
        csv_inflation_factor=0.1,
    ):
        df = daft.read_csv(str(csv_files)).limit(1)
        assert (
            df.num_partitions() == 1
        ), "Should have 1 partitions [(CSV1, CSV2, CSV3)] since we have a limit 1 but are underestimating the size of data of the CSVs"

    # With a very large CSV inflation factor, the merger will think that these CSVs provide more rows of data and will be more conservative
    with daft.execution_config_ctx(
        scan_tasks_min_size_bytes=17,
        scan_tasks_max_size_bytes=20,
        csv_inflation_factor=2.0,
    ):
        df = daft.read_csv(str(csv_files)).limit(1)
        assert df.num_partitions() == 3, "Should have 3 partitions [(CSV1, CSV2, CSV3)] since we have a limit 1"
