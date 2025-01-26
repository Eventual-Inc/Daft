from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft


@pytest.fixture(scope="function")
def parquet_files(tmpdir):
    """Writes 1 Parquet file with 10 rowgroups, each of 100 bytes in size."""
    tbl = pa.table({"data": ["aaa"] * 100})
    path = tmpdir / "file.pq"
    papq.write_table(tbl, str(path), row_group_size=10, use_dictionary=False)

    return tmpdir


def test_split_parquet_read(parquet_files):
    with daft.execution_config_ctx(
        scan_tasks_min_size_bytes=1,
        scan_tasks_max_size_bytes=10,
    ):
        df = daft.read_parquet(str(parquet_files))
        assert df.num_partitions() == 10, "Should have 10 partitions since we will split the file"
        assert df.to_pydict() == {"data": ["aaa"] * 100}


@pytest.mark.skip(reason="Not implemented yet")
def test_split_parquet_read_some_splits(tmpdir):
    with daft.execution_config_ctx(scantask_splitting_level=2):
        # Write a mix of 20 large and 20 small files
        # Small ones should not be split, large ones should be split into 10 rowgroups each
        # This gives us a total of 200 + 20 scantasks

        # Write 20 large files into tmpdir
        large_file_paths = []
        for i in range(20):
            tbl = pa.table({"data": [str(f"large{i}") for i in range(100)]})
            path = tmpdir / f"file.{i}.large.pq"
            papq.write_table(tbl, str(path), row_group_size=10, use_dictionary=False)
            large_file_paths.append(str(path))

        # Write 20 small files into tmpdir
        small_file_paths = []
        for i in range(20):
            tbl = pa.table({"data": ["small"]})
            path = tmpdir / f"file.{i}.small.pq"
            papq.write_table(tbl, str(path), row_group_size=1, use_dictionary=False)
            small_file_paths.append(str(path))

        # Test [large_paths, ..., small_paths, ...]
        with daft.execution_config_ctx(
            scan_tasks_min_size_bytes=20,
            scan_tasks_max_size_bytes=100,
        ):
            df = daft.read_parquet(large_file_paths + small_file_paths)
            assert (
                df.num_partitions() == 220
            ), "Should have 220 partitions since we will split all large files (20 * 10 rowgroups) but keep small files unsplit"
            assert df.to_pydict() == {"data": [str(f"large{i}") for i in range(100)] * 20 + ["small"] * 20}

        # Test interleaved [large_path, small_path, large_path, small_path, ...]
        with daft.execution_config_ctx(
            scan_tasks_min_size_bytes=20,
            scan_tasks_max_size_bytes=100,
        ):
            interleaved_paths = [path for pair in zip(large_file_paths, small_file_paths) for path in pair]
            df = daft.read_parquet(interleaved_paths)
            assert (
                df.num_partitions() == 220
            ), "Should have 220 partitions since we will split all large files (20 * 10 rowgroups) but keep small files unsplit"
            assert df.to_pydict() == {"data": ([str(f"large{i}") for i in range(100)] + ["small"]) * 20}

        # Test [small_paths, ..., large_paths]
        with daft.execution_config_ctx(
            scan_tasks_min_size_bytes=20,
            scan_tasks_max_size_bytes=100,
        ):
            df = daft.read_parquet(small_file_paths + large_file_paths)
            assert (
                df.num_partitions() == 220
            ), "Should have 220 partitions since we will split all large files (20 * 10 rowgroups) but keep small files unsplit"
            assert df.to_pydict() == {"data": ["small"] * 20 + [str(f"large{i}") for i in range(100)] * 20}
