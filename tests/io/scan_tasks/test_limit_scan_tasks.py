import pyarrow as pa
import pyarrow.parquet as papq

import daft


def test_split_parquet_reads_with_limit(tmpdir):
    with daft.execution_config_ctx(enable_aggressive_scantask_splitting=True):
        # Write 20 large files into tmpdir
        large_file_paths = []
        for i in range(20):
            tbl = pa.table({"data": [str(f"large{i}") for i in range(100)]})
            path = tmpdir / f"file.{i}.large.pq"
            papq.write_table(tbl, str(path), row_group_size=10, use_dictionary=False)
            large_file_paths.append(str(path))

        # When reading the large files, we perform a split and can prune ScanTasks using metadata
        with daft.execution_config_ctx(
            scan_tasks_min_size_bytes=20,
            scan_tasks_max_size_bytes=100,
        ):
            df = daft.read_parquet(large_file_paths)
            df = df.limit(8)
            assert (
                df.num_partitions() == 1
            ), "Limit pruning coupled with ScanTask splitting should return just 1 partition"

        # Write 20 small files into tmpdir
        small_file_paths = []
        for i in range(20):
            tbl = pa.table({"data": ["small"]})
            path = tmpdir / f"file.{i}.small.pq"
            papq.write_table(tbl, str(path), row_group_size=1, use_dictionary=False)
            small_file_paths.append(str(path))

        # When reading the small files, we do not perform splits and thus cannot prune ScanTasks
        with daft.execution_config_ctx(
            scan_tasks_min_size_bytes=20,
            scan_tasks_max_size_bytes=100,
        ):
            df = daft.read_parquet(small_file_paths)
            df = df.limit(8)
            assert (
                df.num_partitions() == 2
            ), "ScanTasks merged into 2, but no limit pruning applied because no parquet metadata was retrieved"
