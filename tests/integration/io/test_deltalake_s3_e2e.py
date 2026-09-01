from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pytest

import daft

deltalake = pytest.importorskip("deltalake")

from tests.io._s3_helpers import S3_BUCKET, delta_storage_options, s3_io_config, s3_uri

pytestmark = [
    pytest.mark.skipif(os.environ.get("DAFT_RUNNER") != "ray", reason="S3 Delta end-to-end tests require Ray runner"),
    pytest.mark.skipif(not S3_BUCKET, reason="requires CHECKPOINTING_TEST_BUCKET-backed real S3 test bucket"),
]


def _read_delta_table_rows(table_uri: str) -> pa.Table:
    table = deltalake.DeltaTable(table_uri, storage_options=delta_storage_options())
    return table.to_pyarrow_table()


@pytest.mark.integration()
def test_deltalake_s3_concurrent_appends_without_dynamodb():
    table_uri = s3_uri("delta", "concurrent-conditional-put")
    io_config = s3_io_config()

    seed = daft.from_pydict({"writer_id": ["seed"], "value": [0]})
    seed.write_deltalake(table_uri, io_config=io_config)

    def append_from_worker(worker_idx: int) -> None:
        df = daft.from_pydict(
            {
                "writer_id": [f"worker-{worker_idx}"],
                "value": [worker_idx],
            }
        )
        df.write_deltalake(table_uri, io_config=io_config)

    worker_count = 4
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [executor.submit(append_from_worker, idx) for idx in range(1, worker_count + 1)]
        for future in futures:
            future.result()

    rows = _read_delta_table_rows(table_uri)
    expected = pa.table(
        {
            "writer_id": ["seed", "worker-1", "worker-2", "worker-3", "worker-4"],
            "value": [0, 1, 2, 3, 4],
        }
    ).sort_by([("writer_id", "ascending")])

    assert rows.sort_by([("writer_id", "ascending")]) == expected


@pytest.mark.integration()
def test_deltalake_s3_write_with_explicit_unsafe_rename():
    table_uri = s3_uri("delta", "unsafe-rename")
    io_config = s3_io_config()

    df = daft.from_pydict({"writer_id": ["unsafe"], "value": [1]})
    df.write_deltalake(table_uri, io_config=io_config, allow_unsafe_rename=True)

    rows = _read_delta_table_rows(table_uri)
    expected = pa.table({"writer_id": ["unsafe"], "value": [1]})

    assert rows == expected
