from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import daft


def test_parquet_ignore_error(tmp_path):
    # Create valid parquet
    valid_path = tmp_path / "valid.parquet"
    table = pa.Table.from_pydict({"a": [1, 2, 3]})
    pq.write_table(table, valid_path)

    # Create invalid/corrupt parquet (wrong bytes but with .parquet suffix)
    bad_path = tmp_path / "bad.parquet"
    with open(bad_path, "wb") as f:
        f.write(b"not a parquet file")

    # When ignore_corrupt_files=True, only rows from valid file should be loaded
    df = daft.read_parquet(str(tmp_path), ignore_corrupt_files=True)
    res = df.collect().to_pydict()
    assert res == {"a": [1, 2, 3]}

    # When ignore_corrupt_files=False, reading should raise
    with pytest.raises(Exception):
        daft.read_parquet(str(tmp_path), ignore_corrupt_files=False).collect()
