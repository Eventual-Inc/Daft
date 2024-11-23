import pathlib

import pyarrow as pa
import pyarrow.parquet as papq

from daft.table.micropartition import MicroPartition


def get_scantask_estimated_size(pq_path: pathlib.Path, columns: list[str] | None = None) -> int:
    """Retrieve the estimated size for reading a given Parquet file"""
    # HACK: use MicroPartition.read_parquet as a way to do this, but perhaps can expose
    # a better API from Rust here. This should be an unloaded MicroPartition.
    mp = MicroPartition.read_parquet(str(pq_path), columns=columns)
    return mp.size_bytes()


def get_actual_size(pq_path: pathlib.Path, columns: list[str] | None = None) -> int:
    # Force materializationm of the MicroPartition using `.slice`
    return MicroPartition.read_parquet(str(pq_path), columns=columns).slice(0, 100).size_bytes()


def assert_close(expected: int, actual: int, pct: float = 0.05):
    assert abs(actual - expected) / expected < pct, f"Expected {expected} to be within {pct} of: {actual}"


def test_estimations_strings(tmpdir):
    pq_path = tmpdir / "strings.pq"

    tbl = pa.table({"foo": ["a" * 100 for _ in range(100)]})
    papq.write_table(tbl, pq_path)
    assert assert_close(get_scantask_estimated_size(pq_path), get_actual_size(pq_path))
