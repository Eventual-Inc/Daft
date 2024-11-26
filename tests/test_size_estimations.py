import pathlib

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

from daft.daft import testing as native_testing_utils
from daft.table.micropartition import MicroPartition


def get_scantask_estimated_size(pq_path: pathlib.Path, columns: list[str] | None = None) -> int:
    """Retrieve the estimated size for reading a given Parquet file"""
    return native_testing_utils.estimate_in_memory_size_bytes(str(pq_path), pq_path.stat().size, columns=columns)


def get_actual_size(pq_path: pathlib.Path, columns: list[str] | None = None) -> int:
    # Force materializationm of the MicroPartition using `.slice`
    mp = MicroPartition.read_parquet(str(pq_path), columns=columns)
    return mp.slice(0, len(mp)).size_bytes()


def assert_close(filepath: pathlib.Path, estimated: int, actual: int, pct: float = 0.05):
    assert (
        abs(actual - estimated) / estimated < pct
    ), f"Expected {filepath.stat().size / 1_000_000:.2f}MB file estimated vs actual to be within {pct * 100}%: {estimated / 1_000_000:.2f}MB vs {actual / 1000_000:.2f}MB ({((estimated - actual) / estimated) * 100:.2f}%)"


@pytest.mark.parametrize("compression", ["snappy", None])
@pytest.mark.parametrize("use_dictionary", [True, False])
@pytest.mark.parametrize("unique", [True, False])
def test_estimations_strings(tmpdir, use_dictionary, compression, unique):
    pq_path = tmpdir / f"strings.use_dictionary={use_dictionary}.unique={unique}.compression={compression}.pq"
    data = [f"{'a' * 100}{i}" for i in range(1000_000)] if unique else ["a" * 100 for _ in range(1000_000)]
    tbl = pa.table({"foo": data})
    papq.write_table(tbl, pq_path, use_dictionary=use_dictionary, compression=compression)
    assert assert_close(pq_path, get_scantask_estimated_size(pq_path), get_actual_size(pq_path))


@pytest.mark.parametrize("compression", ["snappy", None])
@pytest.mark.parametrize("use_dictionary", [True, False])
@pytest.mark.parametrize("unique", [True, False])
def test_estimations_ints(tmpdir, use_dictionary, compression, unique):
    pq_path = tmpdir / f"ints.use_dictionary={use_dictionary}.unique={unique}.compression={compression}.pq"

    data = [i for i in range(1000_000)] if unique else [1 for _ in range(1000_000)]
    tbl = pa.table({"foo": data})
    papq.write_table(tbl, pq_path, use_dictionary=use_dictionary, compression=compression)
    assert assert_close(pq_path, get_scantask_estimated_size(pq_path), get_actual_size(pq_path))
