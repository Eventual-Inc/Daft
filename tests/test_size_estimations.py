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
    return MicroPartition.read_parquet(str(pq_path), columns=columns).slice(0, 100).size_bytes()


def assert_close(expected: int, actual: int, pct: float = 0.05):
    assert (
        abs(actual - expected) / expected < pct
    ), f"Expected estimations {expected} to be within {pct} of actual: {actual}"


@pytest.mark.parametrize("compression", ["snappy", None])
@pytest.mark.parametrize("use_dictionary", [True, False])
@pytest.mark.parametrize("unique", [True, False])
def test_estimations_strings(tmpdir, use_dictionary, compression, unique):
    pq_path = tmpdir / "strings.pq"
    data = [f"{'a' * 100}{i}" for i in range(1000)] if unique else ["a" * 100 for _ in range(1000)]
    tbl = pa.table({"foo": data})
    papq.write_table(tbl, pq_path, use_dictionary=use_dictionary, compression=compression)
    assert assert_close(get_scantask_estimated_size(pq_path), get_actual_size(pq_path))


@pytest.mark.parametrize("compression", ["snappy", None])
@pytest.mark.parametrize("use_dictionary", [True, False])
@pytest.mark.parametrize("unique", [True, False])
def test_estimations_ints(tmpdir, use_dictionary, compression, unique):
    pq_path = tmpdir / "ints.pq"

    data = [i for i in range(1000)] if unique else [1 for _ in range(1000)]
    tbl = pa.table({"foo": data})
    papq.write_table(tbl, pq_path, use_dictionary=use_dictionary, compression=compression)
    assert assert_close(get_scantask_estimated_size(pq_path), get_actual_size(pq_path))
