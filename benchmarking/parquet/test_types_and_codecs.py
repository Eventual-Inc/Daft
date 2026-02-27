"""Benchmarks for nested/complex types and compression codecs.

Tests decode performance for:
- List<Int64>, Struct{a: Int64, b: String}, Map<String, Int64>
- Codecs: snappy, zstd, gzip, uncompressed

Each combination generates a separate parquet file.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft

NUM_ROWS = 500_000
NUM_ROW_GROUPS = 4
# Embed params in directory name so changes force regeneration.
LOCAL_DATA_PATH = Path(__file__).parent / f"local_data/r{NUM_ROWS}_rg{NUM_ROW_GROUPS}"

CODECS = ["snappy", "zstd", "gzip", "none"]
TYPES = ["list_int64", "struct", "map"]


def _generate_list_int64_table(n: int, rng: np.random.Generator) -> pa.Table:
    offsets = np.zeros(n + 1, dtype=np.int32)
    lengths = rng.integers(1, 10, size=n)
    offsets[1:] = np.cumsum(lengths)
    total = int(offsets[-1])
    values = pa.array(rng.integers(0, 1_000_000, size=total), type=pa.int64())
    list_arr = pa.ListArray.from_arrays(offsets, values)
    return pa.table({"col": list_arr})


def _generate_struct_table(n: int, rng: np.random.Generator) -> pa.Table:
    ints = pa.array(rng.integers(0, 1_000_000, size=n), type=pa.int64())
    strs = pa.array([f"str_{i}" for i in rng.integers(0, 10_000, size=n)], type=pa.string())
    struct_arr = pa.StructArray.from_arrays([ints, strs], names=["a", "b"])
    return pa.table({"col": struct_arr})


def _generate_map_table(n: int, rng: np.random.Generator) -> pa.Table:
    offsets = np.zeros(n + 1, dtype=np.int32)
    lengths = rng.integers(1, 6, size=n)
    offsets[1:] = np.cumsum(lengths)
    total = int(offsets[-1])
    keys = pa.array([f"key_{i}" for i in rng.integers(0, 100, size=total)], type=pa.string())
    values = pa.array(rng.integers(0, 1_000_000, size=total), type=pa.int64())
    map_arr = pa.MapArray.from_arrays(offsets, keys, values)
    return pa.table({"col": map_arr})


TYPE_GENERATORS = {
    "list_int64": _generate_list_int64_table,
    "struct": _generate_struct_table,
    "map": _generate_map_table,
}


def _generate_file(type_name: str, codec: str) -> str:
    filepath = LOCAL_DATA_PATH / f"types_codecs_{type_name}_{codec}.parquet"
    if filepath.exists():
        return str(filepath)

    LOCAL_DATA_PATH.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(42)
    table = TYPE_GENERATORS[type_name](NUM_ROWS, rng)
    papq.write_table(
        table,
        str(filepath),
        row_group_size=NUM_ROWS // NUM_ROW_GROUPS,
        compression=codec,
    )
    return str(filepath)


@pytest.fixture(
    scope="session",
    params=[(t, c) for t in TYPES for c in CODECS],
    ids=[f"{t}_{c}" for t in TYPES for c in CODECS],
)
def typed_parquet_file(request):
    type_name, codec = request.param
    return _generate_file(type_name, codec), type_name, codec


def test_daft_read_typed(typed_parquet_file, benchmark):
    filepath, type_name, codec = typed_parquet_file
    benchmark.group = f"{type_name}_{codec}"
    benchmark(lambda: daft.read_parquet(filepath).collect())


def test_pyarrow_read_typed(typed_parquet_file, benchmark):
    filepath, type_name, codec = typed_parquet_file
    benchmark.group = f"{type_name}_{codec}"
    benchmark(lambda: papq.read_table(filepath))
