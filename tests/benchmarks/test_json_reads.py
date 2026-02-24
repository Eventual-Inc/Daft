from __future__ import annotations

import json
import random
import string
import tempfile
from pathlib import Path

import pytest

import daft

NUM_ROWS = 1_000_000
SEED = 42


def _rand_string(rng: random.Random, min_len: int = 1, max_len: int = 31) -> str:
    length = rng.randint(min_len, max_len)
    return "".join(rng.choices(string.ascii_letters + string.digits, k=length))


def _write_jsonl(path: Path, rows: list[dict]) -> Path:
    with open(path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    return path


def _write_json_array(path: Path, rows: list[dict]) -> Path:
    with open(path, "w") as f:
        json.dump(rows, f)
    return path


# ---------------------------------------------------------------------------
# Data generators — each returns a path to a temp file.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def json_data_dir():
    """Session-scoped temp directory so files are generated once and reused."""
    with tempfile.TemporaryDirectory(prefix="daft_json_bench_") as tmpdir:
        yield Path(tmpdir)


@pytest.fixture(scope="session")
def single_col_int64(json_data_dir):
    rng = random.Random(SEED)
    rows = [{"col": rng.randint(-(2**62), 2**62)} for _ in range(NUM_ROWS)]
    return _write_jsonl(json_data_dir / "single_int64.jsonl", rows)


@pytest.fixture(scope="session")
def single_col_float64(json_data_dir):
    rng = random.Random(SEED + 1)
    rows = [{"col": rng.uniform(-500.0, 500.0)} for _ in range(NUM_ROWS)]
    return _write_jsonl(json_data_dir / "single_float64.jsonl", rows)


@pytest.fixture(scope="session")
def single_col_string(json_data_dir):
    rng = random.Random(SEED + 2)
    rows = [{"col": _rand_string(rng)} for _ in range(NUM_ROWS)]
    return _write_jsonl(json_data_dir / "single_string.jsonl", rows)


@pytest.fixture(scope="session")
def single_col_boolean(json_data_dir):
    rng = random.Random(SEED + 3)
    rows = [{"col": rng.choice([True, False])} for _ in range(NUM_ROWS)]
    return _write_jsonl(json_data_dir / "single_boolean.jsonl", rows)


@pytest.fixture(scope="session")
def wide_table_jsonl(json_data_dir):
    """20 columns: 10 ints, 5 floats, 5 strings."""
    rng = random.Random(SEED + 4)
    rows = []
    for _ in range(NUM_ROWS):
        row = {}
        for i in range(10):
            row[f"int_{i}"] = rng.randint(-(2**62), 2**62)
        for i in range(5):
            row[f"float_{i}"] = rng.uniform(-500.0, 500.0)
        for i in range(5):
            row[f"str_{i}"] = _rand_string(rng)
        rows.append(row)
    return _write_jsonl(json_data_dir / "wide_20cols.jsonl", rows)


@pytest.fixture(scope="session")
def nested_list_of_int(json_data_dir):
    rng = random.Random(SEED + 5)
    rows = [{"col": [rng.randint(-(2**62), 2**62) for _ in range(rng.randint(1, 9))]} for _ in range(NUM_ROWS)]
    return _write_jsonl(json_data_dir / "nested_list_int.jsonl", rows)


@pytest.fixture(scope="session")
def nested_struct(json_data_dir):
    rng = random.Random(SEED + 6)
    rows = [{"col": {"a": rng.randint(-(2**62), 2**62), "b": _rand_string(rng)}} for _ in range(NUM_ROWS)]
    return _write_jsonl(json_data_dir / "nested_struct.jsonl", rows)


@pytest.fixture(scope="session")
def high_nulls_jsonl(json_data_dir):
    """~50% null density across three columns."""
    rng = random.Random(SEED + 7)
    rows = []
    for _ in range(NUM_ROWS):
        row = {
            "int_col": rng.randint(-(2**62), 2**62) if rng.random() > 0.5 else None,
            "str_col": _rand_string(rng) if rng.random() > 0.5 else None,
            "float_col": rng.uniform(-500.0, 500.0) if rng.random() > 0.5 else None,
        }
        rows.append(row)
    return _write_jsonl(json_data_dir / "high_nulls.jsonl", rows)


@pytest.fixture(scope="session")
def json_array_format(json_data_dir):
    """Same data layout as wide table but in JSON array format [...]."""
    rng = random.Random(SEED + 8)
    rows = []
    for _ in range(NUM_ROWS):
        row = {
            "int_col": rng.randint(-(2**62), 2**62),
            "str_col": _rand_string(rng),
            "float_col": rng.uniform(-500.0, 500.0),
            "bool_col": rng.choice([True, False]),
        }
        rows.append(row)
    return _write_json_array(json_data_dir / "array_format.json", rows)


# ---------------------------------------------------------------------------
# Benchmarks — core type reads
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="json_read")
def test_read_jsonl_int64(single_col_int64, benchmark):
    def bench():
        return daft.read_json(str(single_col_int64)).collect()

    result = benchmark(bench)
    assert len(result) == NUM_ROWS


@pytest.mark.benchmark(group="json_read")
def test_read_jsonl_float64(single_col_float64, benchmark):
    def bench():
        return daft.read_json(str(single_col_float64)).collect()

    result = benchmark(bench)
    assert len(result) == NUM_ROWS


@pytest.mark.benchmark(group="json_read")
def test_read_jsonl_string(single_col_string, benchmark):
    def bench():
        return daft.read_json(str(single_col_string)).collect()

    result = benchmark(bench)
    assert len(result) == NUM_ROWS


@pytest.mark.benchmark(group="json_read")
def test_read_jsonl_boolean(single_col_boolean, benchmark):
    def bench():
        return daft.read_json(str(single_col_boolean)).collect()

    result = benchmark(bench)
    assert len(result) == NUM_ROWS


# ---------------------------------------------------------------------------
# Benchmarks — multi-column and complex types
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="json_read")
def test_read_jsonl_wide_table(wide_table_jsonl, benchmark):
    def bench():
        return daft.read_json(str(wide_table_jsonl)).collect()

    result = benchmark(bench)
    assert len(result) == NUM_ROWS


@pytest.mark.benchmark(group="json_read")
def test_read_jsonl_nested_list(nested_list_of_int, benchmark):
    def bench():
        return daft.read_json(str(nested_list_of_int)).collect()

    result = benchmark(bench)
    assert len(result) == NUM_ROWS


@pytest.mark.benchmark(group="json_read")
def test_read_jsonl_nested_struct(nested_struct, benchmark):
    def bench():
        return daft.read_json(str(nested_struct)).collect()

    result = benchmark(bench)
    assert len(result) == NUM_ROWS


# ---------------------------------------------------------------------------
# Benchmarks — edge cases
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="json_read")
def test_read_jsonl_high_nulls(high_nulls_jsonl, benchmark):
    def bench():
        return daft.read_json(str(high_nulls_jsonl)).collect()

    result = benchmark(bench)
    assert len(result) == NUM_ROWS


@pytest.mark.benchmark(group="json_read")
def test_read_json_array_format(json_array_format, benchmark):
    def bench():
        return daft.read_json(str(json_array_format)).collect()

    result = benchmark(bench)
    assert len(result) == NUM_ROWS


# ---------------------------------------------------------------------------
# Benchmarks — read options (projection, limit, explicit schema)
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="json_read_options")
def test_read_jsonl_with_column_projection(wide_table_jsonl, benchmark):
    """Read only 2 of 20 columns from the wide table."""

    def bench():
        return daft.read_json(str(wide_table_jsonl)).select("int_0", "str_0").collect()

    result = benchmark(bench)
    assert len(result) == NUM_ROWS
    assert result.column_names == ["int_0", "str_0"]


@pytest.mark.benchmark(group="json_read_options")
def test_read_jsonl_with_limit(wide_table_jsonl, benchmark):
    """Read only the first 1000 rows."""

    def bench():
        return daft.read_json(str(wide_table_jsonl)).limit(1000).collect()

    result = benchmark(bench)
    assert len(result) == 1000


@pytest.mark.benchmark(group="json_read_options")
def test_read_jsonl_with_explicit_schema(wide_table_jsonl, benchmark):
    """Provide an explicit schema to skip inference — compare against test_read_jsonl_wide_table."""
    schema = {}
    for i in range(10):
        schema[f"int_{i}"] = daft.DataType.int64()
    for i in range(5):
        schema[f"float_{i}"] = daft.DataType.float64()
    for i in range(5):
        schema[f"str_{i}"] = daft.DataType.string()

    def bench():
        return daft.read_json(str(wide_table_jsonl), schema=schema).collect()

    result = benchmark(bench)
    assert len(result) == NUM_ROWS
