"""Tests for ignore_corrupt_files in read_parquet, read_csv, read_iceberg, and read_lance."""

from __future__ import annotations

import os
import urllib.parse

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft

# ── Helpers ───────────────────────────────────────────────────────────────────


def _write_parquet(directory: str, name: str, data: dict) -> str:
    path = os.path.join(directory, name)
    papq.write_table(pa.table(data), path)
    return path


def _write_corrupt_parquet(directory: str, name: str) -> str:
    """Write a file with valid Parquet magic bytes but a garbage footer."""
    path = os.path.join(directory, name)
    with open(path, "wb") as f:
        f.write(b"PAR1" + b"\x00" * 20 + b"PAR1")
    return path


def _write_csv(directory: str, name: str, content: str) -> str:
    path = os.path.join(directory, name)
    with open(path, "w") as f:
        f.write(content)
    return path


def _write_corrupt_csv(directory: str, name: str) -> str:
    """Write a file with binary garbage (not valid UTF-8)."""
    path = os.path.join(directory, name)
    with open(path, "wb") as f:
        f.write(b"\x00\x01\x02\x03\xff\xfe\xfd")
    return path


def _basename(path: str) -> str:
    """Extract the filename from a local path or a file:// URL."""
    return os.path.basename(urllib.parse.urlparse(path).path)


# ── Parquet ───────────────────────────────────────────────────────────────────


def test_parquet_ignore_corrupt_skips_and_reports(tmp_path):
    """Corrupt Parquet file is skipped, valid rows returned, and skipped_files is populated."""
    d = str(tmp_path)
    _write_parquet(d, "good1.parquet", {"a": [1, 2, 3]})
    _write_corrupt_parquet(d, "bad.parquet")
    _write_parquet(d, "good2.parquet", {"a": [4, 5, 6]})

    df = daft.read_parquet(d, ignore_corrupt_files=True)
    df.collect()

    assert sorted(df.to_pydict()["a"]) == [1, 2, 3, 4, 5, 6]

    skipped = df.skipped_files
    assert len(skipped) == 1
    path, reason = skipped[0]
    assert _basename(path) == "bad.parquet"
    assert reason


def test_parquet_ignore_corrupt_false_raises(tmp_path):
    """Without ignore_corrupt_files, a corrupt file raises an error."""
    d = str(tmp_path)
    _write_parquet(d, "good.parquet", {"a": [1, 2, 3]})
    _write_corrupt_parquet(d, "bad.parquet")

    with pytest.raises(Exception):
        daft.read_parquet(d, ignore_corrupt_files=False).collect()


def test_parquet_ignore_corrupt_all_good_no_skips(tmp_path):
    """All valid files: all rows returned, skipped_files is empty."""
    d = str(tmp_path)
    _write_parquet(d, "a.parquet", {"x": [10, 20]})
    _write_parquet(d, "b.parquet", {"x": [30, 40]})

    df = daft.read_parquet(d, ignore_corrupt_files=True)
    df.collect()

    assert sorted(df.to_pydict()["x"]) == [10, 20, 30, 40]
    assert df.skipped_files == []


def test_parquet_ignore_corrupt_schema_inference_fallback(tmp_path):
    """Schema is inferred from the first readable file when the first file (lexicographically) is corrupt."""
    d = str(tmp_path)
    _write_corrupt_parquet(d, "aaa_bad.parquet")
    _write_parquet(d, "zzz_good.parquet", {"col_a": [7, 8, 9]})

    df = daft.read_parquet(d, ignore_corrupt_files=True)
    df.collect()

    assert "col_a" in df.schema().column_names()
    assert sorted(df.to_pydict()["col_a"]) == [7, 8, 9]
    assert any(_basename(p) == "aaa_bad.parquet" for p, _ in df.skipped_files)


def test_parquet_ignore_corrupt_count_correct(tmp_path):
    """COUNT(*) returns only rows from non-corrupt files."""
    d = str(tmp_path)
    _write_parquet(d, "good.parquet", {"v": list(range(100))})
    _write_corrupt_parquet(d, "bad.parquet")

    assert daft.read_parquet(d, ignore_corrupt_files=True).count_rows() == 100


# ── CSV ───────────────────────────────────────────────────────────────────────


def test_csv_ignore_corrupt_skips_and_reports(tmp_path):
    """Corrupt CSV file is skipped, valid rows returned, and skipped_files is populated."""
    d = str(tmp_path)
    _write_csv(d, "good1.csv", "a\n1\n2\n3\n")
    _write_csv(d, "good2.csv", "a\n4\n5\n6\n")
    _write_corrupt_csv(d, "zzz_bad.csv")

    df = daft.read_csv(d, ignore_corrupt_files=True)
    df.collect()

    assert sorted(df.to_pydict()["a"]) == [1, 2, 3, 4, 5, 6]

    skipped = df.skipped_files
    assert len(skipped) == 1
    path, reason = skipped[0]
    assert _basename(path) == "zzz_bad.csv"
    assert reason


def test_csv_ignore_corrupt_false_raises(tmp_path):
    """Without ignore_corrupt_files, an unreadable CSV raises an error."""
    d = str(tmp_path)
    _write_csv(d, "good.csv", "a\n1\n2\n")
    _write_corrupt_csv(d, "bad.csv")

    with pytest.raises(Exception):
        daft.read_csv(d, ignore_corrupt_files=False).collect()


def test_csv_ignore_corrupt_all_good_no_skips(tmp_path):
    """All valid CSV files: all rows returned, skipped_files is empty."""
    d = str(tmp_path)
    _write_csv(d, "a.csv", "n\n10\n20\n")
    _write_csv(d, "b.csv", "n\n30\n40\n")

    df = daft.read_csv(d, ignore_corrupt_files=True)
    df.collect()

    assert sorted(df.to_pydict()["n"]) == [10, 20, 30, 40]
    assert df.skipped_files == []


def test_csv_ignore_corrupt_field_count_mismatch(tmp_path):
    """CSV rows with wrong field count are treated as corrupt and skipped."""
    d = str(tmp_path)
    _write_csv(d, "good.csv", "a,b\n1,2\n3,4\n")
    _write_csv(d, "zzz_bad.csv", "a,b\n1,2,EXTRA\n5,6,EXTRA\n")

    df = daft.read_csv(d, ignore_corrupt_files=True)
    df.collect()

    result = df.to_pydict()
    assert sorted(result["a"]) == [1, 3]
    assert sorted(result["b"]) == [2, 4]
    assert any(_basename(p) == "zzz_bad.csv" for p, _ in df.skipped_files)


# ── Iceberg ───────────────────────────────────────────────────────────────────
#
# These tests require pyiceberg. They are automatically skipped when the
# package is not installed (pytest.importorskip inside the fixture).
#
# Iceberg data files go through the Rust Parquet reader, so corrupt files are
# reflected in df.skipped_files just like plain read_parquet.


@pytest.fixture
def local_iceberg_catalog(tmp_path):
    SqlCatalog = pytest.importorskip("pyiceberg.catalog.sql").SqlCatalog
    catalog = SqlCatalog(
        "default",
        uri=f"sqlite:///{tmp_path}/pyiceberg_catalog.db",
        warehouse=f"file://{tmp_path}",
    )
    catalog.create_namespace("default")
    yield catalog
    catalog.engine.dispose()


def _iceberg_data_file_local_paths(table) -> list[str]:
    """Return sorted local filesystem paths of all Parquet data files in the table."""
    paths = []
    for task in table.scan().plan_files():
        url = task.file.file_path  # e.g. "file:///path/to/file.parquet"
        paths.append(urllib.parse.urlparse(url).path)
    return sorted(paths)


def test_iceberg_ignore_corrupt_skips_and_reports(local_iceberg_catalog):
    """Corrupt Iceberg data file is skipped, valid rows returned, and skipped_files is populated."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    schema = Schema(NestedField(1, "id", LongType(), required=False))
    table = local_iceberg_catalog.create_table("default.test_corrupt", schema=schema)

    table.append(pa.table({"id": pa.array([1, 2, 3], type=pa.int64())}))
    table.append(pa.table({"id": pa.array([4, 5, 6], type=pa.int64())}))

    data_files = _iceberg_data_file_local_paths(table)
    assert len(data_files) == 2, f"Expected 2 data files, got {len(data_files)}"

    with open(data_files[0], "wb") as f:
        f.write(b"PAR1" + b"\x00" * 20 + b"PAR1")

    df = daft.read_iceberg(table, ignore_corrupt_files=True)
    df.collect()

    result = sorted(df.to_pydict()["id"])
    assert len(result) == 3
    assert set(result).issubset({1, 2, 3, 4, 5, 6})

    skipped = df.skipped_files
    assert len(skipped) == 1
    _, reason = skipped[0]
    assert reason


def test_iceberg_ignore_corrupt_false_raises(local_iceberg_catalog):
    """Without ignore_corrupt_files, a corrupt Iceberg data file raises an error."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    schema = Schema(NestedField(1, "id", LongType(), required=False))
    table = local_iceberg_catalog.create_table("default.test_raises", schema=schema)
    table.append(pa.table({"id": pa.array([1, 2, 3], type=pa.int64())}))

    data_files = _iceberg_data_file_local_paths(table)
    with open(data_files[0], "wb") as f:
        f.write(b"PAR1" + b"\x00" * 20 + b"PAR1")

    with pytest.raises(Exception):
        daft.read_iceberg(table, ignore_corrupt_files=False).collect()


def test_iceberg_ignore_corrupt_all_good_no_skips(local_iceberg_catalog):
    """All valid Iceberg files: all rows returned, skipped_files is empty."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    schema = Schema(NestedField(1, "id", LongType(), required=False))
    table = local_iceberg_catalog.create_table("default.test_all_good", schema=schema)
    table.append(pa.table({"id": pa.array([1, 2, 3], type=pa.int64())}))
    table.append(pa.table({"id": pa.array([4, 5, 6], type=pa.int64())}))

    df = daft.read_iceberg(table, ignore_corrupt_files=True)
    df.collect()

    assert sorted(df.to_pydict()["id"]) == [1, 2, 3, 4, 5, 6]
    assert df.skipped_files == []


# ── Lance ─────────────────────────────────────────────────────────────────────
#
# These tests require lance. They are automatically skipped when the package
# is not installed (pytest.importorskip at the start of each test).
#
# Lance error handling runs in Python (_lancedb_table_factory_function), so
# corrupt fragments are silently skipped with a warning log but are NOT
# reflected in df.skipped_files (which is populated only by the Rust reader).


def _write_lance(path: str, data: dict, mode: str | None = None) -> None:
    lance = pytest.importorskip("lance")
    kwargs: dict = {"mode": mode} if mode else {}
    lance.write_dataset(pa.table(data), path, **kwargs)


def _get_lance_fragment_files(path: str) -> list[str]:
    """Return sorted absolute paths of all data fragment files in a Lance dataset."""
    data_dir = os.path.join(path, "data")
    if not os.path.isdir(data_dir):
        return []
    return sorted(os.path.join(data_dir, f) for f in os.listdir(data_dir) if os.path.isfile(os.path.join(data_dir, f)))


def test_lance_ignore_corrupt_skips_corrupt_fragment(tmp_path):
    """A corrupt Lance fragment is skipped; data from the valid fragment is returned."""
    pytest.importorskip("lance")
    d = str(tmp_path / "ds")

    _write_lance(d, {"id": [1, 2, 3]})
    _write_lance(d, {"id": [4, 5, 6]}, mode="append")

    fragment_files = _get_lance_fragment_files(d)
    assert len(fragment_files) >= 2, f"Expected at least 2 fragment files, got {fragment_files}"

    with open(fragment_files[0], "wb") as f:
        f.write(b"\x00" * 128)

    df = daft.read_lance(d, ignore_corrupt_files=True)
    result = sorted(df.to_pydict()["id"])

    assert len(result) == 3
    assert set(result).issubset({1, 2, 3, 4, 5, 6})


def test_lance_ignore_corrupt_false_raises(tmp_path):
    """Without ignore_corrupt_files, a corrupt Lance fragment raises an error."""
    pytest.importorskip("lance")
    d = str(tmp_path / "ds")

    _write_lance(d, {"id": [1, 2, 3]})
    _write_lance(d, {"id": [4, 5, 6]}, mode="append")

    fragment_files = _get_lance_fragment_files(d)
    with open(fragment_files[0], "wb") as f:
        f.write(b"\x00" * 128)

    with pytest.raises(Exception):
        daft.read_lance(d, ignore_corrupt_files=False).collect()


def test_lance_ignore_corrupt_all_good(tmp_path):
    """All valid Lance fragments: all rows returned normally."""
    pytest.importorskip("lance")
    d = str(tmp_path / "ds")

    _write_lance(d, {"id": [1, 2, 3]})
    _write_lance(d, {"id": [4, 5, 6]}, mode="append")

    df = daft.read_lance(d, ignore_corrupt_files=True)
    assert sorted(df.to_pydict()["id"]) == [1, 2, 3, 4, 5, 6]
