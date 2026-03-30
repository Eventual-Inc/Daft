"""Tests for ignore_corrupt_files in read_parquet and read_csv."""
from __future__ import annotations

import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_parquet(directory: str, name: str, data: dict) -> str:
    path = os.path.join(directory, name)
    papq.write_table(pa.table(data), path)
    return path


def _write_corrupt_parquet(directory: str, name: str) -> str:
    """Write a file with valid Parquet magic bytes prefix but garbage footer."""
    path = os.path.join(directory, name)
    with open(path, "wb") as f:
        f.write(b"PAR1" + b"\x00" * 20 + b"PAR1")  # bad footer between magic bytes
    return path


def _write_csv(directory: str, name: str, content: str) -> str:
    path = os.path.join(directory, name)
    with open(path, "w") as f:
        f.write(content)
    return path


def _write_corrupt_csv(directory: str, name: str) -> str:
    """Write a zero-byte file (unreadable as CSV with headers)."""
    path = os.path.join(directory, name)
    open(path, "wb").close()
    return path


# ---------------------------------------------------------------------------
# Parquet tests
# ---------------------------------------------------------------------------


def test_parquet_ignore_corrupt_files_skips_corrupt(tmp_path):
    """A corrupt Parquet file is skipped; valid files are returned."""
    d = str(tmp_path)
    _write_parquet(d, "good1.parquet", {"a": [1, 2, 3]})
    _write_corrupt_parquet(d, "bad.parquet")
    _write_parquet(d, "good2.parquet", {"a": [4, 5, 6]})

    df = daft.read_parquet(d, ignore_corrupt_files=True)
    result = sorted(df.to_pydict()["a"])
    assert result == [1, 2, 3, 4, 5, 6]


def test_parquet_ignore_corrupt_files_false_raises(tmp_path):
    """Without ignore_corrupt_files, a corrupt file raises an error."""
    d = str(tmp_path)
    _write_parquet(d, "good.parquet", {"a": [1, 2, 3]})
    _write_corrupt_parquet(d, "bad.parquet")

    with pytest.raises(Exception):
        daft.read_parquet(d, ignore_corrupt_files=False).collect()


def test_parquet_ignore_corrupt_files_all_good(tmp_path):
    """When no files are corrupt, all rows are returned normally."""
    d = str(tmp_path)
    _write_parquet(d, "a.parquet", {"x": [10, 20]})
    _write_parquet(d, "b.parquet", {"x": [30, 40]})

    result = sorted(daft.read_parquet(d, ignore_corrupt_files=True).to_pydict()["x"])
    assert result == [10, 20, 30, 40]


def test_parquet_ignore_corrupt_files_schema_inference_fallback(tmp_path):
    """Schema is inferred from the first readable file when the first file is corrupt."""
    d = str(tmp_path)
    # Put corrupt file first (lexicographically before good.parquet)
    _write_corrupt_parquet(d, "aaa_bad.parquet")
    _write_parquet(d, "zzz_good.parquet", {"col_a": [7, 8, 9]})

    df = daft.read_parquet(d, ignore_corrupt_files=True)
    assert "col_a" in df.schema.column_names()
    result = sorted(df.to_pydict()["col_a"])
    assert result == [7, 8, 9]


def test_parquet_ignore_corrupt_files_count_correct(tmp_path):
    """COUNT(*) returns the actual row count of non-corrupt files, not a wrong stat."""
    d = str(tmp_path)
    _write_parquet(d, "good.parquet", {"v": list(range(100))})
    _write_corrupt_parquet(d, "bad.parquet")

    count = daft.read_parquet(d, ignore_corrupt_files=True).count_rows()
    assert count == 100


# ---------------------------------------------------------------------------
# CSV tests
# ---------------------------------------------------------------------------


def test_csv_ignore_corrupt_files_skips_unreadable(tmp_path):
    """An unreadable (zero-byte) CSV is skipped; valid files are returned."""
    d = str(tmp_path)
    _write_csv(d, "good1.csv", "a\n1\n2\n3\n")
    _write_corrupt_csv(d, "bad.csv")
    _write_csv(d, "good2.csv", "a\n4\n5\n6\n")

    df = daft.read_csv(d, ignore_corrupt_files=True)
    result = sorted(df.to_pydict()["a"])
    assert result == [1, 2, 3, 4, 5, 6]


def test_csv_ignore_corrupt_files_false_raises(tmp_path):
    """Without ignore_corrupt_files, an unreadable CSV raises an error."""
    d = str(tmp_path)
    _write_csv(d, "good.csv", "a\n1\n2\n")
    _write_corrupt_csv(d, "bad.csv")

    with pytest.raises(Exception):
        daft.read_csv(d, ignore_corrupt_files=False).collect()


def test_csv_ignore_corrupt_files_all_good(tmp_path):
    """When no CSV files are corrupt, all rows are returned normally."""
    d = str(tmp_path)
    _write_csv(d, "a.csv", "n\n10\n20\n")
    _write_csv(d, "b.csv", "n\n30\n40\n")

    result = sorted(daft.read_csv(d, ignore_corrupt_files=True).to_pydict()["n"])
    assert result == [10, 20, 30, 40]
