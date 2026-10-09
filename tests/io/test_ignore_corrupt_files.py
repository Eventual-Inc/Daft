"""Tests for ignore_corrupt_files in file readers and Iceberg."""

from __future__ import annotations

import io
import os
import urllib.parse
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as papq
import pytest
from pyarrow import orc

import daft
from daft.catalog import Table
from tests.io.test_orc import _corrupt_orc_stripe

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


def _write_parquet_valid_footer_corrupt_data(directory: str, name: str) -> str:
    """Write a Parquet file with a valid footer but corrupt row-group data.

    This exercises the DaftError::ArrowRsError branch in is_parquet_corrupt,
    where arrow-rs/parquet-rs fails to decode column chunks and emits a message
    like "Parquet error: ..." that is caught by string matching.
    """
    import io
    import struct

    buf = io.BytesIO()
    papq.write_table(pa.table({"a": [1, 2, 3]}), buf)
    data = bytearray(buf.getvalue())

    # Footer length is the 4-byte little-endian int just before the trailing magic.
    footer_len = struct.unpack_from("<i", data, len(data) - 8)[0]
    footer_start = len(data) - 8 - footer_len

    # Overwrite everything between the leading magic and the footer with 0xFF,
    # leaving the footer and both PAR1 sentinels intact.
    for i in range(4, footer_start):
        data[i] = 0xFF

    path = os.path.join(directory, name)
    with open(path, "wb") as f:
        f.write(bytes(data))
    return path


def _write_parquet_partial_corrupt(directory: str, name: str) -> str:
    """Write a multi-row-group Parquet file where only the second row group is corrupt.

    Row group 0 is valid and readable; row group 1 has its column data
    overwritten with 0xFF.  The footer (and therefore metadata for both
    row groups) remains intact, so the reader will successfully decode
    row group 0 before hitting corruption in row group 1.
    """
    import struct

    path = os.path.join(directory, name)
    schema = pa.schema([("a", pa.int64())])
    writer = papq.ParquetWriter(path, schema)
    writer.write_table(pa.table({"a": [1, 2, 3]}))  # row group 0
    writer.write_table(pa.table({"a": [4, 5, 6]}))  # row group 1
    writer.close()

    metadata = papq.read_metadata(path)
    rg1_offset = metadata.row_group(1).column(0).data_page_offset

    with open(path, "rb") as f:
        data = bytearray(f.read())

    footer_len = struct.unpack_from("<i", data, len(data) - 8)[0]
    footer_start = len(data) - 8 - footer_len

    for i in range(rg1_offset, footer_start):
        data[i] = 0xFF

    with open(path, "wb") as f:
        f.write(bytes(data))
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
    """Corrupt Parquet file is skipped, valid rows returned, and skipped_corrupt_files is populated."""
    d = str(tmp_path)
    _write_parquet(d, "good1.parquet", {"a": [1, 2, 3]})
    _write_corrupt_parquet(d, "bad.parquet")
    _write_parquet(d, "good2.parquet", {"a": [4, 5, 6]})

    df = daft.read_parquet(d, ignore_corrupt_files=True)
    df.collect()

    assert sorted(df.to_pydict()["a"]) == [1, 2, 3, 4, 5, 6]

    skipped = df.skipped_corrupt_files
    assert len(skipped) == 1
    path, reason, partial = skipped[0]
    assert _basename(path) == "bad.parquet"
    assert reason
    assert not partial


def test_parquet_ignore_corrupt_false_raises(tmp_path):
    """Without ignore_corrupt_files, a corrupt file raises an error."""
    d = str(tmp_path)
    _write_parquet(d, "good.parquet", {"a": [1, 2, 3]})
    _write_corrupt_parquet(d, "bad.parquet")

    with pytest.raises(Exception):
        daft.read_parquet(d, ignore_corrupt_files=False).collect()


def test_parquet_ignore_corrupt_all_good_no_skips(tmp_path):
    """All valid files: all rows returned, skipped_corrupt_files is empty."""
    d = str(tmp_path)
    _write_parquet(d, "a.parquet", {"x": [10, 20]})
    _write_parquet(d, "b.parquet", {"x": [30, 40]})

    df = daft.read_parquet(d, ignore_corrupt_files=True)
    df.collect()

    assert sorted(df.to_pydict()["x"]) == [10, 20, 30, 40]
    assert df.skipped_corrupt_files == []


def test_parquet_ignore_corrupt_schema_inference_fallback(tmp_path):
    """Schema is inferred from the first readable file when the first file (lexicographically) is corrupt."""
    d = str(tmp_path)
    _write_corrupt_parquet(d, "aaa_bad.parquet")
    _write_parquet(d, "zzz_good.parquet", {"col_a": [7, 8, 9]})

    df = daft.read_parquet(d, ignore_corrupt_files=True)
    df.collect()

    assert "col_a" in df.schema().column_names()
    assert sorted(df.to_pydict()["col_a"]) == [7, 8, 9]
    assert any(_basename(p) == "aaa_bad.parquet" for p, _, _ in df.skipped_corrupt_files)


def test_parquet_ignore_corrupt_count_correct(tmp_path):
    """COUNT(*) returns only rows from non-corrupt files."""
    d = str(tmp_path)
    _write_parquet(d, "good.parquet", {"v": list(range(100))})
    _write_corrupt_parquet(d, "bad.parquet")

    assert daft.read_parquet(d, ignore_corrupt_files=True).count_rows() == 100


def test_parquet_ignore_corrupt_rowgroup_data(tmp_path):
    """File with valid footer but corrupt row-group data is skipped via CorruptFile variant."""
    d = str(tmp_path)
    _write_parquet(d, "good.parquet", {"a": [10, 20, 30]})
    _write_parquet_valid_footer_corrupt_data(d, "zzz_bad_rowgroup.parquet")

    df = daft.read_parquet(d, ignore_corrupt_files=True)
    df.collect()

    assert sorted(df.to_pydict()["a"]) == [10, 20, 30]
    skipped = df.skipped_corrupt_files
    assert len(skipped) == 1
    path, reason, _partial = skipped[0]
    assert _basename(path) == "zzz_bad_rowgroup.parquet"
    assert reason


def test_parquet_ignore_corrupt_partial_read(tmp_path):
    """File with valid first row group and corrupt second row group reports partial=True."""
    d = str(tmp_path)
    _write_parquet(d, "good.parquet", {"a": [10, 20, 30]})
    _write_parquet_partial_corrupt(d, "partial.parquet")

    df = daft.read_parquet(d, ignore_corrupt_files=True)
    df.collect()

    result = sorted(df.to_pydict()["a"])
    # good.parquet contributes [10, 20, 30]; first row group of partial.parquet contributes [1, 2, 3]
    assert 10 in result and 20 in result and 30 in result
    assert 1 in result and 2 in result and 3 in result

    skipped = df.skipped_corrupt_files
    assert len(skipped) == 1
    path, reason, partial = skipped[0]
    assert _basename(path) == "partial.parquet"
    assert reason
    assert partial


def test_parquet_ignore_corrupt_all_corrupt_raises(tmp_path):
    """When every file is corrupt, an error is raised even with ignore_corrupt_files=True."""
    d = str(tmp_path)
    _write_corrupt_parquet(d, "bad1.parquet")
    _write_corrupt_parquet(d, "bad2.parquet")

    with pytest.raises(Exception):
        daft.read_parquet(d, ignore_corrupt_files=True).collect()


def test_parquet_ignore_corrupt_multiple_corrupt_files(tmp_path):
    """Multiple corrupt files are all recorded in skipped_corrupt_files."""
    d = str(tmp_path)
    _write_parquet(d, "good.parquet", {"a": [1, 2, 3]})
    _write_corrupt_parquet(d, "bad1.parquet")
    _write_corrupt_parquet(d, "bad2.parquet")

    df = daft.read_parquet(d, ignore_corrupt_files=True)
    df.collect()

    assert sorted(df.to_pydict()["a"]) == [1, 2, 3]
    skipped_names = {_basename(p) for p, _, _ in df.skipped_corrupt_files}
    assert skipped_names == {"bad1.parquet", "bad2.parquet"}


# ── ORC ───────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("first_bad", [False, True])
def test_orc_ignore_corrupt_fallback_and_complete_report(tmp_path: Path, first_bad: bool) -> None:
    good = tmp_path / "good.orc"
    orc.write_table(pa.table({"id": [1, 2, 3]}), good)
    bad = [tmp_path / "bad1.orc", tmp_path / "bad2.orc"]
    for path in bad:
        path.write_bytes(b"not ORC")
    paths = [*bad, good] if first_bad else [good, *bad]
    df = daft.read_orc([str(path) for path in paths], batch_size=1, ignore_corrupt_files=True)
    with pytest.raises(ValueError, match="until.*collected"):
        _ = df.skipped_corrupt_files
    df.collect()
    assert sorted(df.to_pydict()["id"]) == [1, 2, 3]
    assert {_basename(path) for path, _, _ in df.skipped_corrupt_files} == {path.name for path in bad}
    assert len(df.skipped_corrupt_files) == 2
    assert all(reason and not partial for _, reason, partial in df.skipped_corrupt_files)
    report = list(df.skipped_corrupt_files)
    df.collect()
    assert df.skipped_corrupt_files == report


def test_orc_ignore_corrupt_default_and_all_bad(tmp_path: Path) -> None:
    bad = tmp_path / "bad.orc"
    bad.write_bytes(b"not ORC")
    with pytest.raises(OSError, match="Not an ORC file"):
        daft.read_orc(str(bad))
    with pytest.raises(ValueError, match="All ORC files.*cannot infer"):
        daft.read_orc(str(bad), ignore_corrupt_files=True)


def test_orc_ignore_corrupt_good_files_have_no_report(tmp_path: Path) -> None:
    path = tmp_path / "good.orc"
    orc.write_table(pa.table({"id": [1, 2]}), path)
    df = daft.read_orc(str(path), ignore_corrupt_files=True).collect()
    assert df.to_pydict() == {"id": [1, 2]}
    assert df.skipped_corrupt_files == []


@pytest.mark.parametrize("kind", ["short", "postscript", "footer", "stripe_footer"])
@pytest.mark.parametrize("ignore", [False, True])
def test_orc_ignore_corrupt_metadata_errors(tmp_path: Path, kind: str, ignore: bool) -> None:
    good, bad = tmp_path / "good.orc", tmp_path / "bad.orc"
    orc.write_table(pa.table({"id": [1, 2]}), good)
    buffer = io.BytesIO()
    orc.write_table(pa.table({"id": [3, 4]}), buffer, compression="uncompressed")
    data = buffer.getvalue()
    if kind == "short":
        damaged = b"ORC"
    elif kind == "postscript":
        damaged = data[:-10]
    elif kind == "footer":
        reader = orc.ORCFile(io.BytesIO(data))
        footer_end = len(data) - 1 - data[-1]
        damaged = (
            data[: footer_end - reader.file_footer_length] + b"\xff" * reader.file_footer_length + data[footer_end:]
        )
    else:
        damaged = _corrupt_orc_stripe(data, 0, footer=True)
    bad.write_bytes(damaged)
    if not ignore:
        with pytest.raises(Exception, match="(?i)(postscript|footer|ORC)"):
            daft.read_orc([str(bad), str(good)], ignore_corrupt_files=ignore).collect()
    else:
        df = daft.read_orc([str(bad), str(good)], ignore_corrupt_files=ignore).collect()
        assert df.to_pydict() == {"id": [1, 2]}
        assert len(df.skipped_corrupt_files) == 1
        path, reason, partial = df.skipped_corrupt_files[0]
        assert _basename(path) == bad.name and reason and not partial


@pytest.mark.parametrize("ignore", [False, True])
@pytest.mark.parametrize("missing", [False, True])
def test_orc_ignore_corrupt_execution_failure(tmp_path: Path, ignore: bool, missing: bool) -> None:
    good, bad = tmp_path / "good.orc", tmp_path / "bad.orc"
    orc.write_table(pa.table({"id": [1]}), good)
    orc.write_table(pa.table({"id": [2]}), bad)
    df = daft.read_orc([str(good), str(bad)], ignore_corrupt_files=ignore)
    if missing:
        bad.rename(tmp_path / "saved-input")
    else:
        bad.write_bytes(b"not ORC")
    if not ignore:
        with pytest.raises(Exception, match="(?i)(FileNotFoundError|does not exist|not an ORC file)"):
            df.collect()
    else:
        df.collect()
        assert df.to_pydict() == {"id": [1]}
        assert len(df.skipped_corrupt_files) == 1
        path, reason, partial = df.skipped_corrupt_files[0]
        assert _basename(path) == bad.name and reason and not partial


def test_orc_ignore_corrupt_all_bad_after_inference(tmp_path: Path) -> None:
    paths = [tmp_path / "first.orc", tmp_path / "second.orc"]
    for path in paths:
        orc.write_table(pa.table({"id": [1]}), path)
    df = daft.read_orc([str(path) for path in paths], ignore_corrupt_files=True)
    for path in paths:
        path.write_bytes(b"not ORC")
    df.collect()
    assert df.to_pydict() == {"id": []}
    assert {_basename(path) for path, _, _ in df.skipped_corrupt_files} == {path.name for path in paths}


@pytest.mark.parametrize("ignore", [False, True])
def test_orc_ignore_corrupt_partial_read(tmp_path: Path, ignore: bool) -> None:
    buffer = io.BytesIO()
    orc.write_table(
        pa.table({"id": range(12000), "name": [f"row-{i:08d}-" + "x" * 80 for i in range(12000)]}),
        buffer,
        stripe_size=65536,
        batch_size=1024,
        compression="uncompressed",
    )
    path = tmp_path / "partial.orc"
    path.write_bytes(_corrupt_orc_stripe(buffer.getvalue()))
    df = daft.read_orc(str(path), batch_size=257, ignore_corrupt_files=ignore)
    if not ignore:
        with pytest.raises(Exception, match="bad read in RleDecoderV2"):
            df.collect()
    else:
        df.collect()
        result = df.to_pydict()
        assert 0 < len(result["id"]) < 12000
        assert result["id"] == list(range(len(result["id"])))
        assert result["name"] == [f"row-{i:08d}-" + "x" * 80 for i in result["id"]]
        assert len(df.skipped_corrupt_files) == 1
        skipped_path, reason, partial = df.skipped_corrupt_files[0]
        assert _basename(skipped_path) == path.name and "bad read in RleDecoderV2" in reason and partial


# ── CSV ───────────────────────────────────────────────────────────────────────


def test_csv_ignore_corrupt_skips_and_reports(tmp_path):
    """Corrupt CSV file is skipped, valid rows returned, and skipped_corrupt_files is populated."""
    d = str(tmp_path)
    _write_csv(d, "good1.csv", "a\n1\n2\n3\n")
    _write_csv(d, "good2.csv", "a\n4\n5\n6\n")
    _write_corrupt_csv(d, "zzz_bad.csv")

    df = daft.read_csv(d, ignore_corrupt_files=True)
    df.collect()

    assert sorted(df.to_pydict()["a"]) == [1, 2, 3, 4, 5, 6]

    skipped = df.skipped_corrupt_files
    assert len(skipped) == 1
    path, reason, partial = skipped[0]
    assert _basename(path) == "zzz_bad.csv"
    assert reason
    assert not partial


def test_csv_ignore_corrupt_false_raises(tmp_path):
    """Without ignore_corrupt_files, an unreadable CSV raises an error."""
    d = str(tmp_path)
    _write_csv(d, "good.csv", "a\n1\n2\n")
    _write_corrupt_csv(d, "bad.csv")

    with pytest.raises(Exception):
        daft.read_csv(d, ignore_corrupt_files=False).collect()


def test_csv_ignore_corrupt_all_good_no_skips(tmp_path):
    """All valid CSV files: all rows returned, skipped_corrupt_files is empty."""
    d = str(tmp_path)
    _write_csv(d, "a.csv", "n\n10\n20\n")
    _write_csv(d, "b.csv", "n\n30\n40\n")

    df = daft.read_csv(d, ignore_corrupt_files=True)
    df.collect()

    assert sorted(df.to_pydict()["n"]) == [10, 20, 30, 40]
    assert df.skipped_corrupt_files == []


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
    assert any(_basename(p) == "zzz_bad.csv" for p, _, _ in df.skipped_corrupt_files)


def test_csv_ignore_corrupt_all_corrupt_raises(tmp_path):
    """When every CSV file is corrupt, an error is raised even with ignore_corrupt_files=True."""
    d = str(tmp_path)
    _write_corrupt_csv(d, "bad1.csv")
    _write_corrupt_csv(d, "bad2.csv")

    with pytest.raises(Exception):
        daft.read_csv(d, ignore_corrupt_files=True).collect()


def test_csv_ignore_corrupt_multiple_corrupt_files(tmp_path):
    """Multiple corrupt CSV files are all recorded in skipped_corrupt_files."""
    d = str(tmp_path)
    _write_csv(d, "good.csv", "a\n1\n2\n3\n")
    # Binary garbage fails during reading. Provide an explicit schema to bypass
    # schema inference so these files are only encountered at read time.
    _write_corrupt_csv(d, "zzz_bad1.csv")
    _write_corrupt_csv(d, "zzz_bad2.csv")

    df = daft.read_csv(
        d,
        schema={"a": daft.DataType.int64()},
        infer_schema=False,
        ignore_corrupt_files=True,
    )
    df.collect()

    assert sorted(df.to_pydict()["a"]) == [1, 2, 3]
    skipped_names = {_basename(p) for p, _, _ in df.skipped_corrupt_files}
    assert skipped_names == {"zzz_bad1.csv", "zzz_bad2.csv"}


# ── Iceberg ───────────────────────────────────────────────────────────────────
#
# These tests require pyiceberg. They are automatically skipped when the
# package is not installed (pytest.importorskip inside the fixture).
#
# Iceberg data files go through the Rust Parquet reader, so corrupt files are
# reflected in df.skipped_corrupt_files just like plain read_parquet.


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
    """Corrupt Iceberg data file is skipped, valid rows returned, and skipped_corrupt_files is populated."""
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

    skipped = df.skipped_corrupt_files
    assert len(skipped) == 1
    _, reason, partial = skipped[0]
    assert reason
    assert not partial


def test_iceberg_table_read_ignore_corrupt_skips_and_reports(local_iceberg_catalog):
    """Table.read forwards ignore_corrupt_files to read_iceberg."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    schema = Schema(NestedField(1, "id", LongType(), required=False))
    iceberg_table = local_iceberg_catalog.create_table("default.test_table_corrupt", schema=schema)

    iceberg_table.append(pa.table({"id": pa.array([1, 2, 3], type=pa.int64())}))
    iceberg_table.append(pa.table({"id": pa.array([4, 5, 6], type=pa.int64())}))

    data_files = _iceberg_data_file_local_paths(iceberg_table)
    assert len(data_files) == 2, f"Expected 2 data files, got {len(data_files)}"

    with open(data_files[0], "wb") as f:
        f.write(b"PAR1" + b"\x00" * 20 + b"PAR1")

    df = Table.from_iceberg(iceberg_table).read(ignore_corrupt_files=True)
    df.collect()

    result = sorted(df.to_pydict()["id"])
    assert len(result) == 3
    assert set(result).issubset({1, 2, 3, 4, 5, 6})

    skipped = df.skipped_corrupt_files
    assert len(skipped) == 1
    _, reason, partial = skipped[0]
    assert reason
    assert not partial


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
    """All valid Iceberg files: all rows returned, skipped_corrupt_files is empty."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    schema = Schema(NestedField(1, "id", LongType(), required=False))
    table = local_iceberg_catalog.create_table("default.test_all_good", schema=schema)
    table.append(pa.table({"id": pa.array([1, 2, 3], type=pa.int64())}))
    table.append(pa.table({"id": pa.array([4, 5, 6], type=pa.int64())}))

    df = daft.read_iceberg(table, ignore_corrupt_files=True)
    df.collect()

    assert sorted(df.to_pydict()["id"]) == [1, 2, 3, 4, 5, 6]
    assert df.skipped_corrupt_files == []
