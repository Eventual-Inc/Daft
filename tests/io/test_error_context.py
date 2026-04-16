from __future__ import annotations

import os
import tempfile

import pytest

import daft


def test_read_json_error_context():
    with tempfile.TemporaryDirectory() as tmpdir:
        bad_file = os.path.join(tmpdir, "bad_data.json")
        with open(bad_file, "w") as f:
            f.write('{"a": 1}\n{INVALID JSON}\n')

        with pytest.raises(Exception) as exc_info:
            df = daft.read_json(bad_file)
            df.collect()

        error_msg = str(exc_info.value)
        assert bad_file in error_msg
        assert "reading" in error_msg.lower()


def test_read_csv_error_context():
    with tempfile.TemporaryDirectory() as tmpdir:
        bad_csv = os.path.join(tmpdir, "bad_data.csv")
        with open(bad_csv, "w") as f:
            f.write("a,b,c\n1,2,3\n4,5\n")

        with pytest.raises(Exception) as exc_info:
            df = daft.read_csv(bad_csv)
            df.collect()

        error_msg = str(exc_info.value)
        assert bad_csv in error_msg
        assert "reading" in error_msg.lower()


def test_missing_file_error_context():
    with pytest.raises(FileNotFoundError):
        df = daft.read_json("/tmp/nonexistent_file_12345.json")
        df.collect()


def test_write_parquet_error_context():
    with tempfile.TemporaryDirectory() as tmpdir:
        blocker = os.path.join(tmpdir, "blocker")
        with open(blocker, "w") as f:
            f.write("block")

        with pytest.raises(Exception) as exc_info:
            df = daft.from_pydict({"a": [1, 2, 3]})
            df.write_parquet(blocker)

        error_msg = str(exc_info.value)
        assert "writing" in error_msg.lower()
        assert "blocker" in error_msg


def test_write_csv_error_context():
    with tempfile.TemporaryDirectory() as tmpdir:
        blocker = os.path.join(tmpdir, "blocker")
        with open(blocker, "w") as f:
            f.write("block")

        with pytest.raises(Exception) as exc_info:
            df = daft.from_pydict({"a": [1, 2, 3]})
            df.write_csv(blocker)

        error_msg = str(exc_info.value)
        assert "writing" in error_msg.lower()
        assert "blocker" in error_msg


def test_read_parquet_error_context():
    with tempfile.TemporaryDirectory() as tmpdir:
        bad_file = os.path.join(tmpdir, "bad_data.parquet")
        with open(bad_file, "w") as f:
            f.write("this is not a parquet file")

        with pytest.raises(Exception) as exc_info:
            df = daft.read_parquet(bad_file)
            df.collect()

        error_msg = str(exc_info.value)
        assert bad_file in error_msg
        assert "reading" in error_msg.lower()


def test_read_json_recordbatch_error_context():
    """Tests read_json (daft-json/read.rs) single-file with_context."""
    from daft.recordbatch.recordbatch import RecordBatch

    with tempfile.TemporaryDirectory() as tmpdir:
        bad_file = os.path.join(tmpdir, "bad_data.json")
        with open(bad_file, "w") as f:
            for i in range(1100):
                f.write(f'{{"a": {i}}}\n')
            f.write(",bad\n")

        with pytest.raises(Exception) as exc_info:
            RecordBatch.read_json(bad_file)

        error_msg = str(exc_info.value)
        assert bad_file in error_msg, f"Expected file path in error: {error_msg}"


def test_read_csv_recordbatch_error_context():
    """Tests read_csv (daft-csv/read.rs) single-file with_context."""
    from daft.recordbatch.recordbatch import RecordBatch

    with tempfile.TemporaryDirectory() as tmpdir:
        bad_file = os.path.join(tmpdir, "bad_data.csv")
        with open(bad_file, "wb") as f:
            f.write(b"\xff\xfe not valid csv \x00\x01")

        with pytest.raises(Exception) as exc_info:
            RecordBatch.read_csv(bad_file)

        error_msg = str(exc_info.value)
        assert bad_file in error_msg, f"Expected file path in error: {error_msg}"


def test_read_parquet_recordbatch_error_context():
    """Tests read_parquet (daft-parquet/read.rs) single-file with_context."""
    from daft.recordbatch.recordbatch import RecordBatch

    with tempfile.TemporaryDirectory() as tmpdir:
        bad_file = os.path.join(tmpdir, "bad_data.parquet")
        with open(bad_file, "w") as f:
            f.write("this is not a parquet file")

        with pytest.raises(Exception) as exc_info:
            RecordBatch.read_parquet(bad_file)

        error_msg = str(exc_info.value)
        assert bad_file in error_msg, f"Expected file path in error: {error_msg}"


def test_read_parquet_statistics_error_context():
    """Tests read_parquet_statistics (daft-parquet/read.rs) via read_parquet_metadata with_context."""
    from daft.recordbatch.recordbatch import RecordBatch

    with tempfile.TemporaryDirectory() as tmpdir:
        bad_file = os.path.join(tmpdir, "bad_data.parquet")
        with open(bad_file, "w") as f:
            f.write("this is not a parquet file")

        with pytest.raises(Exception) as exc_info:
            RecordBatch.read_parquet_statistics(paths=[bad_file])

        error_msg = str(exc_info.value)
        assert bad_file in error_msg, f"Expected file path in error: {error_msg}"


def test_read_json_schema_error_context():
    """Tests read_json_schema (daft-json/schema.rs) with_context."""
    from daft.daft import read_json_schema as _read_json_schema

    with tempfile.TemporaryDirectory() as tmpdir:
        bad_file = os.path.join(tmpdir, "bad_data.json")
        with open(bad_file, "w") as f:
            f.write("{INVALID}\n")

        with pytest.raises(Exception) as exc_info:
            _read_json_schema(bad_file)

        error_msg = str(exc_info.value)
        assert bad_file in error_msg, f"Expected file path in error: {error_msg}"


def test_read_csv_schema_error_context():
    """Tests read_csv_schema (daft-csv/metadata.rs) with_context."""
    from daft.daft import read_csv_schema as _read_csv_schema

    with tempfile.TemporaryDirectory() as tmpdir:
        bad_file = os.path.join(tmpdir, "bad_data.csv")
        with open(bad_file, "wb") as f:
            f.write(b"\xff\xfe\x00\x01")

        with pytest.raises(Exception) as exc_info:
            _read_csv_schema(bad_file)

        error_msg = str(exc_info.value)
        assert bad_file in error_msg, f"Expected file path in error: {error_msg}"


def test_read_parquet_schema_error_context():
    """Tests read_parquet_schema_and_metadata + read_parquet_metadata with_context."""
    from daft.daft import read_parquet_schema as _read_parquet_schema

    with tempfile.TemporaryDirectory() as tmpdir:
        bad_file = os.path.join(tmpdir, "bad_data.parquet")
        with open(bad_file, "w") as f:
            f.write("this is not a parquet file")

        with pytest.raises(Exception) as exc_info:
            _read_parquet_schema(bad_file)

        error_msg = str(exc_info.value)
        assert bad_file in error_msg, f"Expected file path in error: {error_msg}"


def test_read_parquet_into_pyarrow_error_context():
    """Tests read_parquet_into_pyarrow (daft-parquet/read.rs) with_context."""
    from daft.recordbatch.recordbatch import read_parquet_into_pyarrow

    with tempfile.TemporaryDirectory() as tmpdir:
        bad_file = os.path.join(tmpdir, "bad_data.parquet")
        with open(bad_file, "w") as f:
            f.write("this is not a parquet file")

        with pytest.raises(Exception) as exc_info:
            read_parquet_into_pyarrow(bad_file)

        error_msg = str(exc_info.value)
        assert bad_file in error_msg, f"Expected file path in error: {error_msg}"


def test_read_parquet_into_pyarrow_bulk_error_context():
    """Tests read_parquet_into_pyarrow_bulk (daft-parquet/read.rs) spawn-level with_context."""
    from daft.recordbatch.recordbatch import read_parquet_into_pyarrow_bulk

    with tempfile.TemporaryDirectory() as tmpdir:
        bad_file = os.path.join(tmpdir, "bad_data.parquet")
        with open(bad_file, "w") as f:
            f.write("this is not a parquet file")

        with pytest.raises(Exception) as exc_info:
            read_parquet_into_pyarrow_bulk([bad_file])

        error_msg = str(exc_info.value)
        assert bad_file in error_msg, f"Expected file path in error: {error_msg}"
